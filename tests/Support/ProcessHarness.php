<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Tests\Support;

use Closure;
use PDO;
use RuntimeException;

/**
 * Runs isolated PHP children against one disposable process-test workspace.
 *
 * FairSQLiteProcessTest uses this harness to start real concurrent PHP processes, collect each child's diagnostics,
 * and fail boundedly when a process cannot reach its coordination barrier.
 */
final class ProcessHarness
{
    private const float DEFAULT_CHILD_TIMEOUT_SECONDS = 15.0;

    private string $workspace;

    /** @var Closure(array<int, string>, array<int, array{string, string}>, array<int, resource>&, string, array<string, mixed>): mixed */
    private Closure $startProcess;

    /**
     * Selects the fixed workspace and child-process boundary used by package tests.
     *
     * The optional starter exists only for deterministic cleanup verification when a later child cannot be spawned.
     * Production-shaped process tests use PHP's native `proc_open()` through the default closure.
     *
     * @param  (callable(array<int, string>, array<int, array{string, string}>, array<int, resource>&, string, array<string, mixed>): mixed)|null  $startProcess  Internal child-process construction seam.
     * @return void The harness is ready to initialize the workspace for one scenario.
     */
    public function __construct(?callable $startProcess = null)
    {
        $runDirectory = $GLOBALS['sqliteFairTestRunDirectory'] ?? null;
        if (! is_string($runDirectory) || $runDirectory === '') {
            throw new RuntimeException('The SQLite fair test run directory is unavailable.');
        }
        $this->workspace = $runDirectory.'/workspaces/sqlite-fair-process';
        $this->startProcess = $startProcess === null
            ? static function (array $command, array $descriptorSpec, array &$pipes, string $workingDirectory, array $environment) {
                return proc_open($command, $descriptorSpec, $pipes, $workingDirectory, $environment);
            }
            : $startProcess(...);
    }

    /**
     * Runs one process-test scenario in a freshly initialized workspace.
     *
     * @param  callable(string): void  $scenario  Test callback receiving the disposable workspace path.
     * @return void The workspace is removed after success or failure.
     */
    public function run(callable $scenario): void
    {
        $this->remove($this->workspace);
        if (! mkdir($this->workspace, 0775, true) && ! is_dir($this->workspace)) {
            throw new RuntimeException('The SQLite fair process workspace could not be created.');
        }
        $coordination = new PDO('sqlite:'.$this->workspace.'/coordination.sqlite', options: [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        ]);
        $coordination->exec('PRAGMA busy_timeout=5000');
        $coordination->exec('CREATE TABLE signals (name TEXT PRIMARY KEY, value TEXT NOT NULL)');
        $coordination->exec('CREATE TABLE events (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL)');
        $coordination = null;

        try {
            $scenario($this->workspace);
        } finally {
            $this->remove($this->workspace);
        }
    }

    /**
     * Returns the package autoloader used by every child process.
     *
     * @return string Absolute path to this package's Composer autoloader.
     */
    public function autoloadPath(): string
    {
        return dirname(__DIR__, 2).'/vendor/autoload.php';
    }

    /**
     * Starts concurrent child programs and bounds their complete runtime.
     *
     * The harness (1) starts every child before awaiting results, (2) drains stdout and stderr without serial pipe
     * blocking, and (3) terminates the remaining group when the shared monotonic deadline expires.
     *
     * @param  list<array{scenario: string, arguments?: array<string, int|string>}>  $specifications  Ordered child scenarios with scalar arguments.
     * @param  float  $timeoutSeconds  Finite positive seconds shared by the complete child group.
     * @return list<array{exit_code: int, stdout: string, stderr: string}> Results in specification order.
     *
     * @throws RuntimeException When the workspace is unavailable, a child cannot start, or any child exceeds the deadline.
     */
    public function runChildren(
        array $specifications,
        float $timeoutSeconds = self::DEFAULT_CHILD_TIMEOUT_SECONDS,
    ): array {
        if (! is_dir($this->workspace)) {
            throw new RuntimeException('Child processes require an active SQLite fair process workspace.');
        }
        if (! is_finite($timeoutSeconds) || $timeoutSeconds <= 0.0) {
            throw new RuntimeException('The SQLite fair child timeout must be finite and positive.');
        }

        $deadline = hrtime(true) / 1e9 + $timeoutSeconds;
        $children = [];
        foreach ($specifications as $index => $child) {
            $pipes = [];
            $startProcess = $this->startProcess;
            $process = $startProcess(
                [
                    PHP_BINARY,
                    '-d',
                    'display_errors=stderr',
                    '-d',
                    'log_errors=0',
                    $this->childDispatcherPath(),
                ],
                [0 => ['pipe', 'r'], 1 => ['pipe', 'w'], 2 => ['pipe', 'w']],
                $pipes,
                $this->workspace,
                array_merge($_ENV, [
                    'SQLITE_FAIR_PROCESS_WORKSPACE' => $this->workspace,
                    'SQLITE_FAIR_PACKAGE_AUTOLOAD' => $this->autoloadPath(),
                    'SQLITE_FAIR_PROCESS_SCENARIO' => $child['scenario'],
                    'SQLITE_FAIR_PROCESS_ARGUMENTS' => json_encode(
                        $child['arguments'] ?? [],
                        JSON_THROW_ON_ERROR,
                    ),
                ]),
            );
            if (! is_resource($process)) {
                $this->closePipes($pipes);
                $this->terminateChildren($children);

                throw new RuntimeException('A SQLite fair child process could not be started.');
            }
            fclose($pipes[0]);
            stream_set_blocking($pipes[1], false);
            stream_set_blocking($pipes[2], false);
            $children[$index] = [
                'process' => $process,
                'stdout_stream' => $pipes[1],
                'stderr_stream' => $pipes[2],
                'stdout' => '',
                'stderr' => '',
                'scenario' => $child['scenario'],
            ];
        }

        $results = [];
        while ($children !== []) {
            foreach ($children as $index => &$child) {
                foreach (['stdout', 'stderr'] as $channel) {
                    $chunk = stream_get_contents($child[$channel.'_stream']);
                    if ($chunk !== false) {
                        $child[$channel] .= $chunk;
                    }
                }

                $status = proc_get_status($child['process']);
                if ($status['running']) {
                    continue;
                }

                foreach (['stdout', 'stderr'] as $channel) {
                    $chunk = stream_get_contents($child[$channel.'_stream']);
                    if ($chunk !== false) {
                        $child[$channel] .= $chunk;
                    }
                    fclose($child[$channel.'_stream']);
                }
                $closedExitCode = proc_close($child['process']);
                $results[$index] = [
                    'exit_code' => $status['exitcode'] >= 0 ? $status['exitcode'] : $closedExitCode,
                    'stdout' => $child['stdout'],
                    'stderr' => $child['stderr'],
                ];
                unset($children[$index]);
            }
            unset($child);

            if ($children === []) {
                break;
            }
            if (hrtime(true) / 1e9 >= $deadline) {
                $blockedChildren = array_map(
                    static fn (array $child): string => sprintf(
                        '%s (stdout: %s; stderr: %s)',
                        $child['scenario'],
                        mb_substr($child['stdout'], 0, 1000),
                        mb_substr($child['stderr'], 0, 1000),
                    ),
                    $children,
                );
                $this->terminateChildren($children);

                throw new RuntimeException(sprintf(
                    'SQLite fair child deadline exceeded: %s.',
                    implode(', ', $blockedChildren),
                ));
            }

            $read = [];
            foreach ($children as $child) {
                if (! feof($child['stdout_stream'])) {
                    $read[] = $child['stdout_stream'];
                }
                if (! feof($child['stderr_stream'])) {
                    $read[] = $child['stderr_stream'];
                }
            }
            if ($read !== []) {
                $write = [];
                $except = [];
                stream_select($read, $write, $except, 0, 50_000);
            }
        }

        ksort($results);

        return array_values($results);
    }

    /**
     * Terminates every still-owned child and closes both output pipes.
     *
     * This group cleanup is shared by spawn failures and deadline failures so no
     * earlier child can outlive a failed process-test group or retain SQLite locks.
     *
     * @param  array<int, array{process: resource, stdout_stream: resource, stderr_stream: resource, stdout: string, stderr: string, scenario: string}>  $children  Children still owned by this harness call.
     * @return void Every supplied process and output pipe has been released best-effort.
     */
    private function terminateChildren(array $children): void
    {
        foreach ($children as $child) {
            if (is_resource($child['process'])) {
                $status = proc_get_status($child['process']);
                if ($status['running']) {
                    @proc_terminate($child['process'], 9);
                }
            }
            $this->closePipes([$child['stdout_stream'], $child['stderr_stream']]);
            if (is_resource($child['process'])) {
                @proc_close($child['process']);
            }
        }
    }

    /** @param array<int, mixed> $pipes */
    private function closePipes(array $pipes): void
    {
        foreach ($pipes as $pipe) {
            if (is_resource($pipe)) {
                @fclose($pipe);
            }
        }
    }

    private function childDispatcherPath(): string
    {
        return dirname(__DIR__).'/Fixtures/process-child.php';
    }

    private function remove(string $path): void
    {
        if (! is_dir($path)) {
            return;
        }
        foreach (scandir($path) ?: [] as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }
            $child = $path.'/'.$entry;
            is_dir($child) ? $this->remove($child) : unlink($child);
        }
        rmdir($path);
    }
}
