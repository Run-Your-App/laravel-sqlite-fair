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

    /** @var Closure(array<int, string>, array<int, array{string, string, string}>, array<int, resource>&, string, array<string, mixed>): mixed */
    private Closure $startProcess;

    /**
     * Selects the fixed workspace and child-process boundary used by package tests.
     *
     * The optional starter exists only for deterministic cleanup verification when a later child cannot be spawned.
     * Production-shaped process tests use PHP's native `proc_open()` through the default closure.
     *
     * @param  (callable(array<int, string>, array<int, array{string, string, string}>, array<int, resource>&, string, array<string, mixed>): mixed)|null  $startProcess  Internal child-process construction seam.
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
     * The harness (1) starts every child before awaiting results, (2) captures stdout and stderr in workspace files
     * without platform-specific process-pipe behavior, and (3) terminates the remaining group when the shared
     * monotonic deadline expires.
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
        $stdinPath = $this->workspace.'/child-stdin';
        $stdin = fopen($stdinPath, 'wb');
        if ($stdin === false) {
            throw new RuntimeException('The SQLite fair child input capture could not be created.');
        }
        fclose($stdin);
        $children = [];
        foreach ($specifications as $index => $child) {
            $pipes = [];
            $stdoutPath = $this->workspace.'/child-'.$index.'.stdout';
            $stderrPath = $this->workspace.'/child-'.$index.'.stderr';
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
                [
                    0 => ['file', $stdinPath, 'r'],
                    1 => ['file', $stdoutPath, 'w'],
                    2 => ['file', $stderrPath, 'w'],
                ],
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
            $children[$index] = [
                'process' => $process,
                'stdout_path' => $stdoutPath,
                'stderr_path' => $stderrPath,
                'scenario' => $child['scenario'],
            ];
        }

        $results = [];
        while ($children !== []) {
            foreach ($children as $index => $child) {
                $status = proc_get_status($child['process']);
                if ($status['running']) {
                    continue;
                }

                $closedExitCode = proc_close($child['process']);
                $results[$index] = [
                    'exit_code' => $status['exitcode'] >= 0 ? $status['exitcode'] : $closedExitCode,
                    'stdout' => $this->readCapture($child['stdout_path']),
                    'stderr' => $this->readCapture($child['stderr_path']),
                ];
                unset($children[$index]);
            }

            if ($children === []) {
                break;
            }
            if (hrtime(true) / 1e9 >= $deadline) {
                $blockedChildren = array_map(
                    fn (array $child): string => sprintf(
                        '%s (stdout: %s; stderr: %s)',
                        $child['scenario'],
                        mb_substr($this->readCapture($child['stdout_path']), 0, 1000),
                        mb_substr($this->readCapture($child['stderr_path']), 0, 1000),
                    ),
                    $children,
                );
                $this->terminateChildren($children);

                throw new RuntimeException(sprintf(
                    'SQLite fair child deadline exceeded: %s.',
                    implode(', ', $blockedChildren),
                ));
            }

            time_nanosleep(0, 10_000_000);
        }

        ksort($results);

        return array_values($results);
    }

    /**
     * Terminates every still-owned child.
     *
     * This group cleanup is shared by spawn failures and deadline failures so no
     * earlier child can outlive a failed process-test group or retain SQLite locks.
     *
     * @param  array<int, array{process: resource, stdout_path: string, stderr_path: string, scenario: string}>  $children  Children still owned by this harness call.
     * @return void Every supplied process has been released best-effort.
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
            if (is_resource($child['process'])) {
                @proc_close($child['process']);
            }
        }
    }

    private function readCapture(string $path): string
    {
        $capture = fopen($path, 'rb');
        if ($capture === false) {
            return '';
        }
        $contents = stream_get_contents($capture);
        fclose($capture);

        return $contents === false ? '' : $contents;
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
