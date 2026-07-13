<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Tests\Support;

use PDO;
use RuntimeException;

/** Runs isolated child PHP programs in the one package process-test workspace. */
final class ProcessHarness
{
    private string $workspace;

    public function __construct()
    {
        $this->workspace = $GLOBALS['sqliteFairTestRunDirectory'].'/workspaces/sqlite-fair-process';
    }

    /** @param callable(string): void $scenario */
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

    public function autoloadPath(): string
    {
        return dirname(__DIR__, 2).'/vendor/autoload.php';
    }

    /**
     * Starts all supplied child programs before collecting their output.
     *
     * @param list<array{scenario: string, arguments?: array<string, int|string>}> $children
     * @return list<array{exit_code: int, stdout: string, stderr: string}>
     */
    public function runChildren(array $specifications): array
    {
        if (! is_dir($this->workspace)) {
            throw new RuntimeException('Child processes require an active SQLite fair process workspace.');
        }

        $children = [];
        foreach ($specifications as $child) {
            $pipes = [];
            $process = proc_open(
                [PHP_BINARY, $this->childDispatcherPath()],
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
                throw new RuntimeException('A SQLite fair child process could not be started.');
            }
            fclose($pipes[0]);
            $children[] = [$process, $pipes[1], $pipes[2]];
        }

        $results = [];
        foreach ($children as [$process, $stdout, $stderr]) {
            $output = stream_get_contents($stdout);
            $errors = stream_get_contents($stderr);
            fclose($stdout);
            fclose($stderr);
            $results[] = [
                'exit_code' => proc_close($process),
                'stdout' => $output === false ? '' : $output,
                'stderr' => $errors === false ? '' : $errors,
            ];
        }

        return $results;
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
