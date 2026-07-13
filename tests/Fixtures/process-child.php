<?php

declare(strict_types=1);

use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;

require getenv('SQLITE_FAIR_PACKAGE_AUTOLOAD');

$workspace = getenv('SQLITE_FAIR_PROCESS_WORKSPACE');
$scenario = getenv('SQLITE_FAIR_PROCESS_SCENARIO');

if (! is_string($workspace) || $workspace === '') {
    throw new RuntimeException('The SQLite fair process workspace is missing.');
}

match ($scenario) {
    'lock-reader' => holdLockDatabaseRead($workspace),
    'ticket-mutator' => deleteTicketAfterReaderReleases($workspace),
    default => throw new RuntimeException("Unknown SQLite fair process scenario [{$scenario}]."),
};

/** Holds a real read transaction until the competing ticket mutation reaches its waiter. */
function holdLockDatabaseRead(string $workspace): void
{
    $pdo = new PDO('sqlite:'.$workspace.'/locks/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pdo->exec('PRAGMA busy_timeout=0');
    $pdo->beginTransaction();
    $statement = $pdo->query('SELECT ticket FROM tickets ORDER BY ticket LIMIT 1');
    $statement->fetchColumn();
    $statement->closeCursor();
    signal($workspace, 'reader-ready');
    waitForSignal($workspace, 'release-reader');
    $pdo->commit();
    signal($workspace, 'reader-released');
}

/** Deletes one ticket after proving the reader blocked exclusive mutation admission. */
function deleteTicketAfterReaderReleases(string $workspace): void
{
    waitForSignal($workspace, 'reader-ready');
    $waiter = new class($workspace) implements Waiter {
        public function __construct(private readonly string $workspace) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            signal($this->workspace, 'release-reader');
            waitForSignal($this->workspace, 'reader-released');
        }
    };
    $database = new LockDatabase($workspace.'/locks', $waiter, static fn (): float => hrtime(true) / 1e9);
    $database->deleteExact(1, hrtime(true) / 1e9 + 5.0);
}

/** Records a deterministic cross-process signal in the harness coordination database. */
function signal(string $workspace, string $name): void
{
    $pdo = coordinationDatabase($workspace);
    $statement = $pdo->prepare('INSERT OR REPLACE INTO signals (name, value) VALUES (:name, :value)');
    $statement->execute(['name' => $name, 'value' => '1']);
    $statement->closeCursor();
}

/** Waits for a sibling-process signal without creating test program files. */
function waitForSignal(string $workspace, string $name): void
{
    $pdo = coordinationDatabase($workspace);
    $pair = stream_socket_pair(STREAM_PF_UNIX, STREAM_SOCK_STREAM, STREAM_IPPROTO_IP);
    if ($pair === false) {
        throw new RuntimeException('The SQLite fair process signal wait could not create a socket pair.');
    }

    do {
        $statement = $pdo->prepare('SELECT value FROM signals WHERE name = :name');
        $statement->execute(['name' => $name]);
        $value = $statement->fetchColumn();
        $statement->closeCursor();
        if ($value === false) {
            $read = [$pair[0]];
            $write = [];
            $except = [];
            stream_select($read, $write, $except, 0, 10_000);
        }
    } while ($value === false);

    fclose($pair[0]);
    fclose($pair[1]);
}

/** Opens the process harness coordination database. */
function coordinationDatabase(string $workspace): PDO
{
    return new PDO('sqlite:'.$workspace.'/coordination.sqlite', options: [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_TIMEOUT => 5,
    ]);
}
