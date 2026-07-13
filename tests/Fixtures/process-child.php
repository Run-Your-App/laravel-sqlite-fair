<?php

declare(strict_types=1);

use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Database\DatabaseTransactionsManager;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnection;
use RunYourApp\LaravelSqliteFair\Lock\FairSQLiteLock;
use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;
use RunYourApp\LaravelSqliteFair\Wait\PollingWaiter;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;

require getenv('SQLITE_FAIR_PACKAGE_AUTOLOAD');

$workspace = getenv('SQLITE_FAIR_PROCESS_WORKSPACE');
$scenario = getenv('SQLITE_FAIR_PROCESS_SCENARIO');
$arguments = json_decode((string) getenv('SQLITE_FAIR_PROCESS_ARGUMENTS'), true, flags: JSON_THROW_ON_ERROR);

if (! is_string($workspace) || $workspace === '') {
    throw new RuntimeException('The SQLite fair process workspace is missing.');
}
if (! is_array($arguments)) {
    throw new RuntimeException('The SQLite fair process arguments must be an object.');
}

match ($scenario) {
    'lock-reader' => holdLockDatabaseRead($workspace),
    'ticket-mutator' => deleteTicketAfterReaderReleases($workspace),
    'boot' => print $workspace,
    'mutual-writer' => runMutualWriter($workspace, $arguments),
    'stale-recovery' => runStaleRecovery($workspace),
    'pre-commit-crash' => runPreCommitCrash($workspace, $arguments),
    'reclaimed-ticket' => runReclaimedTicket($workspace, $arguments),
    'committed-crash' => runCommittedCrash($workspace, $arguments),
    'fifo' => runFifo($workspace, $arguments),
    'unknown-commit' => runUnknownCommit($workspace),
    'unknown-rollback' => runUnknownRollback($workspace, $arguments),
    'cleanup-failure' => runCleanupFailure($workspace),
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
    $state = (object) ['deletes' => 0, 'blocks' => 0];
    $waiter = new class($workspace, $state) implements Waiter
    {
        public function __construct(private readonly string $workspace, private readonly object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            if ($this->state->deletes !== 0) {
                throw new RuntimeException('The ticket mutation reached DELETE before its competing reader released the lock.');
            }
            $this->state->blocks++;
            signal($this->workspace, 'release-reader');
            waitForSignal($this->workspace, 'reader-released');
        }
    };
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private readonly object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function prepare(string $query, array $options = []): PDOStatement|false
        {
            if (str_starts_with($query, 'DELETE FROM tickets')) {
                $this->state->deletes++;
            }

            return parent::prepare($query, $options);
        }
    };
    $database = new LockDatabase(
        $workspace.'/locks',
        $waiter,
        static fn (): float => hrtime(true) / 1e9,
        $factory,
    );
    $database->deleteExact(1, hrtime(true) / 1e9 + 5.0);
    if ($state->blocks < 1 || $state->deletes !== 1) {
        throw new RuntimeException('The ticket mutation did not wait once before executing exactly one DELETE.');
    }
}

/** Waits briefly without using wall-clock sleep calls. */
function pauseChild(): void
{
    static $waiter;
    $waiter ??= new PollingWaiter();
    $clock = static fn (): float => hrtime(true) / 1e9;
    $waiter->block($clock() + 0.01, $clock);
}

/** @return array{LockDatabase, PDO, FairSQLiteLock, Closure(): float} */
function fairRuntime(string $workspace, float $staleHeadSeconds = 1.0): array
{
    $clock = static fn (): float => hrtime(true) / 1e9;
    $waiter = new PollingWaiter();
    $database = new LockDatabase($workspace.'/locks', $waiter, $clock);
    $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock(
        $app,
        $database,
        $waiter,
        $staleHeadSeconds,
        static function (Throwable $exception): void {},
        static function (): void {},
        $clock,
    );

    return [$database, $app, $lock, $clock];
}

/** @param array<string, mixed> $arguments */
function runMutualWriter(string $workspace, array $arguments): void
{
    [$database, $app, $lock, $clock] = fairRuntime($workspace, 2.0);
    $label = (string) $arguments['label'];
    $ticket = $lock->acquire($clock() + 5.0);
    recordEvent($workspace, 'enter:'.$label);
    pauseChild();
    recordEvent($workspace, 'exit:'.$label);
    $app->commit();
    if ($ticket !== null) {
        $database->deleteExact($ticket);
    }
}

/** Recovers a stale ticket and writes under the acquired application fence. */
function runStaleRecovery(string $workspace): void
{
    $now = 0.0;
    $clock = static function () use (&$now): float {
        return $now += 0.6;
    };
    $waiter = new PollingWaiter();
    $database = new LockDatabase($workspace.'/locks', $waiter, $clock);
    $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, $waiter, 1.0, static function (Throwable $exception): void {}, static function (): void {}, $clock);
    $ticket = $lock->acquire();
    $app->exec("INSERT INTO writes VALUES ('recovered')");
    $app->commit();
    if ($ticket !== null) {
        $database->deleteExact($ticket);
    }
}

/** @param array<string, mixed> $arguments */
function runPreCommitCrash(string $workspace, array $arguments): void
{
    [$database, $app, $lock, $clock] = fairRuntime($workspace);
    if ($arguments['role'] === 'crasher') {
        $lock->acquire($clock() + 5.0);
        $app->exec("INSERT INTO writes VALUES ('crashed')");
        signal($workspace, 'crashed-ready');
        exit(23);
    }
    waitForSignal($workspace, 'crashed-ready');
    $ticket = $lock->acquire($clock() + 5.0);
    $app->exec("INSERT INTO writes VALUES ('follower')");
    $app->commit();
    if ($ticket !== null) {
        $database->deleteExact($ticket);
    }
}

/** @param array<string, mixed> $arguments */
function runReclaimedTicket(string $workspace, array $arguments): void
{
    if ($arguments['role'] === 'reclaimer') {
        $pdo = new PDO('sqlite:'.$workspace.'/locks/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        do {
            $tickets = $pdo->query('SELECT ticket FROM tickets ORDER BY ticket')->fetchAll(PDO::FETCH_COLUMN);
            if (count($tickets) < 2) {
                pauseChild();
            }
        } while (count($tickets) < 2);
        $reclaimed = (int) max($tickets);
        $pdo->exec('PRAGMA busy_timeout=0');
        $pdo->exec('BEGIN IMMEDIATE');
        $statement = $pdo->prepare('DELETE FROM tickets WHERE ticket = :ticket');
        $statement->execute(['ticket' => $reclaimed]);
        $pdo->commit();
        signal($workspace, 'reclaimed', (string) $reclaimed);

        return;
    }

    $now = 0.0;
    $clock = static function () use (&$now, $workspace): float {
        return signalValue($workspace, 'reclaimed') === null ? $now : $now += 0.6;
    };
    $waiter = new PollingWaiter();
    $database = new LockDatabase($workspace.'/locks', $waiter, $clock);
    $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, $waiter, 1.0, static function (Throwable $exception): void {}, static function (): void {}, $clock);
    $ticket = $lock->acquire();
    echo $ticket;
    $app->commit();
    if ($ticket !== null) {
        $database->deleteExact($ticket);
    }
}

/** @param array<string, mixed> $arguments */
function runCommittedCrash(string $workspace, array $arguments): void
{
    $now = 0.0;
    $clock = static function () use (&$now): float {
        return $now += 0.6;
    };
    $waiter = new PollingWaiter();
    $database = new LockDatabase($workspace.'/locks', $waiter, $clock);
    $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, $waiter, 1.0, static function (Throwable $exception): void {}, static function (): void {}, $clock);
    if ($arguments['role'] === 'writer') {
        $ticket = $lock->acquire();
        $app->exec("INSERT INTO writes VALUES ('committed-before-crash')");
        $app->commit();
        signal($workspace, 'committed-crash', (string) $ticket);
        exit(24);
    }
    waitForSignal($workspace, 'committed-crash');
    $ticket = $lock->acquire();
    $app->exec("INSERT INTO writes VALUES ('follower-after-stale')");
    $app->commit();
    if ($ticket !== null) {
        $database->deleteExact($ticket);
    }
}

/** @param array<string, mixed> $arguments */
function runFifo(string $workspace, array $arguments): void
{
    [$database, $app, $lock, $clock] = fairRuntime($workspace, 30.0);
    if ($arguments['role'] === 'holder') {
        $ticket = $lock->acquire($clock() + 10.0);
        if ($ticket !== null) {
            exit(91);
        }
        signal($workspace, 'holder-ready');
        $observer = new PDO('sqlite:'.$workspace.'/locks/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        do {
            $count = (int) $observer->query('SELECT COUNT(*) FROM tickets')->fetchColumn();
            if ($count < 3) {
                pauseChild();
            }
        } while ($count < 3);
        $app->commit();

        return;
    }
    waitForSignal($workspace, 'holder-ready');
    $requiredTickets = (int) $arguments['required_tickets'];
    $observer = new PDO('sqlite:'.$workspace.'/locks/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    do {
        $count = (int) $observer->query('SELECT COUNT(*) FROM tickets')->fetchColumn();
        if ($count < $requiredTickets) {
            pauseChild();
        }
    } while ($count < $requiredTickets);
    $ticket = $lock->acquire($clock() + 10.0);
    $statement = $app->prepare('INSERT INTO fifo (label, ticket) VALUES (:label, :ticket)');
    $statement->execute(['label' => $arguments['label'], 'ticket' => $ticket]);
    $app->commit();
    if ($ticket !== null) {
        $database->deleteExact($ticket);
    }
}

/** Proves that an unknown commit retires the connection identity and releases its PDO. */
function runUnknownCommit(string $workspace): void
{
    $appPath = $workspace.'/unknown-commit.sqlite';
    $lockPath = $workspace.'/unknown-commit-lock';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath, null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]) extends PDO
    {
        public function commit(): bool
        {
            throw new PDOException('commit outcome unknown');
        }
    };
    $weak = WeakReference::create($pdo);
    $config = [
        'driver' => 'fair-sqlite', 'name' => 'unknown-commit', 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $lockPath, 'stale_head_seconds' => 10.0, 'wait_strategy' => 'polling', 'debug' => false,
    ];
    $connection = new FairSQLiteConnection($pdo, $appPath, '', $config, $appPath, $lockPath);
    $manager = new class extends DatabaseTransactionsManager
    {
        /** @return array{pending: int, committed: int, current: bool, callbacks: int} */
        public function state(string $connection): array
        {
            $current = $this->currentTransaction[$connection] ?? null;

            return [
                'pending' => $this->pendingTransactions->count(),
                'committed' => $this->committedTransactions->count(),
                'current' => $current !== null,
                'callbacks' => $current === null ? 0 : count($current->getCallbacks()),
            ];
        }
    };
    $connection->setTransactionManager($manager);
    $pdo = null;
    $callbackRan = false;
    $connection->beginTransaction();
    $connection->afterCommit(static function () use (&$callbackRan): void {
        $callbackRan = true;
    });
    $connection->statement("INSERT INTO writes (value) VALUES ('unknown')");
    $pendingState = $manager->state('unknown-commit');
    try {
        $connection->commit();
        exit(90);
    } catch (PDOException $exception) {
        if ($exception->getMessage() !== 'commit outcome unknown') {
            exit(91);
        }
    }
    if ($connection->transactionLevel() !== 1 || ! $connection->hasUnknownPdoOutcome()) {
        exit(92);
    }
    if ($callbackRan || $pendingState !== ['pending' => 1, 'committed' => 0, 'current' => true, 'callbacks' => 1]) {
        exit(93);
    }
    if ($manager->state('unknown-commit') !== $pendingState) {
        exit(94);
    }
    try {
        $connection->statement("INSERT INTO writes (value) VALUES ('blocked')");
        exit(95);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->transaction(static fn (): string => 'blocked');
        exit(96);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->getPdo();
        exit(97);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->getRawPdo();
        exit(98);
    } catch (FairSQLiteException) {
    }
    $resolverCalls = 0;
    try {
        new FairSQLiteConnection(
            static function () use (&$resolverCalls, $appPath): PDO {
                $resolverCalls++;

                return new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            },
            $appPath,
            '',
            $config,
            $appPath,
            $lockPath,
        );
        exit(99);
    } catch (FairSQLiteException) {
    }
    if ($resolverCalls !== 0) {
        exit(100);
    }
    gc_collect_cycles();
    if ($weak->get() !== null) {
        exit(101);
    }
    echo 'retired-and-released';
}

/** @param array<string, mixed> $arguments */
function runUnknownRollback(string $workspace, array $arguments): void
{
    $mode = (string) $arguments['mode'];
    $appPath = $workspace.'/'.$mode.'.sqlite';
    $lockPath = $workspace.'/'.$mode.'-lock';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath, $mode) extends PDO
    {
        public bool $failRollback = false;

        public function __construct(string $dsn, private readonly string $mode)
        {
            parent::__construct($dsn, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function rollBack(): bool
        {
            if ($this->mode === 'full' || ($this->mode === 'nontransactional' && $this->failRollback)) {
                throw new PDOException($this->mode.' rollback outcome unknown');
            }

            return parent::rollBack();
        }

        public function exec(string $statement): int|false
        {
            if ($this->mode === 'savepoint' && str_starts_with($statement, 'ROLLBACK TO SAVEPOINT')) {
                throw new PDOException('savepoint rollback outcome unknown');
            }

            return parent::exec($statement);
        }
    };
    $weak = WeakReference::create($pdo);
    $config = [
        'driver' => 'fair-sqlite', 'name' => $mode, 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $lockPath, 'stale_head_seconds' => 10.0, 'wait_strategy' => 'polling', 'debug' => false,
    ];
    $connection = new FairSQLiteConnection($pdo, $appPath, '', $config, $appPath, $lockPath);
    if ($mode === 'nontransactional') {
        $connection->runNonTransactional(
            static fn (FairSQLiteConnection $connection): bool => $connection->statement("INSERT INTO writes (value) VALUES ('bootstrap')"),
        );
        $observer = new PDO('sqlite:'.$lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $observer->exec('CREATE TABLE cleanup_audit (attempts INTEGER NOT NULL)');
        $observer->exec('INSERT INTO cleanup_audit VALUES (0)');
        $observer->exec('CREATE TRIGGER count_own_cleanup BEFORE DELETE ON tickets BEGIN UPDATE cleanup_audit SET attempts = attempts + 1; END');
        $observer = null;
        $pdo->failRollback = true;
    }
    $pdo = null;
    $expectedLevel = 1;
    $expectedPrimary = $mode.' rollback outcome unknown';
    try {
        if ($mode === 'savepoint') {
            $connection->beginTransaction();
            $connection->beginTransaction();
            $expectedLevel = 2;
            $expectedPrimary = 'savepoint rollback outcome unknown';
            $connection->rollBack(1);
        } elseif ($mode === 'nontransactional') {
            $expectedLevel = 0;
            $connection->runNonTransactional(static fn (): null => null);
        } else {
            $connection->beginTransaction();
            $connection->rollBack();
        }
        exit(80);
    } catch (Throwable $exception) {
        if ($exception->getMessage() !== $expectedPrimary) {
            exit(81);
        }
    }
    if ($connection->transactionLevel() !== $expectedLevel || ! $connection->hasUnknownPdoOutcome()) {
        exit(82);
    }
    try {
        $connection->select('SELECT 1');
        exit(83);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->beginTransaction();
        exit(84);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->runNonTransactional(static fn (): null => null);
        exit(85);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->withWaitTimeout(1.0, static fn (): null => null);
        exit(86);
    } catch (FairSQLiteException) {
    }
    try {
        $connection->reconnect();
        exit(87);
    } catch (FairSQLiteException) {
    }
    if ($mode === 'nontransactional') {
        $observer = new PDO('sqlite:'.$lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        if ((int) $observer->query('SELECT COUNT(*) FROM tickets')->fetchColumn() !== 0) {
            exit(89);
        }
        if ((int) $observer->query('SELECT attempts FROM cleanup_audit')->fetchColumn() !== 1) {
            exit(90);
        }
        $observer = null;
    }
    gc_collect_cycles();
    if ($weak->get() !== null) {
        exit(88);
    }
    echo $mode;
}

/** Preserves a pre-business primary error while reporting rollback and cleanup failures once. */
function runCleanupFailure(string $workspace): void
{
    $appPath = $workspace.'/cleanup-failure.sqlite';
    $lockPath = $workspace.'/cleanup-failure-lock';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath, null, null, [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]) extends PDO
    {
        public bool $failRollback = false;

        public function rollBack(): bool
        {
            if ($this->failRollback) {
                throw new PDOException('rollback outcome unknown during abort');
            }

            return parent::rollBack();
        }
    };
    $reported = [];
    $handler = new class($reported) implements ExceptionHandler
    {
        /** @param array<int, Throwable> $reported */
        public function __construct(private array &$reported) {}

        public function report(Throwable $e): void
        {
            $this->reported[] = $e;
        }

        public function shouldReport(Throwable $e): bool
        {
            return true;
        }

        public function render($request, Throwable $e): never
        {
            throw $e;
        }

        public function renderForConsole($output, Throwable $e): void {}
    };
    $container = new Container();
    Container::setInstance($container);
    $container->instance(ExceptionHandler::class, $handler);
    $config = [
        'driver' => 'fair-sqlite', 'name' => 'cleanup-failure', 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $lockPath, 'stale_head_seconds' => 0.001, 'wait_strategy' => 'polling', 'debug' => false,
    ];
    $foreignQueue = new LockDatabase($lockPath, new PollingWaiter(), static fn (): float => hrtime(true) / 1e9);
    $foreignQueue->admit();
    $foreignQueue = null;
    $connection = new FairSQLiteConnection($pdo, $appPath, '', $config, $appPath, $lockPath);
    $armUnknownRollback = static function () use ($pdo): void {
        $pdo->failRollback = true;
    };
    $manager = new class($lockPath, $armUnknownRollback) extends DatabaseTransactionsManager
    {
        public function __construct(private readonly string $lockPath, private readonly Closure $armUnknownRollback)
        {
            parent::__construct();
        }

        public function begin($connection, $level): void
        {
            ($this->armUnknownRollback)();
            $lock = new PDO('sqlite:'.$this->lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            $lock->exec('DROP TABLE tickets');

            throw new RuntimeException('transaction manager begin failed');
        }
    };
    $connection->setTransactionManager($manager);
    try {
        $connection->beginTransaction();
        exit(70);
    } catch (RuntimeException $exception) {
        if ($exception->getMessage() !== 'transaction manager begin failed') {
            exit(71);
        }
    }
    if (! $connection->hasUnknownPdoOutcome() || $connection->transactionLevel() !== 0) {
        exit(72);
    }
    $messages = array_map(static fn (Throwable $exception): string => $exception->getMessage(), $reported);
    if (count($messages) !== 2) {
        exit(73);
    }
    if (count(array_filter($messages, static fn (string $message): bool => $message === 'rollback outcome unknown during abort')) !== 1) {
        exit(74);
    }
    if (count(array_filter($messages, static fn (string $message): bool => str_contains($message, 'no such table: tickets'))) !== 1) {
        exit(75);
    }
    echo 'original-priority-and-one-cleanup-report';
}

/** Records a deterministic cross-process signal in the harness coordination database. */
function signal(string $workspace, string $name, string $value = '1'): void
{
    $pdo = coordinationDatabase($workspace);
    $statement = $pdo->prepare('INSERT OR REPLACE INTO signals (name, value) VALUES (:name, :value)');
    $statement->execute(['name' => $name, 'value' => $value]);
    $statement->closeCursor();
}

/** Reads one deterministic cross-process signal value. */
function signalValue(string $workspace, string $name): ?string
{
    $pdo = coordinationDatabase($workspace);
    $statement = $pdo->prepare('SELECT value FROM signals WHERE name = :name');
    $statement->execute(['name' => $name]);
    $value = $statement->fetchColumn();

    return is_string($value) ? $value : null;
}

/** Records one globally ordered cross-process event. */
function recordEvent(string $workspace, string $event): void
{
    $pdo = coordinationDatabase($workspace);
    $statement = $pdo->prepare('INSERT INTO events (event) VALUES (:event)');
    $statement->execute(['event' => $event]);
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
