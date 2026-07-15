<?php

declare(strict_types=1);

use Illuminate\Container\Container;
use Illuminate\Contracts\Debug\ExceptionHandler;
use Illuminate\Database\DatabaseTransactionsManager;
use Illuminate\Support\Facades\Facade;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnection;
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
    'wait-for-missing-signal' => waitForSignal($workspace, 'never-signalled', 30.0),
    'signal-waiter' => runSignalWaiter($workspace),
    'signal-sender' => runSignalSender($workspace),
    'mutual-writer' => runMutualWriter($workspace, $arguments),
    'stale-recovery' => runStaleRecovery($workspace, $arguments),
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

        public function beginContention(): void {}

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

/**
 * Creates the real Laravel connection used by process-level writer scenarios.
 *
 * @param  string  $workspace  Shared process-test workspace containing the application and lock databases.
 * @param  string  $name  Child-local Laravel connection name used in the fair identity.
 * @param  float  $staleHeadSeconds  Monotonic age required before a foreign head is fenced and removed.
 * @param  (Closure(): float)|null  $monotonic  Optional deterministic clock for stale-recovery scenarios.
 * @param  PDO|null  $pdo  Optional fault-injecting application PDO used by crash-boundary scenarios.
 * @param  string|null  $ticketCreatedSignal  Optional persisted barrier emitted by the production debug transition.
 */
function fairConnection(
    string $workspace,
    string $name,
    float $staleHeadSeconds = 1.0,
    ?Closure $monotonic = null,
    ?PDO $pdo = null,
    ?string $ticketCreatedSignal = null,
): FairSQLiteConnection {
    $appPath = $workspace.'/app.sqlite';
    $lockPath = $workspace.'/locks';
    $pdo ??= new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $config = [
        'driver' => 'fair-sqlite',
        'name' => $name,
        'database' => $appPath,
        'prefix' => '',
        'lock_directory' => $lockPath,
        'stale_head_seconds' => $staleHeadSeconds,
        'wait_strategy' => 'polling',
        'debug' => $ticketCreatedSignal !== null,
    ];

    if ($ticketCreatedSignal !== null) {
        installTicketSignalLogger($workspace, $ticketCreatedSignal);
    }

    return new FairSQLiteConnection($pdo, $appPath, '', $config, $appPath, $lockPath, $monotonic);
}

/** Installs a child-local logger that converts the real ticket transition into a SQLite barrier. */
function installTicketSignalLogger(string $workspace, string $ticketCreatedSignal): void
{
    $container = new Container;
    Container::setInstance($container);
    Facade::setFacadeApplication($container);
    $container->instance('log', new class($workspace, $ticketCreatedSignal)
    {
        public function __construct(private readonly string $workspace, private readonly string $ticketCreatedSignal) {}

        /** @param array<string, mixed> $context */
        public function debug(string $message, array $context): void
        {
            if (($context['event'] ?? null) === 'ticket_created') {
                signal($this->workspace, $this->ticketCreatedSignal);
            }
        }
    });
}

/** Returns a deterministic clock that advances on every stale-state observation. */
function advancingClock(): Closure
{
    $now = 0.0;

    return static function () use (&$now): float {
        return $now += 0.6;
    };
}

/** Holds stale observation until both recoverers have joined the same queue head. */
function coordinatedStaleClock(string $workspace, string $label, string $peer): Closure
{
    $now = 0.0;
    $joined = false;

    return static function () use (&$now, &$joined, $workspace, $label, $peer): float {
        if (! $joined) {
            signal($workspace, 'stale-observed-'.$label);
            waitForSignal($workspace, 'stale-observed-'.$peer);
            $joined = true;
        }

        return $now += 0.6;
    };
}

/** @param array<string, mixed> $arguments */
function runMutualWriter(string $workspace, array $arguments): void
{
    $role = (string) $arguments['role'];
    $label = (string) $arguments['label'];
    if ($role === 'contender') {
        waitForSignal($workspace, 'mutual-holder-entered');
    }
    $connection = fairConnection(
        $workspace,
        'mutual-'.$label,
        2.0,
        ticketCreatedSignal: $role === 'contender' ? 'mutual-contender-queued' : null,
    );
    $connection->transaction(static function () use ($connection, $workspace, $role, $label): void {
        recordEvent($workspace, 'enter:'.$label);
        if ($role === 'holder') {
            signal($workspace, 'mutual-holder-entered');
            waitForSignal($workspace, 'mutual-contender-queued');
        }
        $connection->statement('INSERT INTO mutual_writes (label) VALUES (?)', [$label]);
        recordEvent($workspace, 'exit:'.$label);
    });
}

/**
 * Recovers a stale ticket and writes through the real Laravel connection lifecycle.
 *
 * @param  array<string, mixed>  $arguments  Optional unique value written by this recoverer.
 */
function runStaleRecovery(string $workspace, array $arguments): void
{
    $label = isset($arguments['label']) ? (string) $arguments['label'] : 'recovered';
    $peer = isset($arguments['peer']) ? (string) $arguments['peer'] : null;
    $clock = $peer === null ? advancingClock() : coordinatedStaleClock($workspace, $label, $peer);
    $connection = fairConnection(
        $workspace,
        'stale-'.$label,
        monotonic: $clock,
        ticketCreatedSignal: $peer === null ? null : 'stale-ticket-'.$label,
    );
    $connection->transaction(
        static fn (FairSQLiteConnection $connection): bool => $connection->statement(
            'INSERT INTO writes (value) VALUES (?)',
            [$label],
        ),
    );
}

/** @param array<string, mixed> $arguments */
function runPreCommitCrash(string $workspace, array $arguments): void
{
    $role = (string) $arguments['role'];
    $connection = fairConnection($workspace, 'pre-commit-'.$role, monotonic: advancingClock());
    if ($arguments['role'] === 'crasher') {
        $connection->beginTransaction();
        $connection->statement("INSERT INTO writes VALUES ('crashed')");
        signal($workspace, 'crashed-ready');
        exit(23);
    }
    waitForSignal($workspace, 'crashed-ready');
    $connection->transaction(
        static fn (FairSQLiteConnection $connection): bool => $connection->statement(
            "INSERT INTO writes VALUES ('follower')",
        ),
    );
}

/** @param array<string, mixed> $arguments */
function runReclaimedTicket(string $workspace, array $arguments): void
{
    if ($arguments['role'] === 'reclaimer') {
        $pdo = new PDO('sqlite:'.$workspace.'/locks/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        waitForSignal($workspace, 'reclaimed-acquirer-queued');
        $tickets = $pdo->query('SELECT ticket FROM tickets ORDER BY ticket')->fetchAll(PDO::FETCH_COLUMN);
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
    $connection = fairConnection(
        $workspace,
        'reclaimed-acquirer',
        monotonic: $clock,
        ticketCreatedSignal: 'reclaimed-acquirer-queued',
    );
    $connection->transaction(static function () use ($connection, $workspace): void {
        echo currentHeadTicket($workspace);
        $connection->statement("INSERT INTO reclaimed_writes VALUES ('acquired')");
    });
}

/** @param array<string, mixed> $arguments */
function runCommittedCrash(string $workspace, array $arguments): void
{
    if ($arguments['role'] === 'writer') {
        $appPath = $workspace.'/app.sqlite';
        $pdo = new class('sqlite:'.$appPath, $workspace) extends PDO
        {
            public function __construct(string $dsn, private readonly string $workspace)
            {
                parent::__construct($dsn, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function commit(): bool
            {
                parent::commit();
                signal($this->workspace, 'committed-crash');
                exit(24);
            }
        };
        $connection = fairConnection(
            $workspace,
            'committed-crash-writer',
            monotonic: advancingClock(),
            pdo: $pdo,
        );
        $connection->transaction(
            static fn (FairSQLiteConnection $connection): bool => $connection->statement(
                "INSERT INTO writes VALUES ('committed-before-crash')",
            ),
        );

        exit(92);
    }
    waitForSignal($workspace, 'committed-crash');
    $connection = fairConnection($workspace, 'committed-crash-follower', monotonic: advancingClock());
    $connection->transaction(
        static fn (FairSQLiteConnection $connection): bool => $connection->statement(
            "INSERT INTO writes VALUES ('follower-after-stale')",
        ),
    );
}

/** @param array<string, mixed> $arguments */
function runFifo(string $workspace, array $arguments): void
{
    $role = (string) $arguments['role'];
    if ($arguments['role'] === 'holder') {
        $connection = fairConnection($workspace, 'fifo-holder', 30.0);
        $connection->beginTransaction();
        signal($workspace, 'holder-ready-one');
        signal($workspace, 'holder-ready-two');
        signal($workspace, 'holder-ready-three');
        waitForSignal($workspace, 'fifo-ticket-three');
        $connection->commit();

        return;
    }
    $requiredTickets = (int) $arguments['required_tickets'];
    $previousLabel = match ($requiredTickets) {
        0 => null,
        1 => 'one',
        2 => 'two',
        default => throw new RuntimeException('The FIFO process prerequisite is invalid.'),
    };
    $label = (string) $arguments['label'];
    waitForSignal($workspace, 'holder-ready-'.$label);
    if ($previousLabel !== null) {
        waitForSignal($workspace, 'fifo-ticket-'.$previousLabel);
    }
    $connection = fairConnection(
        $workspace,
        'fifo-writer-'.$label,
        30.0,
        ticketCreatedSignal: 'fifo-ticket-'.$label,
    );
    $connection->transaction(static function () use ($connection, $workspace, $arguments): void {
        $connection->statement(
            'INSERT INTO fifo (label, ticket) VALUES (?, ?)',
            [$arguments['label'], currentHeadTicket($workspace)],
        );
    });
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
    $container = new Container;
    Container::setInstance($container);
    $container->instance(ExceptionHandler::class, $handler);
    $config = [
        'driver' => 'fair-sqlite', 'name' => 'cleanup-failure', 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $lockPath, 'stale_head_seconds' => 0.001, 'wait_strategy' => 'polling', 'debug' => false,
    ];
    $foreignQueue = new LockDatabase($lockPath, new PollingWaiter, static fn (): float => hrtime(true) / 1e9);
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

/** Waits on a real listener and reports the persisted signal notification. */
function runSignalWaiter(string $workspace): void
{
    waitForSignal($workspace, 'roundtrip');
    echo 'notified';
}

/** Waits until the listener is registered, then persists and delivers its signal. */
function runSignalSender(string $workspace): void
{
    waitForSignal($workspace, '__waiter__:roundtrip', 2.0);
    signal($workspace, 'roundtrip');
    echo 'sent';
}

/** Records a deterministic cross-process signal and wakes its registered listener. */
function signal(string $workspace, string $name, string $value = '1'): void
{
    $pdo = coordinationDatabase($workspace);
    $statement = $pdo->prepare('INSERT OR REPLACE INTO signals (name, value) VALUES (:name, :value)');
    $statement->execute(['name' => $name, 'value' => $value]);
    $statement->closeCursor();

    $address = signalValue($workspace, '__waiter__:'.$name);
    if ($address === null) {
        return;
    }

    $errorCode = 0;
    $errorMessage = '';
    $connection = @stream_socket_client(
        'tcp://'.$address,
        $errorCode,
        $errorMessage,
        1.0,
        STREAM_CLIENT_CONNECT,
    );
    if ($connection === false) {
        return;
    }
    fwrite($connection, '1');
    fclose($connection);
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

/** Returns the committed queue head while a fair transaction owns it. */
function currentHeadTicket(string $workspace): int
{
    $observer = new PDO(
        'sqlite:'.$workspace.'/locks/lock.sqlite',
        options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION],
    );
    $ticket = $observer->query('SELECT ticket FROM tickets ORDER BY ticket LIMIT 1')->fetchColumn();
    if (! is_int($ticket) && ! is_string($ticket)) {
        throw new RuntimeException('The process scenario expected one committed queue head.');
    }

    return (int) $ticket;
}

/** Records one globally ordered cross-process event. */
function recordEvent(string $workspace, string $event): void
{
    $pdo = coordinationDatabase($workspace);
    $statement = $pdo->prepare('INSERT INTO events (event) VALUES (:event)');
    $statement->execute(['event' => $event]);
}

/** Waits boundedly on a real TCP listener for one persisted SQLite signal. */
function waitForSignal(string $workspace, string $name, float $timeoutSeconds = 10.0): void
{
    if (! is_finite($timeoutSeconds) || $timeoutSeconds <= 0.0) {
        throw new RuntimeException('The process coordination timeout must be finite and positive.');
    }
    if (signalValue($workspace, $name) !== null) {
        return;
    }

    $errorCode = 0;
    $errorMessage = '';
    $listener = @stream_socket_server(
        'tcp://127.0.0.1:0',
        $errorCode,
        $errorMessage,
        STREAM_SERVER_BIND | STREAM_SERVER_LISTEN,
    );
    if ($listener === false) {
        throw new RuntimeException("The SQLite fair process barrier [{$name}] could not open its listener.");
    }
    $address = stream_socket_get_name($listener, false);
    if (! is_string($address) || $address === '') {
        fclose($listener);
        throw new RuntimeException("The SQLite fair process barrier [{$name}] has no listener address.");
    }
    $registration = '__waiter__:'.$name;
    $deadline = hrtime(true) / 1e9 + $timeoutSeconds;
    signal($workspace, $registration, $address);
    try {
        if (signalValue($workspace, $name) !== null) {
            return;
        }
        $remaining = max(0.0, $deadline - hrtime(true) / 1e9);
        $notification = @stream_socket_accept($listener, $remaining);
        if ($notification !== false) {
            fclose($notification);
        }
        if (signalValue($workspace, $name) === null) {
            throw new RuntimeException("The SQLite fair process barrier [{$name}] was not reached.");
        }
    } finally {
        $pdo = coordinationDatabase($workspace);
        $statement = $pdo->prepare('DELETE FROM signals WHERE name = :name AND value = :value');
        $statement->execute(['name' => $registration, 'value' => $address]);
        fclose($listener);
    }
}

/** Opens the process harness coordination database. */
function coordinationDatabase(string $workspace): PDO
{
    return new PDO('sqlite:'.$workspace.'/coordination.sqlite', options: [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_TIMEOUT => 5,
    ]);
}
