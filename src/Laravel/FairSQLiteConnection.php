<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Laravel;

use Closure;
use Illuminate\Database\DeadlockException;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\Facades\Log;
use LogicException;
use PDO;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Lock\FairSQLiteLock;
use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;
use RunYourApp\LaravelSqliteFair\Wait\WaiterFactory;
use Throwable;

/**
 * Coordinates every file-backed Laravel write through one fair SQLite lifecycle.
 *
 * `FairSQLiteConnector` creates this connection for the `fair-sqlite` driver. Application code keeps using Laravel's
 * callback, manual, statement, query-builder, and Eloquent APIs while this class owns their shared ticket, application
 * fence, cleanup, bounded wait, and unknown-PDO retirement state.
 */
final class FairSQLiteConnection extends SQLiteConnection
{
    /** @var array<string, true> */
    private static array $unknownPdoOutcomes = [];

    /** @var array<string, array{app: string, lock: string}> */
    private static array $identitiesByName = [];

    /** @var array<string, string> */
    private static array $identityKeysByAppPath = [];

    /** @var array<string, string> */
    private static array $identityKeysByLockPath = [];

    /** @var callable(): float */
    private $monotonic;

    private ?LockDatabase $lockDatabase = null;

    private ?FairSQLiteLock $fairLock;

    private readonly string $identityKey;

    private readonly string $lockPath;

    private readonly string $waitStrategy;

    private readonly float $staleHeadSeconds;

    private readonly bool $debug;

    private ?int $activeTicket = null;

    private bool $fairFenceHeld = false;

    private bool $nonTransactionalScope = false;

    private int $nonTransactionalWrites = 0;

    private int $waitScopeDepth = 0;

    private int $waitScopeTopLevelCalls = 0;

    private ?float $waitScopeDeadline = null;

    /**
     * Creates a file-backed Laravel connection with fair writer coordination.
     *
     * The package connector supplies canonical application and lock paths plus validated waiter configuration. This
     * constructor rejects ambiguous or retired identities before installing the private lock database and host waiter.
     * The optional clock exists only for deterministic package verification.
     *
     * @param  PDO|Closure  $pdo  Eager application PDO or Laravel PDO resolver.
     * @param  string  $database  Canonical application database path used by Laravel.
     * @param  string  $tablePrefix  Laravel table prefix retained for query and schema builders.
     * @param  array<string, mixed>  $config  Validated Laravel and Fair SQLite connection values.
     * @param  string  $appPath  Canonical application database path used in the process identity.
     * @param  string  $lockPath  Canonical directory containing the private ticket database.
     * @param  (callable(): float)|null  $monotonic  Optional monotonic seconds source for package verification.
     * @return void The constructed connection owns its validated fair runtime immediately.
     *
     * @throws FairSQLiteException When the name, identity, or fair configuration cannot be used safely.
     * @throws Throwable When PDO, lock-database bootstrap, or native waiter startup fails.
     *
     * @internal Laravel applications obtain this connection through the `fair-sqlite` driver.
     */
    public function __construct(
        PDO|Closure $pdo,
        string $database,
        string $tablePrefix,
        array $config,
        string $appPath,
        string $lockPath,
        ?callable $monotonic = null,
    ) {
        $name = $config['name'] ?? null;
        if (! is_string($name) || $name === '') {
            throw new FairSQLiteException('A fair SQLite connection requires a non-empty connection name.');
        }

        self::assertIdentityConfiguration($name, $appPath, $lockPath);
        self::assertIdentityIsUsable($name, $appPath, $lockPath);
        $this->identityKey = self::identityKey($name, $appPath, $lockPath);

        $strategy = $config['wait_strategy'] ?? null;
        $staleHeadSeconds = $config['stale_head_seconds'] ?? null;
        // The public connector always supplies this validated value; false keeps the internal direct-construction seam quiet.
        $debug = $config['debug'] ?? false;
        if (! is_string($strategy)
            || (! is_int($staleHeadSeconds) && ! is_float($staleHeadSeconds))
            || ! is_bool($debug)) {
            throw new FairSQLiteException('A fair SQLite connection requires validated wait configuration.');
        }
        $this->lockPath = $lockPath;
        $this->waitStrategy = $strategy;
        $this->staleHeadSeconds = (float) $staleHeadSeconds;
        $this->debug = $debug;

        parent::__construct($pdo, $database, $tablePrefix, $config);

        $clock = $monotonic ?? static fn (): float => hrtime(true) / 1e9;
        $this->monotonic = $clock;
        $this->installFairRuntime(parent::getPdo());
    }

    /**
     * Registers one deterministic connection identity before PDO is opened.
     *
     * A connection name, application path, or lock path may belong to only one exact identity in the current process.
     * The connector calls this guard before constructing PDO so aliases cannot create competing lock owners.
     *
     * @param  string  $name  Non-empty Laravel connection name.
     * @param  string  $appPath  Canonical application database path.
     * @param  string  $lockPath  Canonical lock directory path.
     * @return void The identity is registered for the lifetime of the current process.
     *
     * @throws FairSQLiteException When an identity component conflicts with a previously registered identity.
     *
     * @internal
     */
    public static function assertIdentityConfiguration(string $name, string $appPath, string $lockPath): void
    {
        $key = self::identityKey($name, $appPath, $lockPath);
        $existing = self::$identitiesByName[$name] ?? null;
        if ($existing !== null && ($existing['app'] !== $appPath || $existing['lock'] !== $lockPath)) {
            throw new FairSQLiteException("SQLite fair connection [{$name}] was already configured for different paths.");
        }
        if (isset(self::$identityKeysByAppPath[$appPath]) && self::$identityKeysByAppPath[$appPath] !== $key) {
            throw new FairSQLiteException('The SQLite application path belongs to a different fair connection identity.');
        }
        if (isset(self::$identityKeysByLockPath[$lockPath]) && self::$identityKeysByLockPath[$lockPath] !== $key) {
            throw new FairSQLiteException('The SQLite lock path belongs to a different fair connection identity.');
        }

        self::$identitiesByName[$name] = ['app' => $appPath, 'lock' => $lockPath];
        self::$identityKeysByAppPath[$appPath] = $key;
        self::$identityKeysByLockPath[$lockPath] = $key;
    }

    /**
     * Rejects a connection identity retired after an unknown PDO outcome.
     *
     * The connector calls this guard before opening a replacement PDO, preventing purge, reconnect, or a new connection
     * object from continuing after an outer commit or rollback whose physical result is unknown in this process.
     *
     * @param  string  $name  Laravel connection name in the deterministic identity.
     * @param  string  $appPath  Canonical application database path in the identity.
     * @param  string  $lockPath  Canonical lock directory path in the identity.
     * @return void The method returns only when the identity remains safe to use.
     *
     * @throws FairSQLiteException When the exact identity requires process recycling.
     *
     * @internal
     */
    public static function assertIdentityIsUsable(string $name, string $appPath, string $lockPath): void
    {
        if (isset(self::$unknownPdoOutcomes[self::identityKey($name, $appPath, $lockPath)])) {
            throw new FairSQLiteException('This SQLite fair connection is retired after an unknown PDO outcome.');
        }
    }

    /**
     * Reports whether this connection identity requires process recycling.
     *
     * Long-running Laravel runtime owners consume this read-only status. Calling it does not open PDO, clear the
     * process registry, or repair the retired connection.
     *
     * @return bool Whether the current process must stop using this identity.
     *
     * @internal
     */
    public function hasUnknownPdoOutcome(): bool
    {
        return isset(self::$unknownPdoOutcomes[$this->identityKey]);
    }

    /**
     * Runs a Laravel transaction through fair writer acquisition.
     *
     * An outer transaction acquires the application writer fence before invoking the callback; nested transactions use
     * Laravel savepoints under that acquisition. Laravel-classified callback concurrency failures may repeat only after
     * a known successful rollback and cleanup. Commit and unknown rollback outcomes are never replayed.
     *
     * @template TReturn
     *
     * @param  Closure(static): TReturn  $callback  Receives this connection after the transaction begins.
     * @param  int  $attempts  Maximum Laravel-classified callback concurrency attempts.
     * @return TReturn The callback result after a known successful commit.
     *
     * @throws Throwable When acquisition, the callback, rollback, cleanup, commit, or Laravel finalization fails.
     */
    public function transaction(Closure $callback, $attempts = 1)
    {
        for ($currentAttempt = 1; $currentAttempt <= $attempts; $currentAttempt++) {
            $this->beginTransaction();

            try {
                $result = $callback($this);
            } catch (Throwable $exception) {
                if ($this->causedByConcurrencyError($exception) && $this->transactions > 1) {
                    $this->transactions--;
                    $this->transactionsManager?->rollback($this->connectionName(), $this->transactions);

                    throw new DeadlockException(
                        $exception->getMessage(),
                        is_int($exception->getCode()) ? $exception->getCode() : 0,
                        $exception,
                    );
                }

                $rollbackSucceeded = true;
                try {
                    $this->rollBack();
                } catch (Throwable $rollback) {
                    $rollbackSucceeded = false;
                    report($rollback);
                }

                if ($rollbackSucceeded
                    && $this->causedByConcurrencyError($exception)
                    && $currentAttempt < $attempts) {
                    continue;
                }

                throw $exception;
            }

            $this->commit();

            return $result;
        }

        throw new LogicException('The fair SQLite transaction attempt loop ended without a result.');
    }

    /**
     * Begins a manual Laravel transaction through the fair writer lifecycle.
     *
     * The outer level acquires and holds one application writer fence with an optional queue ticket before Laravel's
     * transaction manager and beginning event are updated. Nested levels create savepoints without a second ticket. A
     * pre-business setup failure leaves no installed local fair scope.
     *
     * @return void The transaction is active and represented in Laravel's transaction state.
     *
     * @throws Throwable When acquisition, PDO, savepoint, manager, or event setup fails.
     */
    public function beginTransaction(): void
    {
        $this->assertUsable();
        if ($this->nonTransactionalScope) {
            throw new LogicException('A transaction cannot start inside runNonTransactional().');
        }
        foreach ($this->beforeStartingTransaction as $callback) {
            $callback($this);
        }

        if ($this->pretending()) {
            $this->transactions++;
            $this->transactionsManager?->begin($this->connectionName(), $this->transactions);
            $this->fireConnectionEvent('beganTransaction');

            return;
        }

        if ($this->transactions > 0) {
            try {
                if ($this->queryGrammar->supportsSavepoints()) {
                    $this->createSavepoint();
                }
            } catch (Throwable $exception) {
                $this->retireUnknownOutcome($exception);
                throw $exception;
            }
            $this->transactions++;
            $this->transactionsManager?->begin($this->connectionName(), $this->transactions);
            $this->fireConnectionEvent('beganTransaction');

            return;
        }

        $ticket = $this->fairLock()->acquire($this->consumeWaitDeadline());
        $this->activeTicket = $ticket;
        $this->fairFenceHeld = true;
        try {
            $this->transactions++;
            $this->transactionsManager?->begin($this->connectionName(), $this->transactions);
            $this->fireConnectionEvent('beganTransaction');
        } catch (Throwable $exception) {
            $this->transactions = 0;
            $this->abortInstalledScope();
            throw $exception;
        }
    }

    /**
     * Commits the current Laravel transaction level.
     *
     * Nested commits update only Laravel's level and manager state. An outer commit writes through PDO first, then
     * removes an owned queue ticket and emits Laravel's committed state. Cleanup failure after a known PDO commit is
     * logged without changing that success; a PDO commit exception retires this process identity.
     *
     * @return void The current transaction level is committed and finalized in Laravel.
     *
     * @throws Throwable When the outer PDO commit has an unknown outcome or Laravel finalization fails.
     */
    public function commit(): void
    {
        $this->assertUsable();
        if ($this->pretending()) {
            $this->finishFrameworkCommit();

            return;
        }
        if ($this->transactions !== 1) {
            $this->finishFrameworkCommit();

            return;
        }

        $levelBeingCommitted = $this->transactions;
        $this->fireConnectionEvent('committing');
        try {
            parent::getPdo()->commit();
        } catch (Throwable $exception) {
            $this->retireUnknownOutcome($exception);
            throw $exception;
        }

        $this->fairFenceHeld = false;
        $this->cleanupAfterPersistedSuccess();
        $this->transactions = 0;
        $this->transactionsManager?->commit($this->connectionName(), $levelBeingCommitted, 0);
        $this->fireConnectionEvent('committed');
    }

    /**
     * Rolls the current Laravel transaction back to an earlier level.
     *
     * A null target selects the preceding level. Invalid targets are no-ops, nested targets use Laravel savepoints, and
     * target zero rolls back the outer PDO transaction before cleaning its optional ticket and finalizing manager state.
     * Any PDO rollback exception retires the identity without pretending Laravel state was finalized.
     *
     * @param  int|null  $toLevel  Zero-based target level, or null for the preceding level.
     * @return void The requested valid rollback level is reflected in Laravel's transaction state.
     *
     * @throws Throwable When PDO rollback, ticket cleanup, or Laravel finalization fails.
     */
    public function rollBack($toLevel = null): void
    {
        $this->assertUsable();
        $target = $toLevel === null ? $this->transactions - 1 : $toLevel;
        if ($target < 0 || $target >= $this->transactions) {
            return;
        }
        if ($this->pretending()) {
            $this->transactions = $target;
            $this->transactionsManager?->rollback($this->connectionName(), $target);
            $this->fireConnectionEvent('rollingBack');

            return;
        }

        try {
            if ($target === 0) {
                parent::getPdo()->rollBack();
            } elseif ($this->queryGrammar->supportsSavepoints()) {
                parent::getPdo()->exec($this->queryGrammar->compileSavepointRollBack('trans'.($target + 1)));
            }
        } catch (Throwable $exception) {
            $this->retireUnknownOutcome($exception);
            throw $exception;
        }

        $cleanup = null;
        if ($target === 0) {
            $this->fairFenceHeld = false;
            try {
                $this->deleteActiveTicket();
            } catch (Throwable $exception) {
                $cleanup = $exception;
            }
            $this->clearFairScope();
        }

        $this->transactions = $target;
        $this->transactionsManager?->rollback($this->connectionName(), $target);
        $this->fireConnectionEvent('rollingBack');

        if ($cleanup !== null) {
            throw $cleanup;
        }
    }

    /**
     * Executes a Laravel statement through fair writer ownership.
     *
     * Outside a transaction the statement receives its own outer fair transaction. Inside a transaction it reuses the
     * active fence. Leading `BEGIN`, `COMMIT`, and `ROLLBACK` SQL is rejected before state changes.
     *
     * @param  string  $query  SQL statement Laravel should prepare and execute.
     * @param  array<array-key, mixed>  $bindings  Values bound by Laravel to the statement.
     * @return bool Whether Laravel executed the statement successfully.
     *
     * @throws Throwable When transaction-control SQL is supplied or fair acquisition, SQL, commit, or rollback fails.
     */
    public function statement($query, $bindings = []): bool
    {
        $this->rejectTransactionControl($query);

        return $this->runFairWrite(fn (): bool => parent::statement($query, $bindings));
    }

    /**
     * Executes an affecting Laravel statement through fair writer ownership.
     *
     * Outside a transaction the statement receives its own outer fair transaction; an active transaction reuses its
     * fence and optional ticket. Leading transaction-control SQL is rejected before execution.
     *
     * @param  string  $query  SQL statement Laravel should prepare and execute.
     * @param  array<array-key, mixed>  $bindings  Values bound by Laravel to the statement.
     * @return int Number of rows PDO reports as affected.
     *
     * @throws Throwable When transaction-control SQL is supplied or fair acquisition, SQL, commit, or rollback fails.
     */
    public function affectingStatement($query, $bindings = []): int
    {
        $this->rejectTransactionControl($query);

        return $this->runFairWrite(fn (): int => parent::affectingStatement($query, $bindings));
    }

    /**
     * Executes one unprepared Laravel statement through fair writer ownership.
     *
     * Outside a transaction the statement receives its own outer fair transaction. This method never splits SQL and
     * rejects only a leading `BEGIN`, `COMMIT`, or `ROLLBACK` token before execution.
     *
     * @param  string  $query  Complete SQL statement passed to Laravel without bindings.
     * @return bool Whether PDO executed the statement successfully.
     *
     * @throws Throwable When transaction-control SQL is supplied or fair acquisition, SQL, commit, or rollback fails.
     */
    public function unprepared($query): bool
    {
        $this->rejectTransactionControl($query);

        return $this->runFairWrite(fn (): bool => parent::unprepared($query));
    }

    /**
     * Runs exactly one nontransactional write while its queue ticket remains owned.
     *
     * This method always joins the ticket queue, releases its temporary application fence, and invokes the callback in
     * a scope that permits exactly one write call. The command is never replayed. Cleanup failure after a known
     * successful write is logged while the callback result remains successful.
     *
     * @template TReturn
     *
     * @param  Closure(static): TReturn  $callback  Executes exactly one write with this connection.
     * @return TReturn The callback result after the write and local scope finalization.
     *
     * @throws Throwable When the scope is nested, the callback performs other than one write, or acquisition or cleanup fails.
     */
    public function runNonTransactional(Closure $callback): mixed
    {
        $this->assertUsable();
        if ($this->transactions > 0 || $this->nonTransactionalScope) {
            throw new LogicException('runNonTransactional() requires an idle fair SQLite connection.');
        }

        $ticket = $this->fairLock()->acquireQueued($this->consumeWaitDeadline());
        $this->activeTicket = $ticket;
        $this->fairFenceHeld = true;
        try {
            parent::getPdo()->rollBack();
            $this->fairFenceHeld = false;
        } catch (Throwable $exception) {
            $this->retireUnknownOutcome($exception);
            throw $exception;
        }

        $this->nonTransactionalScope = true;
        $this->nonTransactionalWrites = 0;
        try {
            $result = $callback($this);
            if ($this->consumeNonTransactionalWriteCount() !== 1) {
                throw new LogicException('runNonTransactional() must execute exactly one write.');
            }
        } catch (Throwable $exception) {
            $this->cleanupAfterOriginalFailure();
            throw $exception;
        }

        $this->nonTransactionalScope = false;
        try {
            $this->deleteActiveTicket();
        } catch (Throwable $cleanup) {
            $this->debug('cleanup_failed', ['operation' => 'non_transactional']);
            report($cleanup);
        }
        $this->clearFairScope();

        return $result;
    }

    /**
     * Applies one bounded writer wait to exactly one top-level fair operation.
     *
     * Nested timeout scopes share the earliest absolute monotonic deadline. A successful callback must start exactly
     * one top-level fair operation, and all timeout state is restored in `finally`. Expiry occurs before business SQL and raises a
     * typed `FairWaitTimeoutException`.
     *
     * @template TReturn
     *
     * @param  float  $seconds  Finite positive seconds available for fair acquisition.
     * @param  Closure(static): TReturn  $callback  Starts exactly one top-level fair operation on this connection.
     * @return TReturn The callback result when acquisition and the operation succeed.
     *
     * @throws LogicException When the duration is invalid or a successful scope does not start exactly one top-level fair operation.
     * @throws FairWaitTimeoutException When the earliest scope deadline expires before business SQL.
     * @throws Throwable When the callback or its database operation fails.
     */
    public function withWaitTimeout(float $seconds, Closure $callback): mixed
    {
        $this->assertUsable();
        if (! is_finite($seconds) || $seconds <= 0.0) {
            throw new LogicException('The fair SQLite wait timeout must be finite and positive.');
        }

        $clock = $this->monotonic;
        $previousDeadline = $this->waitScopeDeadline;
        $outermost = $this->waitScopeDepth === 0;
        if ($outermost) {
            $this->waitScopeTopLevelCalls = 0;
        }
        $deadline = $clock() + $seconds;
        $this->waitScopeDeadline = $previousDeadline === null ? $deadline : min($previousDeadline, $deadline);
        $this->waitScopeDepth++;

        try {
            $result = $callback($this);
            $this->assertCompletedWaitScope($outermost);

            return $result;
        } finally {
            $this->waitScopeDepth--;
            $this->waitScopeDeadline = $previousDeadline;
            if ($outermost) {
                $this->waitScopeTopLevelCalls = 0;
            }
        }
    }

    /**
     * Records Laravel pretend queries without acquiring SQLite writer state.
     *
     * Pretend transactions update only Laravel's temporary transaction and query-log state. They do not open the lock
     * database, acquire a fence, create a ticket, or persist application SQL.
     *
     * @param  Closure(\Illuminate\Database\Connection): mixed  $callback  Laravel query operations to record without execution.
     * @return array<int, array{query: string, bindings: array<array-key, mixed>, time: float|null}> Recorded Laravel query log.
     *
     * @throws FairSQLiteException When the connection identity has already been retired.
     * @throws Throwable When the pretend callback fails.
     */
    public function pretend(Closure $callback): array
    {
        $this->assertUsable();

        return parent::pretend($callback);
    }

    /**
     * Reconnects through Laravel only while this process identity remains usable.
     *
     * Retirement is checked before Laravel invokes its PDO resolver, so an unknown commit or rollback outcome cannot be
     * bypassed by purge or reconnect in the same process.
     *
     * @return mixed Laravel's reconnect result.
     *
     * @throws FairSQLiteException When the identity has been retired.
     * @throws Throwable When Laravel cannot create the replacement PDO.
     */
    public function reconnect(): mixed
    {
        $this->assertUsable();

        return parent::reconnect();
    }

    /**
     * Disconnects the application PDO without clearing process retirement state.
     *
     * A retired identity keeps fail-fast PDO resolvers installed so later access cannot silently continue. A usable
     * connection delegates normal disconnection to Laravel.
     *
     * @return void The active PDO is released without clearing any retirement marker.
     */
    public function disconnect(): void
    {
        if ($this->hasUnknownPdoOutcome()) {
            $this->disconnectAfterUnknownOutcome();

            return;
        }

        parent::disconnect();
    }

    /**
     * Returns Laravel's write PDO only while the connection identity is usable.
     *
     * @return PDO Active application write PDO.
     *
     * @throws FairSQLiteException When the identity has been retired.
     * @throws Throwable When Laravel's PDO resolver fails.
     */
    public function getPdo(): PDO
    {
        $this->assertUsable();

        return parent::getPdo();
    }

    /**
     * Returns Laravel's read PDO only while the connection identity is usable.
     *
     * @return PDO Active application read PDO.
     *
     * @throws FairSQLiteException When the identity has been retired.
     * @throws Throwable When Laravel's PDO resolver fails.
     */
    public function getReadPdo(): PDO
    {
        $this->assertUsable();

        return parent::getReadPdo();
    }

    /**
     * Returns Laravel's unresolved or active write PDO without bypassing retirement.
     *
     * @return PDO|Closure|null Active PDO, deferred PDO resolver, or null after normal disconnection.
     *
     * @throws FairSQLiteException When the identity has been retired.
     */
    public function getRawPdo(): PDO|Closure|null
    {
        $this->assertUsable();

        return parent::getRawPdo();
    }

    /**
     * Returns Laravel's unresolved or active read PDO without bypassing retirement.
     *
     * @return PDO|Closure|null Active read PDO, deferred resolver, or null after normal disconnection.
     *
     * @throws FairSQLiteException When the identity has been retired.
     */
    public function getRawReadPdo(): PDO|Closure|null
    {
        $this->assertUsable();

        return parent::getRawReadPdo();
    }

    /**
     * Replaces Laravel's PDO and installs matching Fair SQLite coordination state.
     *
     * A concrete PDO receives a fresh waiter, lock-database handle, and application fence owner. A resolver or null
     * clears the current fair runtime until Laravel supplies a concrete PDO. Retirement always blocks replacement.
     *
     * @param  PDO|Closure|null  $pdo  Concrete application PDO, deferred Laravel resolver, or null.
     * @return static This connection after replacing its PDO state.
     *
     * @throws FairSQLiteException When the identity has been retired or coordination cannot be installed.
     * @throws Throwable When lock-database or native waiter startup fails for a concrete PDO.
     */
    public function setPdo($pdo): static
    {
        $this->assertUsable();

        parent::setPdo($pdo);
        if ($pdo instanceof PDO) {
            $this->installFairRuntime($pdo);
        } else {
            $this->fairLock = null;
            $this->lockDatabase = null;
        }

        return $this;
    }

    /** @param array<array-key, mixed> $bindings */
    protected function run($query, $bindings, Closure $callback)
    {
        $this->assertUsable();

        return parent::run($query, $bindings, $callback);
    }

    /** @template TReturn
     * @param  Closure(): TReturn  $write
     * @return TReturn
     */
    private function runFairWrite(Closure $write): mixed
    {
        $this->assertUsable();
        if ($this->pretending() || $this->transactions > 0) {
            return $write();
        }
        if ($this->nonTransactionalScope) {
            if ($this->nonTransactionalWrites >= 1) {
                throw new LogicException('runNonTransactional() permits exactly one write.');
            }
            $result = $write();
            $this->nonTransactionalWrites++;

            return $result;
        }

        return $this->transaction(fn (): mixed => $write());
    }

    private function rejectTransactionControl(mixed $query): void
    {
        if (is_string($query) && preg_match('/^\s*(BEGIN|COMMIT|ROLLBACK)\b/i', $query) === 1) {
            throw new LogicException('Direct SQL transaction control is not supported by the fair SQLite connection.');
        }
    }

    private function consumeWaitDeadline(): ?float
    {
        if ($this->waitScopeDepth === 0) {
            return null;
        }
        $this->waitScopeTopLevelCalls++;
        if ($this->waitScopeTopLevelCalls > 1) {
            throw new LogicException('A wait-timeout scope permits exactly one top-level fair operation.');
        }

        $clock = $this->monotonic;
        $deadline = (float) $this->waitScopeDeadline;
        if ($deadline <= $clock()) {
            $this->debug('wait_timeout', ['operation' => 'wait_scope']);
            throw new FairWaitTimeoutException('The SQLite fair writer wait deadline expired.');
        }

        return $deadline;
    }

    /** Require exactly one top-level fair call after a successful outer wait scope. */
    private function assertCompletedWaitScope(bool $outermost): void
    {
        if ($outermost && $this->waitScopeTopLevelCalls !== 1) {
            throw new LogicException('A wait-timeout scope permits exactly one top-level fair operation.');
        }
    }

    private function finishFrameworkCommit(): void
    {
        [$levelBeingCommitted, $this->transactions] = [
            $this->transactions,
            max(0, $this->transactions - 1),
        ];
        $this->transactionsManager?->commit($this->connectionName(), $levelBeingCommitted, $this->transactions);
        $this->fireConnectionEvent('committed');
    }

    private function abortInstalledScope(): void
    {
        $this->fairLock()->abortBeforeBusiness($this->fairFenceHeld, $this->activeTicket);
        $this->clearFairScope();
    }

    private function cleanupAfterPersistedSuccess(): void
    {
        try {
            $this->deleteActiveTicket();
        } catch (Throwable $cleanup) {
            $this->debug('cleanup_failed', ['operation' => 'persisted_success']);
            report($cleanup);
        }
        $this->clearFairScope();
    }

    private function cleanupAfterOriginalFailure(): void
    {
        $this->nonTransactionalScope = false;
        try {
            $this->deleteActiveTicket();
        } catch (Throwable $cleanup) {
            $this->debug('cleanup_failed', ['operation' => 'original_failure']);
            report($cleanup);
        }
        $this->clearFairScope();
    }

    private function deleteActiveTicket(): void
    {
        if ($this->activeTicket !== null) {
            $this->lockDatabase()->deleteExact($this->activeTicket);
        }
    }

    private function clearFairScope(): void
    {
        $this->activeTicket = null;
        $this->fairFenceHeld = false;
        $this->nonTransactionalScope = false;
        $this->nonTransactionalWrites = 0;
    }

    private function retireUnknownOutcome(Throwable $primary): void
    {
        $fairLock = $this->fairLock;
        $this->debug('unknown_pdo_outcome', ['operation' => 'app_pdo']);
        $this->markUnknownPdoOutcome($primary);
        $this->disconnectAfterUnknownOutcome();
        if ($this->activeTicket !== null && $fairLock !== null) {
            try {
                $fairLock->cleanupOwnTicket($this->activeTicket);
            } catch (Throwable $cleanup) {
                $this->debug('cleanup_failed', ['operation' => 'unknown_outcome']);
                report($cleanup);
            }
        }
        $this->clearFairScope();
    }

    private function markUnknownPdoOutcome(Throwable $exception): null
    {
        self::$unknownPdoOutcomes[$this->identityKey] = true;

        return null;
    }

    private function disconnectAfterUnknownOutcome(): null
    {
        // Avoid Connection::setPdo(), which would erase the Laravel transaction level.
        $retired = static fn (): PDO => throw new FairSQLiteException(
            'This SQLite fair connection is retired after an unknown PDO outcome.',
        );
        $this->pdo = $retired;
        $this->readPdo = $retired;
        $this->fairLock = null;
        $this->lockDatabase = null;

        return null;
    }

    private function assertUsable(): void
    {
        if ($this->hasUnknownPdoOutcome()) {
            throw new FairSQLiteException('This SQLite fair connection is retired after an unknown PDO outcome.');
        }
    }

    private function fairLock(): FairSQLiteLock
    {
        $this->assertUsable();
        if ($this->fairLock === null) {
            throw new FairSQLiteException('The SQLite fair lock lifecycle is unavailable.');
        }

        return $this->fairLock;
    }

    private function lockDatabase(): LockDatabase
    {
        $this->assertUsable();
        if ($this->lockDatabase === null) {
            throw new FairSQLiteException('The SQLite fair ticket database lifecycle is unavailable.');
        }

        return $this->lockDatabase;
    }

    private function installFairRuntime(PDO $pdo): void
    {
        $clock = $this->monotonic;
        $waiter = WaiterFactory::make($this->waitStrategy, $this->lockPath, $this->debug);
        $this->lockDatabase = new LockDatabase($this->lockPath, $waiter, $clock, debug: $this->debug);
        $this->fairLock = new FairSQLiteLock(
            $pdo,
            $this->lockDatabase,
            $waiter,
            $this->staleHeadSeconds,
            fn (Throwable $exception): null => $this->markUnknownPdoOutcome($exception),
            fn (): null => $this->disconnectAfterUnknownOutcome(),
            $clock,
            $this->debug,
        );
    }

    private function consumeNonTransactionalWriteCount(): int
    {
        $count = $this->nonTransactionalWrites;
        $this->nonTransactionalWrites = 0;

        return $count;
    }

    private function connectionName(): string
    {
        $name = $this->getName();
        if (! is_string($name) || $name === '') {
            throw new FairSQLiteException('The fair SQLite connection name is unavailable.');
        }

        return $name;
    }

    private static function identityKey(string $name, string $appPath, string $lockPath): string
    {
        return hash('sha256', $name."\0".$appPath."\0".$lockPath);
    }

    /**
     * Emit one structured diagnostic for a real connection-state transition.
     *
     * @param  string  $event  Stable event identifier from the package logging contract.
     * @param  array<string, int|string>  $context  Secret-free identifiers describing the transition.
     */
    private function debug(string $event, array $context = []): void
    {
        if ($this->debug) {
            try {
                Log::debug('Fair SQLite transition.', ['event' => $event, 'pid' => getmypid(), ...$context]);
            } catch (Throwable) {
                // Optional diagnostics must never change the connection lifecycle.
            }
        }
    }
}
