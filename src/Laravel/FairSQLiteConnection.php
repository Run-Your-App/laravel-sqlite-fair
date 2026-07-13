<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Laravel;

use Closure;
use Illuminate\Database\DeadlockException;
use Illuminate\Database\SQLiteConnection;
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
 * Callback, manual, implicit and nontransactional writes share the same ticket,
 * fence, cleanup and unknown-PDO retirement state.
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

    private ?int $activeTicket = null;

    private bool $fairFenceHeld = false;

    private bool $nonTransactionalScope = false;

    private int $nonTransactionalWrites = 0;

    private int $waitScopeDepth = 0;

    private int $waitScopeTopLevelCalls = 0;

    private ?float $waitScopeDeadline = null;

    /**
     * @param  array<string, mixed>  $config
     * @param  (callable(): float)|null  $monotonic  Internal deterministic monotonic clock seam.
     *
     * @internal The optional monotonic callable is a package verification seam.
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
        if (! is_string($strategy)
            || (! is_int($staleHeadSeconds) && ! is_float($staleHeadSeconds))) {
            throw new FairSQLiteException('A fair SQLite connection requires validated wait configuration.');
        }
        $this->lockPath = $lockPath;
        $this->waitStrategy = $strategy;
        $this->staleHeadSeconds = (float) $staleHeadSeconds;

        parent::__construct($pdo, $database, $tablePrefix, $config);

        $clock = $monotonic ?? static fn (): float => hrtime(true) / 1e9;
        $this->monotonic = $clock;
        $this->installFairRuntime(parent::getPdo());
    }

    /** Rejects ambiguous aliases before a PDO or lock database is opened. @internal */
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

    /** Fails before opening PDO when this exact process-local identity is retired. @internal */
    public static function assertIdentityIsUsable(string $name, string $appPath, string $lockPath): void
    {
        if (isset(self::$unknownPdoOutcomes[self::identityKey($name, $appPath, $lockPath)])) {
            throw new FairSQLiteException('This SQLite fair connection is retired after an unknown PDO outcome.');
        }
    }

    /** Reports whether this exact connection identity requires process recycling. @internal */
    public function hasUnknownPdoOutcome(): bool
    {
        return isset(self::$unknownPdoOutcomes[$this->identityKey]);
    }

    /**
     * Executes a callback through Laravel-compatible fair transaction attempts.
     *
     * @template TReturn
     *
     * @param  Closure(static): TReturn  $callback
     * @return TReturn
     *
     * @throws Throwable
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

    /** @param array<array-key, mixed> $bindings */
    public function statement($query, $bindings = []): bool
    {
        $this->rejectTransactionControl($query);

        return $this->runFairWrite(fn (): bool => parent::statement($query, $bindings));
    }

    /** @param array<array-key, mixed> $bindings */
    public function affectingStatement($query, $bindings = []): int
    {
        $this->rejectTransactionControl($query);

        return $this->runFairWrite(fn (): int => parent::affectingStatement($query, $bindings));
    }

    public function unprepared($query): bool
    {
        $this->rejectTransactionControl($query);

        return $this->runFairWrite(fn (): bool => parent::unprepared($query));
    }

    /**
     * Runs exactly one nontransactional write while its queue ticket remains owned.
     *
     * @template TReturn
     *
     * @param  Closure(static): TReturn  $callback
     * @return TReturn
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
            report($cleanup);
        }
        $this->clearFairScope();

        return $result;
    }

    /**
     * Applies one bounded writer wait to exactly one top-level fair operation.
     *
     * @template TReturn
     *
     * @param  Closure(static): TReturn  $callback
     * @return TReturn
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
            return $callback($this);
        } finally {
            $this->waitScopeDepth--;
            $this->waitScopeDeadline = $previousDeadline;
            if ($outermost) {
                $this->waitScopeTopLevelCalls = 0;
            }
        }
    }

    /** @return array<int, array{query: string, bindings: array<array-key, mixed>, time: float|null}> */
    public function pretend(Closure $callback): array
    {
        $this->assertUsable();

        return parent::pretend($callback);
    }

    public function reconnect(): mixed
    {
        $this->assertUsable();

        return parent::reconnect();
    }

    public function disconnect(): void
    {
        if ($this->hasUnknownPdoOutcome()) {
            $this->disconnectAfterUnknownOutcome();

            return;
        }

        parent::disconnect();
    }

    public function getPdo(): PDO
    {
        $this->assertUsable();

        return parent::getPdo();
    }

    public function getReadPdo(): PDO
    {
        $this->assertUsable();

        return parent::getReadPdo();
    }

    public function getRawPdo(): PDO|Closure|null
    {
        $this->assertUsable();

        return parent::getRawPdo();
    }

    public function getRawReadPdo(): PDO|Closure|null
    {
        $this->assertUsable();

        return parent::getRawReadPdo();
    }

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
            throw new FairWaitTimeoutException('The SQLite fair writer wait deadline expired.');
        }

        return $deadline;
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
        $this->markUnknownPdoOutcome($primary);
        $this->disconnectAfterUnknownOutcome();
        if ($this->activeTicket !== null && $fairLock !== null) {
            try {
                $fairLock->cleanupOwnTicket($this->activeTicket);
            } catch (Throwable $cleanup) {
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
        LockDatabase::prepareDirectory($this->lockPath);
        $waiter = WaiterFactory::make($this->waitStrategy, $this->lockPath);
        $this->lockDatabase = new LockDatabase($this->lockPath, $waiter, $clock);
        $this->fairLock = new FairSQLiteLock(
            $pdo,
            $this->lockDatabase,
            $waiter,
            $this->staleHeadSeconds,
            fn (Throwable $exception): null => $this->markUnknownPdoOutcome($exception),
            fn (): null => $this->disconnectAfterUnknownOutcome(),
            $clock,
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
}
