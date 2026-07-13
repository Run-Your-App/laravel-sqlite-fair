<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Lock;

use Illuminate\Support\Facades\Log;
use PDO;
use PDOException;
use PDOStatement;
use RuntimeException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;
use Throwable;

/**
 * Owns the private SQLite ticket database for one application database.
 *
 * FairSQLiteLock uses this owner to admit, inspect, recover, and remove committed
 * FIFO tickets. The PDO is opened lazily and invalidated after an indeterminate
 * handle outcome. Each retryable statement or mutation is bounded by the caller's
 * monotonic deadline and coordinated through the shared Waiter.
 *
 * Mutation units are intentionally separate so a commit with an unknown result is
 * never replayed. This owner creates and validates only `lock.sqlite`; it never
 * identifies, opens, or mutates the application database.
 *
 * @internal
 */
final class LockDatabase
{
    private ?PDO $pdo = null;

    /** @var callable(): float */
    private $monotonic;

    /** @var callable(string): PDO */
    private $pdoFactory;

    /**
     * Creates the lazy private ticket-database owner.
     *
     * No directory, PDO, PRAGMA, or schema is touched until {@see open()}.
     * The optional factory exists only for deterministic package verification.
     *
     * @param  string  $directory  Absolute directory dedicated to one application database.
     * @param  Waiter  $waiter  Wait adapter shared by every retryable lock-database unit.
     * @param  callable(): float  $monotonic  Monotonic seconds used for absolute deadlines.
     * @param  (callable(string): PDO)|null  $pdoFactory  Internal PDO-construction seam receiving lock.sqlite's path.
     * @param  bool  $debug  Whether abnormal transitions emit structured Laravel debug logs.
     * @return void The lazy owner is initialized without opening its PDO.
     *
     * @internal
     */
    public function __construct(
        private readonly string $directory,
        private readonly Waiter $waiter,
        callable $monotonic,
        ?callable $pdoFactory = null,
        private readonly bool $debug = false,
    ) {
        $this->monotonic = $monotonic;
        $this->pdoFactory = $pdoFactory ?? static fn (string $path): PDO => new PDO('sqlite:'.$path, options: [
            PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
            PDO::ATTR_STRINGIFY_FETCHES => false,
        ]);
    }

    /**
     * Open and validate the private ticket database once per PDO handle.
     *
     * A new handle receives `busy_timeout=0`, persistent PRAGMAs, schema bootstrap,
     * and final validation exactly once. Later calls reuse it; invalidation causes
     * the next call to repeat the complete handle setup.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return void A validated lock-database handle is available for subsequent operations.
     *
     * @throws Throwable When filesystem setup, PDO setup, bootstrap, validation, or waiting fails.
     */
    public function open(?float $deadline = null): void
    {
        if ($this->pdo !== null) {
            return;
        }
        self::prepareDirectory($this->directory);

        $factory = $this->pdoFactory;
        $this->pdo = $factory($this->directory.DIRECTORY_SEPARATOR.'lock.sqlite');

        try {
            // This must be the first SQL operation on every newly opened handle.
            $this->retryStatement(fn (): int|false => $this->requirePdo()->exec('PRAGMA busy_timeout=0'), $deadline);
            $this->configurePersistentPragmas($deadline);
            $this->bootstrapSchema($deadline);
            $this->validate($deadline);
        } catch (Throwable $exception) {
            $this->invalidate();
            throw $exception;
        }
    }

    /**
     * Creates the shared ticket-database and native-waiter directory when missing.
     *
     * Path semantics belong to FairSQLiteConnector. This method performs only the
     * idempotent filesystem creation needed by the connector and lazy direct owner.
     *
     * @param  string  $directory  Caller-validated directory to create recursively.
     * @return void The supplied directory exists when the method returns.
     *
     * @throws RuntimeException When the directory cannot be created.
     *
     * @internal
     */
    public static function prepareDirectory(string $directory): void
    {
        if (! is_dir($directory) && ! mkdir($directory, 0775, true) && ! is_dir($directory)) {
            throw new RuntimeException("The SQLite fair lock directory [{$directory}] could not be created.");
        }
    }

    /**
     * Read the smallest committed queue ticket.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return int|null The current queue head, or null when the queue is empty.
     *
     * @throws Throwable When opening, querying, decoding, or waiting fails.
     */
    public function readHead(?float $deadline = null): ?int
    {
        $this->open($deadline);

        return $this->retryStatement(function (): ?int {
            $value = $this->queryValue('SELECT ticket FROM tickets ORDER BY ticket ASC LIMIT 1');

            if ($value === false) {
                return null;
            }
            if (! is_int($value) && ! is_string($value)) {
                throw new RuntimeException('The SQLite fair head ticket is not numeric.');
            }

            return (int) $value;
        }, $deadline);
    }

    /**
     * Append one committed ticket to the FIFO queue.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return int The AUTOINCREMENT ticket only after its transaction committed successfully.
     *
     * @throws Throwable When admission cannot reach a known committed outcome.
     */
    public function admit(?float $deadline = null): int
    {
        $ticket = $this->mutation(
            function (PDO $pdo): int {
                $pdo->exec('INSERT INTO tickets DEFAULT VALUES');

                return (int) $pdo->lastInsertId();
            },
            $deadline,
        );

        $this->debug('ticket_created', ['ticket' => $ticket]);

        return $ticket;
    }

    /**
     * Delete one revalidated foreign queue head during fenced recovery.
     *
     * @param  int  $observedForeignHead  Exact foreign head observed again under the application writer fence.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return void The exact foreign ticket is absent after a known committed delete.
     *
     * @throws Throwable When the delete cannot reach a known committed outcome.
     */
    public function deleteForeignHead(int $observedForeignHead, ?float $deadline = null): void
    {
        $this->mutation(function (PDO $pdo) use ($observedForeignHead): null {
            $statement = $this->prepare($pdo, 'DELETE FROM tickets WHERE ticket = :observedForeignHead');
            try {
                $statement->execute(['observedForeignHead' => $observedForeignHead]);
            } finally {
                $statement->closeCursor();
            }

            return null;
        }, $deadline);
    }

    /**
     * Delete exactly one owned ticket after a known application outcome.
     *
     * @param  int  $ownTicket  Ticket owned by the calling fair connection.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return void The exact owned ticket is absent after a known committed delete.
     *
     * @throws Throwable When cleanup cannot reach a known committed outcome.
     */
    public function deleteExact(int $ownTicket, ?float $deadline = null): void
    {
        $this->mutation(function (PDO $pdo) use ($ownTicket): null {
            $statement = $this->prepare($pdo, 'DELETE FROM tickets WHERE ticket = :ownTicket');
            try {
                $statement->execute(['ownTicket' => $ownTicket]);
            } finally {
                $statement->closeCursor();
            }

            return null;
        }, $deadline);
    }

    /**
     * Perform one nonblocking emergency cleanup attempt.
     *
     * @param  int  $ownTicket  Ticket owned by the aborting connection.
     * @return void The single nonblocking cleanup attempt completed successfully.
     *
     * @throws Throwable When the existing handle cannot complete the one permitted attempt.
     */
    public function cleanupExact(int $ownTicket): void
    {
        if ($this->pdo === null) {
            throw new RuntimeException('A nonblocking cleanup requires the existing lock database handle.');
        }
        $pdo = $this->requirePdo();
        $pdo->exec('PRAGMA busy_timeout=0');
        $active = false;
        try {
            $pdo->exec('BEGIN EXCLUSIVE');
            $active = true;
            $statement = $this->prepare($pdo, 'DELETE FROM tickets WHERE ticket = :ownTicket');
            try {
                $statement->execute(['ownTicket' => $ownTicket]);
            } finally {
                $statement->closeCursor();
            }
            $pdo->commit();
        } catch (Throwable $exception) {
            if ($active && $pdo->inTransaction()) {
                try {
                    $pdo->rollBack();
                    $this->debug('lock_rollback', ['operation' => 'cleanup']);
                } catch (Throwable $rollback) {
                    $this->debug('cleanup_failed', ['operation' => 'cleanup_rollback']);
                    report($rollback);
                    $this->invalidate();
                }
            }
            if ($active) {
                $this->invalidate();
            }
            throw $exception;
        }
    }

    /**
     * Classify only numeric SQLite BUSY or LOCKED PDO failures.
     *
     * @param  Throwable  $exception  Failure whose SQLite driver code should be inspected.
     * @return bool True for base or extended SQLite codes 5 and 6; false otherwise.
     *
     * @internal Shared by lock-database and application-fence owners.
     */
    public static function isBusyOrLocked(Throwable $exception): bool
    {
        return in_array(self::sqliteResultCode($exception), [5, 6], true);
    }

    private function configurePersistentPragmas(?float $deadline): void
    {
        $this->retryStatement(fn (): int|false => $this->requirePdo()->exec('PRAGMA journal_mode=DELETE'), $deadline);
        $this->retryStatement(fn (): int|false => $this->requirePdo()->exec('PRAGMA synchronous=NORMAL'), $deadline);
        $this->assertPragmas($deadline);
    }

    private function bootstrapSchema(?float $deadline): void
    {
        $version = (int) $this->retryStatement(fn (): mixed => $this->queryValue('PRAGMA user_version'), $deadline);
        $tables = $this->tables($deadline);
        if ($version === 1 && $tables === ['tickets']) {
            return;
        }

        $bootstrapped = $this->mutation(function (PDO $pdo): bool {
            // The two autocommit prechecks can straddle another process's bootstrap.
            // Only this exclusive re-read may reject or create the shared schema.
            $currentVersion = $this->queryInteger('PRAGMA user_version');
            $currentTables = $this->stringColumnNow("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
            if ($currentVersion === 1 && $currentTables === ['tickets']) {
                return false;
            }
            if ($currentVersion !== 0 || $currentTables !== []) {
                throw new RuntimeException('The SQLite fair lock database changed to an unsupported schema during bootstrap.');
            }
            $pdo->exec('CREATE TABLE tickets (ticket INTEGER PRIMARY KEY AUTOINCREMENT)');
            $pdo->exec('PRAGMA user_version=1');

            return true;
        }, $deadline);

        if ($bootstrapped) {
            $this->debug('lock_database_bootstrap');
        }
    }

    private function validate(?float $deadline): void
    {
        if ((int) $this->retryStatement(fn (): mixed => $this->queryValue('PRAGMA user_version'), $deadline) !== 1
            || $this->tables($deadline) !== ['tickets']
            || $this->internalTables($deadline) !== ['sqlite_sequence']
            || $this->ticketColumns($deadline) !== [['ticket', 'INTEGER', 1]]) {
            throw new RuntimeException('The SQLite fair lock database schema validation failed.');
        }
        $sql = (string) $this->retryStatement(fn (): mixed => $this->queryValue("SELECT sql FROM sqlite_master WHERE type='table' AND name='tickets'"), $deadline);
        if (preg_match('/^CREATE TABLE tickets \(ticket INTEGER PRIMARY KEY AUTOINCREMENT\)$/i', $sql) !== 1) {
            throw new RuntimeException('The SQLite fair ticket table does not own the required AUTOINCREMENT key.');
        }
        $this->assertPragmas($deadline);
    }

    /** @return list<string> */
    private function internalTables(?float $deadline): array
    {
        return $this->stringColumn("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'sqlite_%' ORDER BY name", $deadline);
    }

    /** @return list<array{string, string, int}> */
    private function ticketColumns(?float $deadline): array
    {
        return $this->retryStatement(function (): array {
            $statement = $this->query('PRAGMA table_info(tickets)');
            try {
                $rows = $statement->fetchAll(PDO::FETCH_ASSOC);
            } finally {
                $statement->closeCursor();
            }
            $columns = [];
            foreach ($rows as $row) {
                if (! is_string($row['name'] ?? null) || ! is_string($row['type'] ?? null) || ! is_int($row['pk'] ?? null)) {
                    throw new RuntimeException('The SQLite fair ticket column metadata is invalid.');
                }
                $columns[] = [$row['name'], mb_strtoupper($row['type']), $row['pk']];
            }

            return $columns;
        }, $deadline);
    }

    /** @return list<string> */
    private function tables(?float $deadline): array
    {
        return $this->stringColumn("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name", $deadline);
    }

    private function assertPragmas(?float $deadline): void
    {
        $busy = (int) $this->retryStatement(fn (): mixed => $this->queryValue('PRAGMA busy_timeout'), $deadline);
        $journal = mb_strtolower((string) $this->retryStatement(fn (): mixed => $this->queryValue('PRAGMA journal_mode'), $deadline));
        $synchronous = (int) $this->retryStatement(fn (): mixed => $this->queryValue('PRAGMA synchronous'), $deadline);
        if ($busy !== 0 || $journal !== 'delete' || $synchronous !== 1) {
            throw new RuntimeException('The SQLite fair lock database PRAGMA validation failed.');
        }
    }

    /**
     * Retries one autocommit statement around the arm-recheck-block sequence.
     *
     * Only numeric SQLite BUSY or LOCKED outcomes are retryable. The operation is
     * re-executed once immediately after arming and draining so a state change in
     * the lost-wakeup window is observed before this owner blocks.
     *
     * @template T
     *
     * @param  callable(): T  $operation  Complete idempotent statement unit.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return T The first successful statement result.
     *
     * @throws Throwable When the deadline expires or the statement fails with a non-contention outcome.
     */
    private function retryStatement(callable $operation, ?float $deadline): mixed
    {
        while (true) {
            $this->assertBeforeDeadline($deadline);
            try {
                return $operation();
            } catch (Throwable $exception) {
                if (! self::isBusyOrLocked($exception)) {
                    throw $exception;
                }
                $this->waiter->arm();
                $this->waiter->drain();
                $this->assertBeforeDeadline($deadline);
                $this->debug('lock_retry', ['operation' => 'statement']);
                try {
                    return $operation();
                } catch (Throwable $recheck) {
                    if (! self::isBusyOrLocked($recheck)) {
                        throw $recheck;
                    }
                }
                $this->debug('lock_retry', ['operation' => 'statement']);
                $this->waiter->block($deadline, $this->monotonic);
            }
        }
    }

    /**
     * Executes one exclusive mutation without replaying an uncertain commit.
     *
     * BEGIN and pre-commit operation failures may retry only after a known rollback
     * or a known absence of a transaction. The successful operation result returns
     * only after commitMutation() establishes a known committed outcome.
     *
     * @template T
     *
     * @param  callable(PDO): T  $operation  Complete mutation performed inside BEGIN EXCLUSIVE.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return T The committed mutation result.
     *
     * @throws Throwable When opening, mutation, rollback, commit, or waiting cannot reach a known outcome.
     */
    private function mutation(callable $operation, ?float $deadline): mixed
    {
        $this->open($deadline);
        $pdo = $this->requirePdo();
        $secondStatecheck = false;
        while (true) {
            $this->assertBeforeDeadline($deadline);
            try {
                $pdo->exec('BEGIN EXCLUSIVE');
            } catch (Throwable $exception) {
                if (! self::isBusyOrLocked($exception)) {
                    throw $exception;
                }
                $this->debug('lock_retry', ['operation' => 'begin_exclusive']);
                $this->waitAfterContention($deadline, $secondStatecheck);

                continue;
            }

            try {
                $result = $operation($pdo);
            } catch (Throwable $exception) {
                try {
                    $pdo->rollBack();
                    $this->debug('lock_rollback', ['operation' => 'mutation']);
                } catch (Throwable $rollback) {
                    $this->debug('cleanup_failed', ['operation' => 'mutation_rollback']);
                    report($rollback);
                    $this->invalidate();
                    throw $exception;
                }
                if (self::isBusyOrLocked($exception)) {
                    $this->debug('lock_retry', ['operation' => 'mutation']);
                    $this->waitAfterContention($deadline, $secondStatecheck);

                    continue;
                }
                throw $exception;
            }

            $this->commitMutation($pdo, $deadline);

            return $result;
        }
    }

    private function assertBeforeDeadline(?float $deadline): void
    {
        $now = $this->monotonic;
        if ($deadline !== null && $now() >= $deadline) {
            $this->debug('wait_timeout', ['operation' => 'lock_database']);
            throw new FairWaitTimeoutException('The SQLite fair lock deadline expired.');
        }
    }

    /**
     * Resolves the active transaction's commit without replaying its mutation.
     *
     * Numeric SQLite BUSY retries only PDO::commit() on the same transaction.
     * Every other commit failure invalidates the handle because its outcome is not
     * safe to reuse. Deadline expiry attempts one rollback before escaping.
     */
    private function commitMutation(PDO $pdo, ?float $deadline): void
    {
        $secondStatecheck = false;

        try {
            while (true) {
                $this->assertBeforeDeadline($deadline);
                try {
                    $pdo->commit();

                    return;
                } catch (Throwable $exception) {
                    if (self::sqliteResultCode($exception) !== 5) {
                        $this->debug('unknown_pdo_outcome', ['operation' => 'lock_commit']);
                        $this->invalidate();
                        throw $exception;
                    }

                    $this->debug('lock_retry', ['operation' => 'commit']);
                    $this->waitAfterContention($deadline, $secondStatecheck);
                }
            }
        } catch (FairWaitTimeoutException $exception) {
            try {
                $pdo->rollBack();
                $this->debug('lock_rollback', ['operation' => 'commit_timeout']);
            } catch (Throwable $rollback) {
                $this->debug('cleanup_failed', ['operation' => 'commit_timeout_rollback']);
                report($rollback);
                $this->invalidate();
            }

            throw $exception;
        }
    }

    /**
     * Advances one shared arm-recheck-block contention cycle.
     *
     * The first call arms and drains, then returns control for the caller's second
     * state check. A consecutive call blocks and resets the cycle.
     */
    private function waitAfterContention(?float $deadline, bool &$secondStatecheck): void
    {
        if ($secondStatecheck) {
            $this->waiter->block($deadline, $this->monotonic);
            $secondStatecheck = false;

            return;
        }

        $this->waiter->arm();
        $this->waiter->drain();
        $secondStatecheck = true;
    }

    /** Returns the base SQLite result code only for PDO SQLite exceptions. */
    private static function sqliteResultCode(Throwable $exception): ?int
    {
        if (! $exception instanceof PDOException || ! is_array($exception->errorInfo) || ! isset($exception->errorInfo[1])) {
            return null;
        }

        return ((int) $exception->errorInfo[1]) & 0xFF;
    }

    private function invalidate(): void
    {
        $this->pdo = null;
    }

    private function requirePdo(): PDO
    {
        return $this->pdo ?? throw new RuntimeException('The SQLite fair lock database handle is not open.');
    }

    private function query(string $sql): PDOStatement
    {
        $statement = $this->requirePdo()->query($sql);
        if ($statement === false) {
            throw new RuntimeException('The SQLite fair lock query could not be prepared.');
        }

        return $statement;
    }

    private function prepare(PDO $pdo, string $sql): PDOStatement
    {
        $statement = $pdo->prepare($sql);
        if ($statement === false) {
            throw new RuntimeException('The SQLite fair lock statement could not be prepared.');
        }

        return $statement;
    }

    /** @return list<string> */
    private function stringColumn(string $sql, ?float $deadline): array
    {
        return $this->retryStatement(fn (): array => $this->stringColumnNow($sql), $deadline);
    }

    /** @return list<string> */
    private function stringColumnNow(string $sql): array
    {
        $statement = $this->query($sql);
        try {
            $values = $statement->fetchAll(PDO::FETCH_COLUMN);
        } finally {
            $statement->closeCursor();
        }
        $strings = [];
        foreach ($values as $value) {
            if (! is_string($value)) {
                throw new RuntimeException('The SQLite fair schema name is not a string.');
            }
            $strings[] = $value;
        }

        return $strings;
    }

    private function queryInteger(string $sql): int
    {
        $value = $this->queryValue($sql);
        if (! is_int($value) && ! is_string($value)) {
            throw new RuntimeException('The SQLite fair schema value is not numeric.');
        }

        return (int) $value;
    }

    /**
     * Fetches one scalar value and always releases the SQLite read cursor.
     *
     * Every internal caller selects an SQLite integer, text, or null scalar. PDO
     * returns false when the query has no row; no array, object, or floating result
     * belongs to this lock-database query boundary.
     *
     * @param  string  $sql  Internal scalar query executed on the open lock PDO.
     * @return int|string|null|false The SQLite scalar, or false when no row exists.
     *
     * @throws RuntimeException When the lock PDO is unavailable or the query cannot be prepared.
     */
    private function queryValue(string $sql): mixed
    {
        $statement = $this->query($sql);
        try {
            return $statement->fetchColumn();
        } finally {
            $statement->closeCursor();
        }
    }

    /**
     * Emit one structured diagnostic for a real lock-state transition.
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
                // Optional diagnostics must never change lock ownership or retry behavior.
            }
        }
    }
}
