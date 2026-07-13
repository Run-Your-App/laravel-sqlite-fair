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
 * Owns the private SQLite ticket database and every retryable lock-database unit.
 *
 * Mutation units are intentionally separate so a commit with an unknown result is
 * never replayed. The application database is neither identified nor opened here.
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
     * Create the lazy private ticket-database owner.
     *
     * No directory, PDO, PRAGMA, or schema is touched until {@see open()}.
     * The optional factory exists only for deterministic package verification.
     *
     * @param  string  $directory  Absolute directory dedicated to one application database.
     * @param  Waiter  $waiter  Wait adapter shared by every retryable lock-database unit.
     * @param  callable(): float  $monotonic  Monotonic seconds used for absolute deadlines.
     * @param  (callable(string): PDO)|null  $pdoFactory  Internal PDO-construction seam receiving lock.sqlite's path.
     * @param  bool  $debug  Whether abnormal transitions emit structured Laravel debug logs.
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
     * @return void
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
     * Prepare the shared ticket-database and native-waiter directory.
     *
     * @param  string  $directory  Absolute directory to validate or create recursively.
     * @return void
     *
     * @throws RuntimeException When the path is not absolute or cannot be created.
     *
     * @internal
     */
    public static function prepareDirectory(string $directory): void
    {
        if (! str_starts_with($directory, DIRECTORY_SEPARATOR) && preg_match('/^[A-Za-z]:[\\\\\/]/', $directory) !== 1) {
            throw new RuntimeException('The SQLite fair lock directory must be absolute.');
        }
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
            $value = $this->query('SELECT ticket FROM tickets ORDER BY ticket ASC LIMIT 1')->fetchColumn();

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
     * @return void
     *
     * @throws Throwable When the delete cannot reach a known committed outcome.
     */
    public function deleteForeignHead(int $observedForeignHead, ?float $deadline = null): void
    {
        $this->mutation(function (PDO $pdo) use ($observedForeignHead): null {
            $statement = $this->prepare($pdo, 'DELETE FROM tickets WHERE ticket = :observedForeignHead');
            $statement->execute(['observedForeignHead' => $observedForeignHead]);

            return null;
        }, $deadline);
    }

    /**
     * Delete exactly one owned ticket after a known application outcome.
     *
     * @param  int  $ownTicket  Ticket owned by the calling fair connection.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return void
     *
     * @throws Throwable When cleanup cannot reach a known committed outcome.
     */
    public function deleteExact(int $ownTicket, ?float $deadline = null): void
    {
        $this->mutation(function (PDO $pdo) use ($ownTicket): null {
            $statement = $this->prepare($pdo, 'DELETE FROM tickets WHERE ticket = :ownTicket');
            $statement->execute(['ownTicket' => $ownTicket]);

            return null;
        }, $deadline);
    }

    /**
     * Perform one nonblocking emergency cleanup attempt.
     *
     * @param  int  $ownTicket  Ticket owned by the aborting connection.
     * @return void
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
            $pdo->exec('BEGIN IMMEDIATE');
            $active = true;
            $statement = $this->prepare($pdo, 'DELETE FROM tickets WHERE ticket = :ownTicket');
            $statement->execute(['ownTicket' => $ownTicket]);
            $pdo->commit();
        } catch (Throwable $exception) {
            if ($active && $pdo->inTransaction()) {
                try {
                    $pdo->rollBack();
                    $this->debug('lock_rollback', ['operation' => 'cleanup']);
                } catch (Throwable $rollback) {
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
        if (! $exception instanceof PDOException || ! is_array($exception->errorInfo) || ! isset($exception->errorInfo[1])) {
            return false;
        }
        $code = (int) $exception->errorInfo[1];

        return in_array($code & 0xFF, [5, 6], true);
    }

    private function configurePersistentPragmas(?float $deadline): void
    {
        $this->retryStatement(fn (): int|false => $this->requirePdo()->exec('PRAGMA journal_mode=DELETE'), $deadline);
        $this->retryStatement(fn (): int|false => $this->requirePdo()->exec('PRAGMA synchronous=NORMAL'), $deadline);
        $this->assertPragmas($deadline);
    }

    private function bootstrapSchema(?float $deadline): void
    {
        $version = (int) $this->retryStatement(fn (): mixed => $this->query('PRAGMA user_version')->fetchColumn(), $deadline);
        $tables = $this->tables($deadline);
        if ($version === 1) {
            if ($tables !== ['tickets']) {
                throw new RuntimeException('The SQLite fair lock database schema is invalid.');
            }

            return;
        }
        if ($version !== 0 || $tables !== []) {
            throw new RuntimeException('The SQLite fair lock database has an unsupported schema version.');
        }

        $this->mutation(function (PDO $pdo): null {
            // Another process may have completed bootstrap after the autocommit precheck.
            // Re-reading under BEGIN IMMEDIATE makes this transaction the schema decision point.
            $currentVersion = $this->queryInteger('PRAGMA user_version');
            $currentTables = $this->stringColumnNow("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name");
            if ($currentVersion === 1 && $currentTables === ['tickets']) {
                return null;
            }
            if ($currentVersion !== 0 || $currentTables !== []) {
                throw new RuntimeException('The SQLite fair lock database changed to an unsupported schema during bootstrap.');
            }
            $pdo->exec('CREATE TABLE tickets (ticket INTEGER PRIMARY KEY AUTOINCREMENT)');
            $pdo->exec('PRAGMA user_version=1');

            return null;
        }, $deadline);
        $this->debug('lock_database_bootstrap');
    }

    private function validate(?float $deadline): void
    {
        if ((int) $this->retryStatement(fn (): mixed => $this->query('PRAGMA user_version')->fetchColumn(), $deadline) !== 1
            || $this->tables($deadline) !== ['tickets']
            || $this->internalTables($deadline) !== ['sqlite_sequence']
            || $this->ticketColumns($deadline) !== [['ticket', 'INTEGER', 1]]) {
            throw new RuntimeException('The SQLite fair lock database schema validation failed.');
        }
        $sql = (string) $this->retryStatement(fn (): mixed => $this->query("SELECT sql FROM sqlite_master WHERE type='table' AND name='tickets'")->fetchColumn(), $deadline);
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
            $rows = $this->query('PRAGMA table_info(tickets)')->fetchAll(PDO::FETCH_ASSOC);
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
        $busy = (int) $this->retryStatement(fn (): mixed => $this->query('PRAGMA busy_timeout')->fetchColumn(), $deadline);
        $journal = mb_strtolower((string) $this->retryStatement(fn (): mixed => $this->query('PRAGMA journal_mode')->fetchColumn(), $deadline));
        $synchronous = (int) $this->retryStatement(fn (): mixed => $this->query('PRAGMA synchronous')->fetchColumn(), $deadline);
        if ($busy !== 0 || $journal !== 'delete' || $synchronous !== 1) {
            throw new RuntimeException('The SQLite fair lock database PRAGMA validation failed.');
        }
    }

    /**
     * @template T
     *
     * @param  callable(): T  $operation
     * @return T
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
     * @template T
     *
     * @param  callable(PDO): T  $operation
     * @return T
     */
    private function mutation(callable $operation, ?float $deadline): mixed
    {
        $this->open($deadline);
        $pdo = $this->requirePdo();
        $secondStatecheck = false;
        while (true) {
            $this->assertBeforeDeadline($deadline);
            try {
                $pdo->exec('BEGIN IMMEDIATE');
            } catch (Throwable $exception) {
                if (! self::isBusyOrLocked($exception)) {
                    throw $exception;
                }
                $this->debug('lock_retry', ['operation' => 'begin_immediate']);
                if ($secondStatecheck) {
                    $this->waiter->block($deadline, $this->monotonic);
                    $secondStatecheck = false;
                } else {
                    $this->waiter->arm();
                    $this->waiter->drain();
                    $secondStatecheck = true;
                }

                continue;
            }

            try {
                $result = $operation($pdo);
            } catch (Throwable $exception) {
                try {
                    $pdo->rollBack();
                    $this->debug('lock_rollback', ['operation' => 'mutation']);
                } catch (Throwable $rollback) {
                    report($rollback);
                    $this->invalidate();
                    throw $exception;
                }
                if (self::isBusyOrLocked($exception)) {
                    $this->debug('lock_retry', ['operation' => 'mutation']);
                    if ($secondStatecheck) {
                        $this->waiter->block($deadline, $this->monotonic);
                        $secondStatecheck = false;
                    } else {
                        $this->waiter->arm();
                        $this->waiter->drain();
                        $secondStatecheck = true;
                    }

                    continue;
                }
                throw $exception;
            }

            try {
                $pdo->commit();
            } catch (Throwable $exception) {
                $this->invalidate();
                throw $exception;
            }

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
        $values = $this->query($sql)->fetchAll(PDO::FETCH_COLUMN);
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
        $value = $this->query($sql)->fetchColumn();
        if (! is_int($value) && ! is_string($value)) {
            throw new RuntimeException('The SQLite fair schema value is not numeric.');
        }

        return (int) $value;
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
            Log::debug('Fair SQLite transition.', [
                'event' => $event,
                'pid' => getmypid(),
                ...$context,
            ]);
        }
    }
}
