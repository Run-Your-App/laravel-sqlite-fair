<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Log;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;
use RunYourApp\LaravelSqliteFair\Wait\PollingWaiter;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;

it('bootstraps the exact lock schema and supports ordered ticket operations', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/lock-database';
    $owned = null;
    $database = new LockDatabase(
        $directory,
        new PollingWaiter,
        static fn (): float => hrtime(true) / 1e9,
        static function (string $path) use (&$owned): PDO {
            return $owned = new PDO('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        },
    );

    expect($database->readHead())->toBeNull();
    $first = $database->admit();
    $second = $database->admit();
    expect([$first, $second, $database->readHead()])->toBe([1, 2, 1]);

    $database->deleteForeignHead($first);
    expect($database->readHead())->toBe(2);
    $database->deleteExact($second);
    expect($database->readHead())->toBeNull();

    expect($owned)->toBeInstanceOf(PDO::class);
    if (! $owned instanceof PDO) {
        throw new RuntimeException('The injected PDO factory did not retain its handle.');
    }
    expect((int) $owned->query('PRAGMA busy_timeout')->fetchColumn())->toBe(0)
        ->and(mb_strtolower((string) $owned->query('PRAGMA journal_mode')->fetchColumn()))->toBe('delete')
        ->and((int) $owned->query('PRAGMA synchronous')->fetchColumn())->toBe(1)
        ->and((int) $owned->query('PRAGMA user_version')->fetchColumn())->toBe(1);
});

it('configures persistent pragmas once per lock pdo instead of once per ticket', function () {
    $state = (object) ['journal' => 0, 'synchronous' => 0];
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/pragma-once-per-handle';
    $database = new LockDatabase(
        $directory,
        new PollingWaiter,
        static fn (): float => 0.0,
        static fn (string $path): PDO => new class($path, $state) extends PDO
        {
            public function __construct(string $path, private readonly object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function exec(string $statement): int|false
            {
                if ($statement === 'PRAGMA journal_mode=DELETE') {
                    $this->state->journal++;
                }
                if ($statement === 'PRAGMA synchronous=NORMAL') {
                    $this->state->synchronous++;
                }

                return parent::exec($statement);
            }
        },
    );

    $first = $database->admit();
    $second = $database->admit();
    $database->deleteExact($first);
    $database->deleteExact($second);

    expect($state->journal)->toBe(1)
        ->and($state->synchronous)->toBe(1)
        ->and($database->readHead())->toBeNull();
});

it('revalidates a concurrent bootstrap that commits between the autocommit prechecks', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/concurrent-bootstrap-precheck';
    $state = (object) ['interleaved' => false];
    $factory = static fn (string $path): PDO => new class($path, $directory, $state) extends PDO
    {
        public function __construct(string $path, private readonly string $directory, private readonly object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
        {
            if (! $this->state->interleaved && str_starts_with($query, "SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE")) {
                $this->state->interleaved = true;
                (new LockDatabase($this->directory, new PollingWaiter, static fn (): float => 0.0))->open();
            }

            return parent::query($query, $fetchMode, ...$fetchModeArgs);
        }
    };
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0, $factory);

    $database->open();

    $verification = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    expect($state->interleaved)->toBeTrue()
        ->and((int) $verification->query('PRAGMA user_version')->fetchColumn())->toBe(1)
        ->and($verification->query("SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")->fetchAll(PDO::FETCH_COLUMN))->toBe(['sqlite_sequence', 'tickets']);
});

it('classifies only numeric sqlite busy and locked base or extended codes', function (int $code, bool $expected) {
    $exception = new PDOException('irrelevant message', $code);
    $exception->errorInfo = ['HY000', $code, 'irrelevant message'];

    expect(LockDatabase::isBusyOrLocked($exception))->toBe($expected);
})->with([[5, true], [6, true], [261, true], [262, true], [19, false], [0, false]]);

it('does not classify a generic throwable code as sqlite contention', function () {
    expect(LockDatabase::isBusyOrLocked(new RuntimeException('not a PDO error', 5)))->toBeFalse();
});

it('uses an idempotent missing-row exact delete', function () {
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/missing-delete', new PollingWaiter, static fn (): float => hrtime(true) / 1e9);
    $database->deleteExact(999);

    expect($database->readHead())->toBeNull();
});

it('rejects an unknown schema version without changing it', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/invalid-version';
    mkdir($directory, 0775, true);
    $pdo = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pdo->exec('PRAGMA journal_mode=DELETE');
    $pdo->exec('PRAGMA synchronous=NORMAL');
    $pdo->exec('PRAGMA user_version=2');
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0);

    expect(fn () => $database->open())->toThrow(RuntimeException::class)
        ->and((int) $pdo->query('PRAGMA user_version')->fetchColumn())->toBe(2);
});

it('rejects an unexpected bootstrap table without replacing it', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/invalid-table';
    mkdir($directory, 0775, true);
    $pdo = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pdo->exec('PRAGMA journal_mode=DELETE');
    $pdo->exec('PRAGMA synchronous=NORMAL');
    $pdo->exec('CREATE TABLE unexpected (id INTEGER PRIMARY KEY)');
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0);

    expect(fn () => $database->open())->toThrow(RuntimeException::class)
        ->and($pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name='unexpected'")->fetchColumn())->toBe('unexpected');
});

it('performs cleanup as one nonblocking attempt without waiter replay', function () {
    $waitState = (object) ['calls' => 0];
    $waiter = new class($waitState) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->calls++;
        }

        public function drain(): void
        {
            $this->state->calls++;
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->calls++;
        }
    };
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/cleanup-once';
    $database = new LockDatabase($directory, $waiter, static fn (): float => 0.0);
    $ticket = $database->admit();
    $blocker = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $blocker->exec('PRAGMA busy_timeout=0');
    $blocker->exec('BEGIN EXCLUSIVE');

    expect(fn () => $database->cleanupExact($ticket))->toThrow(PDOException::class)
        ->and($waitState->calls)->toBe(0);
    $blocker->rollBack();
    expect($database->readHead())->toBe($ticket);
});

it('does not open a new handle for cleanup', function () {
    $factoryCalls = 0;
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/cleanup-no-open',
        new PollingWaiter,
        static fn (): float => 0.0,
        static function (string $path) use (&$factoryCalls): PDO {
            $factoryCalls++;

            return new PDO('sqlite:'.$path);
        },
    );

    expect(fn () => $database->cleanupExact(1))->toThrow(RuntimeException::class)
        ->and($factoryCalls)->toBe(0);
});

it('sets zero busy timeout first and completes pragma setup before bootstrap begin', function () {
    $state = (object) ['sql' => []];
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/ordered-bootstrap';
    $database = new LockDatabase(
        $directory,
        new PollingWaiter,
        static fn (): float => 0.0,
        static fn (string $path): PDO => new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_STRINGIFY_FETCHES => false]);
            }

            public function exec(string $statement): int|false
            {
                $this->state->sql[] = $statement;

                return parent::exec($statement);
            }
        },
    );
    $database->open();

    expect($state->sql[0])->toBe('PRAGMA busy_timeout=0')
        ->and(array_search('PRAGMA journal_mode=DELETE', $state->sql, true))->toBeLessThan(array_search('BEGIN EXCLUSIVE', $state->sql, true))
        ->and(array_search('PRAGMA synchronous=NORMAL', $state->sql, true))->toBeLessThan(array_search('BEGIN EXCLUSIVE', $state->sql, true));
});

it('does not replay admission after an unknown commit and reopens only on a later call', function () {
    $state = (object) ['factory' => 0, 'throwCommit' => false, 'inserts' => 0];
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/admission-commit-unknown';
    $factory = static function (string $path) use ($state): PDO {
        $state->factory++;

        return new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_STRINGIFY_FETCHES => false]);
            }

            public function exec(string $statement): int|false
            {
                if ($statement === 'INSERT INTO tickets DEFAULT VALUES') {
                    $this->state->inserts++;
                }

                return parent::exec($statement);
            }

            public function commit(): bool
            {
                if ($this->state->throwCommit) {
                    parent::commit();
                    throw new RuntimeException('unknown commit');
                }

                return parent::commit();
            }
        };
    };
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0, $factory);
    $database->open();
    $state->throwCommit = true;

    expect(fn () => $database->admit())->toThrow(RuntimeException::class, 'unknown commit')
        ->and($state->inserts)->toBe(1)
        ->and($state->factory)->toBe(1);
    $state->throwCommit = false;
    expect($database->readHead())->toBe(1)
        ->and($state->factory)->toBe(2);
});

it('retries only the active commit after numeric sqlite busy', function () {
    $state = (object) ['active' => false, 'begins' => 0, 'inserts' => 0, 'commits' => 0];
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($this->state->active && $statement === 'BEGIN EXCLUSIVE') {
                $this->state->begins++;
            }
            if ($this->state->active && $statement === 'INSERT INTO tickets DEFAULT VALUES') {
                $this->state->inserts++;
            }

            return parent::exec($statement);
        }

        public function commit(): bool
        {
            if ($this->state->active && ++$this->state->commits === 1) {
                $exception = new PDOException('busy commit');
                $exception->errorInfo = ['HY000', 5, 'ignored'];
                throw $exception;
            }

            return parent::commit();
        }
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/busy-commit',
        new PollingWaiter,
        static fn (): float => hrtime(true) / 1e9,
        $factory,
    );
    $database->open();
    $state->active = true;

    expect($database->admit())->toBe(1)
        ->and($state->begins)->toBe(1)
        ->and($state->inserts)->toBe(1)
        ->and($state->commits)->toBe(2)
        ->and($database->readHead())->toBe(1);
});

it('does not retry a locked or unknown commit outcome', function (?int $sqliteCode, string $message) {
    $failure = $sqliteCode === null ? new RuntimeException($message) : new PDOException($message);
    if ($failure instanceof PDOException) {
        $failure->errorInfo = ['HY000', $sqliteCode, 'ignored'];
    }
    $state = (object) ['active' => false, 'begins' => 0, 'inserts' => 0, 'commits' => 0];
    $factory = static fn (string $path): PDO => new class($path, $state, $failure) extends PDO
    {
        public function __construct(string $path, private object $state, private Throwable $failure)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($this->state->active && $statement === 'BEGIN EXCLUSIVE') {
                $this->state->begins++;
            }
            if ($this->state->active && $statement === 'INSERT INTO tickets DEFAULT VALUES') {
                $this->state->inserts++;
            }

            return parent::exec($statement);
        }

        public function commit(): bool
        {
            if ($this->state->active) {
                $this->state->commits++;
                throw $this->failure;
            }

            return parent::commit();
        }
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/terminal-commit-'.$failure->getMessage(),
        new PollingWaiter,
        static fn (): float => hrtime(true) / 1e9,
        $factory,
    );
    $database->open();
    $state->active = true;

    expect(fn () => $database->admit())->toThrow($failure::class, $failure->getMessage())
        ->and($state->begins)->toBe(1)
        ->and($state->inserts)->toBe(1)
        ->and($state->commits)->toBe(1);
})->with([
    'sqlite locked' => [6, 'locked'],
    'unknown runtime outcome' => [null, 'unknown'],
]);

it('rolls back an active busy commit when its absolute deadline expires', function () {
    $state = (object) ['active' => false, 'commits' => 0, 'rollbacks' => 0];
    $clock = static fn (): float => $state->active && $state->commits > 0 ? 2.0 : 0.0;
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function commit(): bool
        {
            if ($this->state->active) {
                $this->state->commits++;
                $exception = new PDOException('busy until deadline');
                $exception->errorInfo = ['HY000', 5, 'ignored'];
                throw $exception;
            }

            return parent::commit();
        }

        public function rollBack(): bool
        {
            if ($this->state->active) {
                $this->state->rollbacks++;
            }

            return parent::rollBack();
        }
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/busy-commit-deadline',
        new PollingWaiter,
        $clock,
        $factory,
    );
    $database->open();
    $state->active = true;

    expect(fn () => $database->admit(1.0))->toThrow(FairWaitTimeoutException::class, 'deadline')
        ->and($state->commits)->toBe(1)
        ->and($state->rollbacks)->toBe(1);
    $state->active = false;
    expect($database->readHead())->toBeNull()
        ->and($database->admit())->toBe(1);
});

it('rolls back an active busy commit when the waiter fails without replaying the mutation', function (string $failurePoint) {
    $state = (object) [
        'active' => false,
        'begins' => 0,
        'inserts' => 0,
        'commits' => 0,
        'rollbacks' => 0,
        'pdo' => null,
    ];
    $waiter = new class($failurePoint) implements Waiter
    {
        public function __construct(private readonly string $failurePoint) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            if ($this->failurePoint === 'arm') {
                throw new RuntimeException('waiter arm failed');
            }
        }

        public function drain(): void
        {
            if ($this->failurePoint === 'drain') {
                throw new RuntimeException('waiter drain failed');
            }
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            if ($this->failurePoint === 'block') {
                throw new RuntimeException('waiter block failed');
            }
        }
    };
    $factory = static function (string $path) use ($state, $failurePoint): PDO {
        return $state->pdo = new class($path, $state, $failurePoint) extends PDO
        {
            public function __construct(string $path, private readonly object $state, private readonly string $failurePoint)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function exec(string $statement): int|false
            {
                if ($this->state->active && $statement === 'BEGIN EXCLUSIVE') {
                    $this->state->begins++;
                }
                if ($this->state->active && $statement === 'INSERT INTO tickets DEFAULT VALUES') {
                    $this->state->inserts++;
                }

                return parent::exec($statement);
            }

            public function commit(): bool
            {
                if ($this->state->active) {
                    $this->state->commits++;
                    $busyAttempts = $this->failurePoint === 'block' ? 2 : 1;
                    if ($this->state->commits <= $busyAttempts) {
                        $exception = new PDOException('busy before waiter failure');
                        $exception->errorInfo = ['HY000', 5, 'ignored'];

                        throw $exception;
                    }
                }

                return parent::commit();
            }

            public function rollBack(): bool
            {
                if ($this->state->active) {
                    $this->state->rollbacks++;
                }

                return parent::rollBack();
            }
        };
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/busy-commit-waiter-'.$failurePoint,
        $waiter,
        static fn (): float => 0.0,
        $factory,
    );
    $database->open();
    $state->active = true;

    expect(fn () => $database->admit())->toThrow(RuntimeException::class, "waiter {$failurePoint} failed")
        ->and($state->begins)->toBe(1)
        ->and($state->inserts)->toBe(1)
        ->and($state->commits)->toBe($failurePoint === 'block' ? 2 : 1)
        ->and($state->rollbacks)->toBe(1)
        ->and($state->pdo)->toBeInstanceOf(PDO::class)
        ->and($state->pdo?->inTransaction())->toBeFalse();

    $state->active = false;
    expect($database->readHead())->toBeNull()
        ->and($database->admit())->toBe(1);
})->with(['arm', 'drain', 'block']);

it('does not replay exact delete units after an unknown commit', function (string $unit) {
    $state = (object) ['factory' => 0, 'throwCommit' => false, 'deletes' => 0];
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/'.$unit.'-commit-unknown';
    $factory = static function (string $path) use ($state): PDO {
        $state->factory++;

        return new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION, PDO::ATTR_STRINGIFY_FETCHES => false]);
            }

            public function prepare(string $query, array $options = []): PDOStatement|false
            {
                if (str_starts_with($query, 'DELETE FROM tickets')) {
                    $this->state->deletes++;
                }

                return parent::prepare($query, $options);
            }

            public function commit(): bool
            {
                if ($this->state->throwCommit) {
                    parent::commit();
                    throw new RuntimeException('unknown delete commit');
                }

                return parent::commit();
            }
        };
    };
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0, $factory);
    $ticket = $database->admit();
    $state->throwCommit = true;

    $delete = $unit === 'foreign'
        ? fn () => $database->deleteForeignHead($ticket)
        : fn () => $database->deleteExact($ticket);
    expect($delete)->toThrow(RuntimeException::class, 'unknown delete commit')
        ->and($state->deletes)->toBe(1)
        ->and($state->factory)->toBe(1);
    $state->throwCommit = false;
    expect($database->readHead())->toBeNull()
        ->and($state->factory)->toBe(2);
})->with(['foreign', 'normal']);

it('retries a numeric busy or locked head read only after arm and drain', function (int $code) {
    $state = (object) ['throwHead' => false, 'headCalls' => 0];
    $wait = (object) ['events' => []];
    $waiter = new class($wait) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->events[] = 'arm';
        }

        public function drain(): void
        {
            $this->state->events[] = 'drain';
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->events[] = 'block';
        }
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/head-busy-'.$code,
        $waiter,
        static fn (): float => 0.0,
        static fn (string $path): PDO => new class($path, $state, $code) extends PDO
        {
            public function __construct(string $path, private object $state, private int $code)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
            {
                if ($this->state->throwHead && str_starts_with($query, 'SELECT ticket') && ++$this->state->headCalls === 1) {
                    $exception = new PDOException('numeric contention');
                    $exception->errorInfo = ['HY000', $this->code, 'ignored'];
                    throw $exception;
                }

                return $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
            }
        },
    );
    $database->open();
    $state->throwHead = true;

    expect($database->readHead())->toBeNull()
        ->and($state->headCalls)->toBe(2)
        ->and($wait->events)->toBe(['arm', 'drain']);
})->with([5, 6, 261, 262]);

it('rejects invalid ticket columns without changing the schema', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/invalid-columns';
    mkdir($directory, 0775, true);
    $pdo = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pdo->exec('PRAGMA journal_mode=DELETE');
    $pdo->exec('PRAGMA synchronous=NORMAL');
    $pdo->exec('CREATE TABLE tickets (wrong INTEGER PRIMARY KEY AUTOINCREMENT)');
    $pdo->exec('PRAGMA user_version=1');
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0);

    expect(fn () => $database->open())->toThrow(RuntimeException::class)
        ->and($pdo->query('PRAGMA table_info(tickets)')->fetchAll(PDO::FETCH_COLUMN, 1))->toBe(['wrong']);
});

it('rejects a ticket primary key without autoincrement without changing it', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/invalid-autoincrement';
    mkdir($directory, 0775, true);
    $pdo = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pdo->exec('PRAGMA journal_mode=DELETE');
    $pdo->exec('PRAGMA synchronous=NORMAL');
    $pdo->exec('CREATE TABLE tickets (ticket INTEGER PRIMARY KEY)');
    $pdo->exec('PRAGMA user_version=1');
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0);

    expect(fn () => $database->open())->toThrow(RuntimeException::class)
        ->and($pdo->query("SELECT sql FROM sqlite_master WHERE name='tickets'")->fetchColumn())
        ->toBe('CREATE TABLE tickets (ticket INTEGER PRIMARY KEY)');
});

it('fails setup immediately for a non-busy error and invalidates the handle', function () {
    $state = (object) ['factory' => 0, 'calls' => 0];
    $factory = static function (string $path) use ($state): PDO {
        $state->factory++;

        return new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function exec(string $statement): int|false
            {
                $this->state->calls++;
                if ($statement === 'PRAGMA busy_timeout=0') {
                    throw new RuntimeException('permanent setup failure');
                }

                return parent::exec($statement);
            }
        };
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/setup-permanent', new PollingWaiter, static fn (): float => 0.0, $factory);

    expect(fn () => $database->open())->toThrow(RuntimeException::class, 'permanent setup failure')
        ->and($state->calls)->toBe(1)
        ->and($state->factory)->toBe(1);
});

it('stops a busy setup retry at the supplied absolute deadline', function () {
    $state = (object) ['calls' => 0];
    $now = -1.0;
    $clock = static function () use (&$now): float {
        return ++$now;
    };
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $this->state->calls++;
            $exception = new PDOException('busy setup');
            $exception->errorInfo = ['HY000', 5, 'ignored'];
            throw $exception;
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/setup-deadline', new PollingWaiter, $clock, $factory);

    expect(fn () => $database->open(1.0))->toThrow(FairWaitTimeoutException::class, 'deadline')
        ->and($state->calls)->toBe(1);
});

it('retries numeric setup contention after arming and draining', function (int $code) {
    $state = (object) ['calls' => 0, 'events' => []];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->events[] = 'arm';
        }

        public function drain(): void
        {
            $this->state->events[] = 'drain';
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->events[] = 'block';
        }
    };
    $factory = static fn (string $path): PDO => new class($path, $state, $code) extends PDO
    {
        public function __construct(string $path, private object $state, private int $code)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'PRAGMA busy_timeout=0' && ++$this->state->calls === 1) {
                $exception = new PDOException('numeric setup contention');
                $exception->errorInfo = ['HY000', $this->code, 'ignored'];
                throw $exception;
            }

            return parent::exec($statement);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/setup-busy-'.$code, $waiter, static fn (): float => 0.0, $factory);
    $database->open();

    expect($state->calls)->toBe(2)
        ->and($state->events)->toBe(['arm', 'drain']);
})->with([5, 6, 261, 262]);

it('rolls back and retries a busy statement inside each mutation unit', function (string $unit) {
    $state = (object) ['unit' => null, 'attempts' => 0, 'events' => []];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->events[] = 'arm';
        }

        public function drain(): void
        {
            $this->state->events[] = 'drain';
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->events[] = 'block';
        }
    };
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($this->state->unit === 'admission' && $statement === 'INSERT INTO tickets DEFAULT VALUES' && ++$this->state->attempts === 1) {
                $exception = new PDOException('busy admission');
                $exception->errorInfo = ['HY000', 5, 'ignored'];
                throw $exception;
            }

            return parent::exec($statement);
        }

        public function prepare(string $query, array $options = []): PDOStatement|false
        {
            if (in_array($this->state->unit, ['foreign', 'normal'], true) && str_starts_with($query, 'DELETE FROM tickets') && ++$this->state->attempts === 1) {
                $exception = new PDOException('busy delete');
                $exception->errorInfo = ['HY000', 6, 'ignored'];
                throw $exception;
            }

            return parent::prepare($query, $options);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/mutation-busy-'.$unit, $waiter, static fn (): float => 0.0, $factory);
    $ticket = $database->admit();
    $state->unit = $unit;
    $state->attempts = 0;

    match ($unit) {
        'admission' => $database->admit(),
        'foreign' => $database->deleteForeignHead($ticket),
        'normal' => $database->deleteExact($ticket),
    };
    expect($state->attempts)->toBe(2)
        ->and($state->events)->toBe(['arm', 'drain']);
})->with(['admission', 'foreign', 'normal']);

it('keeps the statement error primary and invalidates after rollback failure', function (string $unit) {
    $state = (object) ['unit' => null, 'factory' => 0, 'rollback' => 0];
    $factory = static function (string $path) use ($state): PDO {
        $state->factory++;

        return new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function exec(string $statement): int|false
            {
                if ($this->state->unit === 'admission' && $statement === 'INSERT INTO tickets DEFAULT VALUES') {
                    throw new RuntimeException('primary admission statement');
                }

                return parent::exec($statement);
            }

            public function prepare(string $query, array $options = []): PDOStatement|false
            {
                if (in_array($this->state->unit, ['foreign', 'normal'], true) && str_starts_with($query, 'DELETE FROM tickets')) {
                    throw new RuntimeException('primary delete statement');
                }

                return parent::prepare($query, $options);
            }

            public function rollBack(): bool
            {
                if ($this->state->unit !== null) {
                    $this->state->rollback++;
                    throw new RuntimeException('secondary rollback');
                }

                return parent::rollBack();
            }
        };
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/rollback-failure-'.$unit, new PollingWaiter, static fn (): float => 0.0, $factory);
    $ticket = $database->admit();
    $state->unit = $unit;
    $operation = match ($unit) {
        'admission' => fn () => $database->admit(),
        'foreign' => fn () => $database->deleteForeignHead($ticket),
        'normal' => fn () => $database->deleteExact($ticket),
    };

    expect($operation)->toThrow(RuntimeException::class, 'primary')
        ->and($state->rollback)->toBe(1)
        ->and($state->factory)->toBe(1);
    $state->unit = null;
    expect($database->readHead())->toBe($ticket)
        ->and($state->factory)->toBe(2);
})->with(['admission', 'foreign', 'normal']);

it('retries numeric contention before begin for every mutation unit', function (string $unit) {
    $state = (object) ['unit' => $unit === 'bootstrap' ? 'bootstrap' : null, 'begins' => 0];
    $waiter = new class implements Waiter
    {
        public function beginContention(): void {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN EXCLUSIVE' && $this->state->unit !== null && ++$this->state->begins === 1) {
                $exception = new PDOException('busy before begin');
                $exception->errorInfo = ['HY000', 5, 'ignored'];
                throw $exception;
            }

            return parent::exec($statement);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/begin-busy-'.$unit, $waiter, static fn (): float => 0.0, $factory);
    if ($unit === 'bootstrap') {
        $database->open();
    } else {
        $ticket = $database->admit();
        $state->unit = $unit;
        $state->begins = 0;
        match ($unit) {
            'admission' => $database->admit(),
            'foreign' => $database->deleteForeignHead($ticket),
            'normal' => $database->deleteExact($ticket),
        };
    }
    expect($state->begins)->toBe(2);
})->with(['bootstrap', 'admission', 'foreign', 'normal']);

it('blocks only after the immediate second begin statecheck is still busy', function () {
    $state = (object) ['begins' => 0, 'events' => []];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->events[] = 'arm';
        }

        public function drain(): void
        {
            $this->state->events[] = 'drain';
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->events[] = 'block';
        }
    };
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN EXCLUSIVE' && ++$this->state->begins <= 2) {
                $exception = new PDOException('busy begin');
                $exception->errorInfo = ['HY000', 5, 'ignored'];
                throw $exception;
            }

            return parent::exec($statement);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/begin-second-busy', $waiter, static fn (): float => 0.0, $factory);
    $database->open();

    expect($state->begins)->toBe(3)
        ->and($state->events)->toBe(['arm', 'drain', 'block']);
});

it('makes exactly one cleanup statement attempt for busy and permanent failures', function (bool $busy) {
    $state = (object) ['fail' => false, 'prepares' => 0, 'waits' => 0];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->waits++;
        }

        public function drain(): void
        {
            $this->state->waits++;
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->waits++;
        }
    };
    $factory = static fn (string $path): PDO => new class($path, $state, $busy) extends PDO
    {
        public function __construct(string $path, private object $state, private bool $busy)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function prepare(string $query, array $options = []): PDOStatement|false
        {
            if ($this->state->fail && str_starts_with($query, 'DELETE FROM tickets')) {
                $this->state->prepares++;
                if ($this->busy) {
                    $exception = new PDOException('busy cleanup');
                    $exception->errorInfo = ['HY000', 5, 'ignored'];
                    throw $exception;
                }
                throw new RuntimeException('permanent cleanup');
            }

            return parent::prepare($query, $options);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/cleanup-failure-'.($busy ? 'busy' : 'permanent'), $waiter, static fn (): float => 0.0, $factory);
    $ticket = $database->admit();
    $state->fail = true;

    expect(fn () => $database->cleanupExact($ticket))->toThrow($busy ? PDOException::class : RuntimeException::class)
        ->and($state->prepares)->toBe(1)
        ->and($state->waits)->toBe(0);
})->with([true, false]);

it('handles busy deadline and permanent failures during final validation', function (string $mode) {
    $state = (object) ['final' => false, 'throws' => 0, 'reads' => 0];
    $clock = static fn (): float => $mode === 'deadline' && $state->throws > 0 ? 2.0 : 0.0;
    $waiter = new class implements Waiter
    {
        public function beginContention(): void {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $factory = static fn (string $path): PDO => new class($path, $state, $mode) extends PDO
    {
        public function __construct(string $path, private object $state, private string $mode)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function commit(): bool
        {
            $result = parent::commit();
            $this->state->final = true;

            return $result;
        }

        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
        {
            if ($this->state->final && $query === 'PRAGMA busy_timeout') {
                $this->state->reads++;
                if ($this->state->throws++ === 0) {
                    if ($this->mode === 'permanent') {
                        throw new RuntimeException('permanent final validation');
                    }
                    $exception = new PDOException('busy final validation');
                    $exception->errorInfo = ['HY000', 5, 'ignored'];
                    throw $exception;
                }
            }

            return $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/final-validation-'.$mode, $waiter, $clock, $factory);

    if ($mode === 'busy') {
        $database->open(1.0);
        expect($state->reads)->toBe(2);
    } elseif ($mode === 'deadline') {
        expect(fn () => $database->open(1.0))->toThrow(FairWaitTimeoutException::class, 'deadline')
            ->and($state->reads)->toBe(1);
    } else {
        expect(fn () => $database->open(1.0))->toThrow(RuntimeException::class, 'permanent final validation')
            ->and($state->reads)->toBe(1);
    }
})->with(['busy', 'deadline', 'permanent']);

it('retries only the active idempotent setup statement for numeric contention', function (string $statement, int $code) {
    $state = (object) ['calls' => 0, 'events' => []];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function beginContention(): void {}

        public function arm(): void
        {
            $this->state->events[] = 'arm';
        }

        public function drain(): void
        {
            $this->state->events[] = 'drain';
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->events[] = ['block', $deadline];
        }
    };
    $factory = static fn (string $path): PDO => new class($path, $state, $statement, $code) extends PDO
    {
        public function __construct(string $path, private object $state, private string $target, private int $code)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === $this->target && ++$this->state->calls === 1) {
                $exception = new PDOException('setup contention');
                $exception->errorInfo = ['HY000', $this->code, 'ignored'];
                throw $exception;
            }

            return parent::exec($statement);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/setup-unit-'.md5($statement.$code), $waiter, static fn (): float => 3.0, $factory);
    $database->open(9.0);

    expect($state->calls)->toBe(2)
        ->and($state->events)->toBe(['arm', 'drain']);
})->with([
    ['PRAGMA busy_timeout=0', 5],
    ['PRAGMA journal_mode=DELETE', 6],
    ['PRAGMA synchronous=NORMAL', 261],
    ['PRAGMA synchronous=NORMAL', 262],
]);

it('does not retry permanent setup statement failures', function (string $statement) {
    $state = (object) ['calls' => 0];
    $factory = static fn (string $path): PDO => new class($path, $state, $statement) extends PDO
    {
        public function __construct(string $path, private object $state, private string $target)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === $this->target) {
                $this->state->calls++;
                throw new RuntimeException('permanent setup unit');
            }

            return parent::exec($statement);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/setup-permanent-'.md5($statement), new PollingWaiter, static fn (): float => 0.0, $factory);

    expect(fn () => $database->open())->toThrow(RuntimeException::class, 'permanent setup unit')
        ->and($state->calls)->toBe(1);
})->with(['PRAGMA busy_timeout=0', 'PRAGMA journal_mode=DELETE', 'PRAGMA synchronous=NORMAL']);

it('rejects invalid final pragma readbacks without replacing the schema', function (string $pragma, string $wrongSelect) {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/invalid-final-'.str_replace('_', '-', $pragma);
    mkdir($directory, 0775, true);
    $seed = new PDO('sqlite:'.$directory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $seed->exec('PRAGMA journal_mode=DELETE');
    $seed->exec('PRAGMA synchronous=NORMAL');
    $seed->exec('CREATE TABLE tickets (ticket INTEGER PRIMARY KEY AUTOINCREMENT)');
    $seed->exec('PRAGMA user_version=1');
    $state = (object) ['reads' => 0];
    $factory = static fn (string $path): PDO => new class($path, $state, $pragma, $wrongSelect) extends PDO
    {
        public function __construct(string $path, private object $state, private string $pragma, private string $wrongSelect)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
        {
            if ($query === 'PRAGMA '.$this->pragma && ++$this->state->reads === 2) {
                return parent::query($this->wrongSelect);
            }

            return $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
        }
    };
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0, $factory);

    expect(fn () => $database->open())->toThrow(RuntimeException::class, 'PRAGMA validation')
        ->and($seed->query("SELECT sql FROM sqlite_master WHERE name='tickets'")->fetchColumn())
        ->toBe('CREATE TABLE tickets (ticket INTEGER PRIMARY KEY AUTOINCREMENT)');
})->with([
    ['busy_timeout', 'SELECT 9'],
    ['journal_mode', "SELECT 'wal'"],
    ['synchronous', 'SELECT 2'],
]);

it('rejects a corrupt lock database without replacing its bytes', function () {
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/corrupt-lock';
    mkdir($directory, 0775, true);
    $path = $directory.'/lock.sqlite';
    copy(dirname(__DIR__).'/Fixtures/corrupt-lock.sqlite', $path);
    $before = hash_file('sha256', $path);
    $database = new LockDatabase($directory, new PollingWaiter, static fn (): float => 0.0);

    expect(fn () => $database->open())->toThrow(PDOException::class)
        ->and(hash_file('sha256', $path))->toBe($before);
});

it('does not retry a permanent head-read failure', function () {
    $state = (object) ['fail' => false, 'reads' => 0];
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
        {
            if ($this->state->fail && str_starts_with($query, 'SELECT ticket')) {
                $this->state->reads++;
                throw new RuntimeException('permanent head read');
            }

            return $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/head-permanent', new PollingWaiter, static fn (): float => 0.0, $factory);
    $database->open();
    $state->fail = true;
    expect(fn () => $database->readHead())->toThrow(RuntimeException::class, 'permanent head read')
        ->and($state->reads)->toBe(1);
});

it('stops a busy head read before its second statecheck at the absolute deadline', function () {
    $state = (object) ['fail' => false, 'reads' => 0];
    $now = -1.0;
    $clock = static function () use (&$now): float {
        return ++$now;
    };
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
        {
            if ($this->state->fail && str_starts_with($query, 'SELECT ticket')) {
                $this->state->reads++;
                $e = new PDOException('busy head');
                $e->errorInfo = ['HY000', 5, 'ignored'];
                throw $e;
            }

            return $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/head-deadline', new PollingWaiter, $clock, $factory);
    $database->open();
    $state->fail = true;
    $now = -1.0;
    expect(fn () => $database->readHead(1.0))->toThrow(FairWaitTimeoutException::class, 'deadline')
        ->and($state->reads)->toBe(1);
});

it('stops exact delete begin and statement retries at the same absolute deadline', function (string $point) {
    $state = (object) ['active' => false, 'attempts' => 0];
    $clock = static fn (): float => $state->attempts > 0 ? 2.0 : 0.0;
    $factory = static fn (string $path): PDO => new class($path, $state, $point) extends PDO
    {
        public function __construct(string $path, private object $state, private string $point)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($this->state->active && $this->point === 'begin' && $statement === 'BEGIN EXCLUSIVE') {
                $this->state->attempts++;
                $e = new PDOException('busy delete begin');
                $e->errorInfo = ['HY000', 5, 'ignored'];
                throw $e;
            }

            return parent::exec($statement);
        }

        public function prepare(string $query, array $options = []): PDOStatement|false
        {
            if ($this->state->active && $this->point === 'statement' && str_starts_with($query, 'DELETE FROM tickets')) {
                $this->state->attempts++;
                $e = new PDOException('busy delete statement');
                $e->errorInfo = ['HY000', 6, 'ignored'];
                throw $e;
            }

            return parent::prepare($query, $options);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/delete-deadline-'.$point, new PollingWaiter, $clock, $factory);
    $ticket = $database->admit();
    $state->active = true;
    expect(fn () => $database->deleteExact($ticket, 1.0))->toThrow(FairWaitTimeoutException::class, 'deadline')
        ->and($state->attempts)->toBe(1)
        ->and($database->readHead())->toBe($ticket);
})->with(['begin', 'statement']);

it('handles bootstrap statement rollback and commit outcomes without unsafe replay', function (string $mode) {
    Log::spy();
    $state = (object) ['mode' => $mode, 'creates' => 0, 'rollbacks' => 0, 'factory' => 0];
    $waiter = new class implements Waiter
    {
        public function beginContention(): void {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $factory = static function (string $path) use ($state): PDO {
        $state->factory++;

        return new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function exec(string $statement): int|false
            {
                if (str_starts_with($statement, 'CREATE TABLE tickets')) {
                    $this->state->creates++;
                    if ($this->state->mode === 'busy' && $this->state->creates === 1) {
                        $exception = new PDOException('busy bootstrap statement');
                        $exception->errorInfo = ['HY000', 5, 'ignored'];
                        throw $exception;
                    }
                    if (in_array($this->state->mode, ['permanent', 'rollback'], true)) {
                        throw new RuntimeException('primary bootstrap statement');
                    }
                }

                return parent::exec($statement);
            }

            public function rollBack(): bool
            {
                $this->state->rollbacks++;
                if ($this->state->mode === 'rollback') {
                    throw new RuntimeException('secondary bootstrap rollback');
                }

                return parent::rollBack();
            }

            public function commit(): bool
            {
                if ($this->state->mode === 'commit') {
                    parent::commit();
                    throw new RuntimeException('unknown bootstrap commit');
                }

                return parent::commit();
            }
        };
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/bootstrap-outcome-'.$mode, $waiter, static fn (): float => 0.0, $factory, true);

    if ($mode === 'busy') {
        $database->open();
        expect($state->creates)->toBe(2)->and($state->rollbacks)->toBe(1);

        return;
    }
    $message = $mode === 'commit' ? 'unknown bootstrap commit' : 'primary bootstrap statement';
    expect(fn () => $database->open())->toThrow(RuntimeException::class, $message)
        ->and($state->creates)->toBe(1)
        ->and($state->factory)->toBe(1);
    if ($mode === 'commit') {
        Log::shouldNotHaveReceived(
            'debug',
            static fn (string $logMessage, array $context): bool => $logMessage === 'Fair SQLite transition.'
                && ($context['event'] ?? null) === 'lock_database_bootstrap',
        );
    }
    $state->mode = 'none';
    $database->open();
    expect($state->factory)->toBe(2);
})->with(['busy', 'permanent', 'rollback', 'commit']);

it('does not retry permanent statement errors after successful rollback', function (string $unit) {
    $state = (object) ['unit' => null, 'attempts' => 0];
    $factory = static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($this->state->unit === 'admission' && $statement === 'INSERT INTO tickets DEFAULT VALUES') {
                $this->state->attempts++;
                throw new RuntimeException('permanent admission statement');
            }

            return parent::exec($statement);
        }

        public function prepare(string $query, array $options = []): PDOStatement|false
        {
            if (in_array($this->state->unit, ['foreign', 'normal'], true) && str_starts_with($query, 'DELETE FROM tickets')) {
                $this->state->attempts++;
                throw new RuntimeException('permanent delete statement');
            }

            return parent::prepare($query, $options);
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/permanent-statement-'.$unit, new PollingWaiter, static fn (): float => 0.0, $factory);
    $ticket = $database->admit();
    $state->unit = $unit;
    $operation = match ($unit) {
        'admission' => fn () => $database->admit(),
        'foreign' => fn () => $database->deleteForeignHead($ticket),
        'normal' => fn () => $database->deleteExact($ticket),
    };

    expect($operation)->toThrow(RuntimeException::class, 'permanent')
        ->and($state->attempts)->toBe(1)
        ->and($database->readHead())->toBe($ticket);
})->with(['admission', 'foreign', 'normal']);
