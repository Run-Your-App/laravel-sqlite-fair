<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Log;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Lock\FairSQLiteLock;
use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;
use RunYourApp\LaravelSqliteFair\Wait\PollingWaiter;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;

it('acquires without a ticket and restores the active app busy timeout', function (int $busyTimeout) {
    $app = new PDO('sqlite:'.$GLOBALS['sqliteFairTestRunDirectory'].'/direct.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $app->exec('PRAGMA busy_timeout='.$busyTimeout);
    $waiter = new PollingWaiter;
    $clock = static fn (): float => hrtime(true) / 1e9;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/direct-lock', $waiter, $clock);
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire())->toBeNull()
        ->and($app->inTransaction())->toBeTrue()
        ->and((int) $app->query('PRAGMA busy_timeout')->fetchColumn())->toBe($busyTimeout);
    $app->rollBack();
})->with([1000, 10000, 4321]);

it('admits before the first app fence when queued acquisition is forced', function () {
    $state = (object) ['database' => null, 'headsAtBegin' => []];
    $clock = static fn (): float => 0.0;
    $waiter = new PollingWaiter;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/forced-queued-lock', $waiter, $clock);
    $state->database = $database;
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/forced-queued.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->headsAtBegin[] = $this->state->database->readHead();
            }

            return parent::exec($statement);
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquireQueued())->toBe(1)
        ->and($state->headsAtBegin)->toBe([1]);
    $app->rollBack();
    $database->deleteExact(1);
});

it('requeues a forced queued acquisition after fenced ownership revalidation is lost', function () {
    $state = (object) ['database' => null, 'begins' => 0];
    $clock = static fn (): float => 0.0;
    $waiter = new PollingWaiter;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/forced-requeue-lock', $waiter, $clock);
    $state->database = $database;
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/forced-requeue.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE' && ++$this->state->begins === 1) {
                $this->state->database->deleteExact(1);
            }

            return $result;
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquireQueued())->toBe(2)
        ->and($state->begins)->toBe(2)
        ->and($database->readHead())->toBe(2);
    $app->rollBack();
    $database->deleteExact(2);
});

it('throws the typed fair timeout after rolling back before one queued cleanup', function () {
    $state = (object) ['now' => 0.0, 'events' => []];
    $clock = static fn (): float => $state->now;
    $waiter = new PollingWaiter;
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/typed-timeout-lock',
        $waiter,
        $clock,
        static fn (string $path): PDO => new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function prepare(string $query, array $options = []): PDOStatement|false
            {
                if (str_contains($query, ':ownTicket')) {
                    $this->state->events[] = 'cleanup';
                }

                return parent::prepare($query, $options);
            }
        },
    );
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/typed-timeout.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->events[] = 'fence';
                $this->state->now = 2.0;
            }

            return $result;
        }

        public function rollBack(): bool
        {
            $this->state->events[] = 'rollback';

            return parent::rollBack();
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect(fn () => $lock->acquireQueued(1.0))
        ->toThrow(FairWaitTimeoutException::class, 'lock deadline')
        ->and(is_subclass_of(FairWaitTimeoutException::class, FairSQLiteException::class))->toBeTrue()
        ->and($state->events)->toBe(['fence', 'rollback', 'cleanup'])
        ->and($database->readHead())->toBeNull();
});

it('throws the typed lock-owner timeout after a bounded queued wait', function () {
    $state = (object) ['now' => 0.0];
    $clock = static fn (): float => $state->now;
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->now = 2.0;
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/typed-lock-owner-timeout-lock', $waiter, $clock);
    $foreign = $database->admit();
    $app = new PDO('sqlite:'.$GLOBALS['sqliteFairTestRunDirectory'].'/typed-lock-owner-timeout.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect(fn () => $lock->acquireQueued(1.0))
        ->toThrow(FairWaitTimeoutException::class, 'writer wait deadline')
        ->and($database->readHead())->toBe($foreign);
});

it('does not delete a ticket when the deadline expires before admission', function () {
    $state = (object) ['clockCalls' => 0, 'deletes' => 0];
    $clock = static function () use ($state): float {
        return $state->clockCalls++ === 0 ? 0.0 : 2.0;
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/typed-pre-admission-timeout-lock',
        new PollingWaiter,
        $clock,
        static fn (string $path): PDO => new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function prepare(string $query, array $options = []): PDOStatement|false
            {
                if (str_contains($query, ':ownTicket')) {
                    $this->state->deletes++;
                }

                return parent::prepare($query, $options);
            }
        },
    );
    $app = new PDO('sqlite:'.$GLOBALS['sqliteFairTestRunDirectory'].'/typed-pre-admission-timeout.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, new PollingWaiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect(fn () => $lock->acquire(1.0))
        ->toThrow(FairWaitTimeoutException::class)
        ->and($state->deletes)->toBe(0);
});

it('queues behind an existing ticket and cleans its own aborted ticket once', function () {
    $app = new PDO('sqlite:'.$GLOBALS['sqliteFairTestRunDirectory'].'/queued.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $waiter = new PollingWaiter;
    $times = [0.0, 0.0, 0.2, 0.4, 0.6, 1.1];
    $clock = static function () use (&$times): float {
        return array_shift($times) ?? 1.1;
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/queued-lock', $waiter, $clock);
    $head = $database->admit();
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    try {
        $lock->acquire(1.0);
    } catch (RuntimeException) {
    }

    expect($database->readHead())->toBe($head);
});

it('fails app busy-timeout read zero and restore at their exact pre-business boundary', function (string $mode) {
    $state = (object) ['mode' => $mode, 'began' => false, 'rollbacks' => 0, 'restoreAttempts' => 0];
    $path = $GLOBALS['sqliteFairTestRunDirectory'].'/app-timeout-'.$mode.'.sqlite';
    $app = new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            parent::exec('PRAGMA busy_timeout=777');
        }

        public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
        {
            if ($this->state->mode === 'read' && $query === 'PRAGMA busy_timeout') {
                throw new RuntimeException('busy timeout read failure');
            }

            return $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
        }

        public function exec(string $statement): int|false
        {
            if ($this->state->mode === 'zero' && $statement === 'PRAGMA busy_timeout=0') {
                throw new RuntimeException('busy timeout zero failure');
            }
            if ($statement === 'BEGIN IMMEDIATE') {
                $result = parent::exec($statement);
                $this->state->began = true;

                return $result;
            }
            if (str_starts_with($this->state->mode, 'restore') && $this->state->began && $statement === 'PRAGMA busy_timeout=777') {
                $this->state->restoreAttempts++;
                if ($this->state->mode !== 'restore' || $this->state->restoreAttempts === 1) {
                    throw new RuntimeException('busy timeout restore failure');
                }
            }

            return parent::exec($statement);
        }

        public function rollBack(): bool
        {
            $this->state->rollbacks++;
            if ($this->state->mode === 'restore-unknown') {
                throw new RuntimeException('unknown app rollback');
            }

            return parent::rollBack();
        }
    };
    $clock = static fn (): float => 0.0;
    $waiter = new PollingWaiter;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/app-timeout-lock-'.$mode, $waiter, $clock);
    $events = [];
    $lock = new FairSQLiteLock(
        $app,
        $database,
        $waiter,
        10.0,
        static function (Throwable $e) use (&$events): void {
            $events[] = 'unknown';
        },
        static function () use (&$events): void {
            $events[] = 'disconnect';
        },
        $clock,
    );

    expect(fn () => $lock->acquire(1.0))->toThrow(RuntimeException::class, 'busy timeout')
        ->and($database->readHead())->toBeNull()
        ->and($events)->toBe(match ($mode) {
            'restore-repeat' => ['disconnect'],
            'restore-unknown' => ['unknown', 'disconnect'],
            default => [],
        })
        ->and($state->rollbacks)->toBe(str_starts_with($mode, 'restore') ? 1 : 0)
        ->and($state->restoreAttempts)->toBe(match ($mode) {
            'restore' => 2,
            'restore-repeat' => 2,
            'restore-unknown' => 1,
            default => 0,
        })
        ->and($app->inTransaction())->toBe($mode === 'restore-unknown');

    if ($mode === 'restore') {
        expect((int) $app->query('PRAGMA busy_timeout')->fetchColumn())->toBe(777);
    }
})->with(['read', 'zero', 'restore', 'restore-repeat', 'restore-unknown']);

it('arms drains and rechecks the queued condition before blocking', function () {
    $state = (object) ['events' => [], 'database' => null, 'changed' => false];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void
        {
            $this->state->events[] = 'arm';
        }

        public function drain(): void
        {
            $this->state->events[] = 'drain';
            if (! $this->state->changed) {
                $this->state->changed = true;
                $this->state->database->deleteForeignHead(1);
            }
        }

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->events[] = 'block';
        }
    };
    $clock = static fn (): float => 0.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/second-check-lock', $waiter, $clock);
    $state->database = $database;
    expect($database->admit())->toBe(1);
    $app = new PDO('sqlite:'.$GLOBALS['sqliteFairTestRunDirectory'].'/second-check.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire(1.0))->toBe(2)
        ->and($state->events)->toBe(['arm', 'drain']);
    $app->rollBack();
    $database->deleteExact(2);
});

it('transitions from one busy direct begin to exactly one admission before queued retry', function () {
    $path = $GLOBALS['sqliteFairTestRunDirectory'].'/app-busy-transition.sqlite';
    $blocker = new PDO('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $blocker->exec('PRAGMA busy_timeout=0');
    $blocker->exec('BEGIN IMMEDIATE');
    $state = (object) ['begins' => 0, 'released' => false, 'beginsAtBlock' => []];
    $app = new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->begins++;
            }

            return parent::exec($statement);
        }
    };
    $waiter = new class($blocker, $state) implements Waiter
    {
        public function __construct(private PDO $blocker, private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->beginsAtBlock[] = $this->state->begins;
            if (! $this->state->released) {
                $this->blocker->rollBack();
                $this->state->released = true;
            }
        }
    };
    $clock = static fn (): float => 0.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/app-busy-transition-lock', $waiter, $clock);
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire(1.0))->toBe(1)
        ->and($state->beginsAtBlock)->toBe([2])
        ->and($state->begins)->toBe(3)
        ->and($database->readHead())->toBe(1);
    $app->rollBack();
    $database->deleteExact(1);
});

it('rolls back before cleaning one owned ticket and marks unknown outcome before disconnect', function (bool $unknown) {
    $lockState = (object) ['deletes' => 0];
    $clock = static fn (): float => 0.0;
    $waiter = new PollingWaiter;
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/abort-owned-'.($unknown ? 'unknown' : 'known'),
        $waiter,
        $clock,
        static fn (string $path): PDO => new class($path, $lockState) extends PDO
        {
            public function __construct(string $path, private object $state)
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
        },
    );
    expect($database->admit())->toBe(1);
    $appState = (object) ['rollbacks' => 0];
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/abort-owned.sqlite', $appState, $unknown) extends PDO
    {
        public function __construct(string $path, private object $state, private bool $unknown)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function rollBack(): bool
        {
            $this->state->rollbacks++;
            if ($this->unknown) {
                throw new RuntimeException('unknown rollback');
            }

            return parent::rollBack();
        }
    };
    $app->exec('BEGIN IMMEDIATE');
    $events = [];
    $lock = new FairSQLiteLock(
        $app,
        $database,
        $waiter,
        10.0,
        static function (Throwable $e) use (&$events): void {
            $events[] = 'unknown';
        },
        static function () use (&$events): void {
            $events[] = 'disconnect';
        },
        $clock,
    );

    $lock->abortBeforeBusiness(true, 1);
    expect($appState->rollbacks)->toBe(1)
        ->and($events)->toBe($unknown ? ['unknown', 'disconnect'] : [])
        ->and($lockState->deletes)->toBe(1)
        ->and($database->readHead())->toBeNull();
})->with([false, true]);

it('keeps a foreign non-head off the app fence below stale threshold then recovers fenced', function () {
    $state = (object) ['now' => 0.0, 'begins' => 0, 'below' => [], 'loops' => 0];
    $clock = static fn (): float => $state->now;
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/stale-count.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->begins++;
            }

            return parent::exec($statement);
        }
    };
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->below[] = $this->state->begins;
            $this->state->loops++;
            $this->state->now = match ($this->state->loops) {
                1 => 0.2, 2 => 0.4, default => 2.0
            };
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/stale-count-lock', $waiter, $clock);
    expect($database->admit())->toBe(1);
    $lock = new FairSQLiteLock($app, $database, $waiter, 1.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire())->toBe(2)
        ->and($state->below)->toBe([0, 0, 0])
        ->and($state->begins)->toBe(2);
    $app->rollBack();
    $database->deleteExact(2);
});

it('restores busy timeout after direct busy and cleans admission on permanent queued begin', function () {
    $path = $GLOBALS['sqliteFairTestRunDirectory'].'/queued-permanent.sqlite';
    $blocker = new PDO('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $blocker->exec('BEGIN IMMEDIATE');
    $state = (object) ['begins' => 0, 'released' => false, 'deadlines' => [], 'blocks' => 0];
    $app = new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            parent::exec('PRAGMA busy_timeout=2468');
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN IMMEDIATE' && ++$this->state->begins === 2) {
                throw new RuntimeException('permanent queued begin');
            }

            return parent::exec($statement);
        }
    };
    $waiter = new class($blocker, $state) implements Waiter
    {
        public function __construct(private PDO $blocker, private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->blocks++;
            $this->state->deadlines[] = $deadline;
            if (! $this->state->released) {
                $this->blocker->rollBack();
                $this->state->released = true;
            }
        }
    };
    $clock = static fn (): float => 10.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/queued-permanent-lock', $waiter, $clock);
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect(fn () => $lock->acquire(15.0))->toThrow(RuntimeException::class, 'permanent queued begin')
        ->and($state->begins)->toBe(2)
        ->and($state->blocks)->toBe(0)
        ->and($state->deadlines)->toBe([])
        ->and((int) $app->query('PRAGMA busy_timeout')->fetchColumn())->toBe(2468)
        ->and($database->readHead())->toBeNull();
});

it('never attempts an app fence while behind two or three committed foreign tickets', function (int $foreignTickets) {
    $state = (object) ['begins' => 0, 'database' => null, 'deleted' => 0];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void
        {
            $head = $this->state->database->readHead();
            if ($head !== null && $this->state->deleted < $this->state->foreignTickets) {
                expect($this->state->begins)->toBe(0);
                $this->state->database->deleteForeignHead($head);
                $this->state->deleted++;
            }
        }

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $state->foreignTickets = $foreignTickets;
    $clock = static fn (): float => 0.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/non-head-'.$foreignTickets, $waiter, $clock);
    $state->database = $database;
    for ($ticket = 1; $ticket <= $foreignTickets; $ticket++) {
        expect($database->admit())->toBe($ticket);
    }
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/non-head-'.$foreignTickets.'.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->begins++;
            }

            return parent::exec($statement);
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    $ownTicket = $foreignTickets + 1;
    expect($lock->acquire())->toBe($ownTicket)
        ->and($state->deleted)->toBe($foreignTickets)
        ->and($state->begins)->toBe(1);
    $app->rollBack();
    $database->deleteExact($ownTicket);
})->with([2, 3]);

it('rolls back and requeues with a higher ticket when under-fence ownership is missing or foreign', function (string $replacement) {
    $state = (object) ['database' => null, 'begins' => 0, 'removedInitial' => false, 'removedReplacement' => false];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void
        {
            $head = $this->state->database->readHead();
            if (! $this->state->removedInitial && $head === 1) {
                $this->state->database->deleteForeignHead(1);
                $this->state->removedInitial = true;
            } elseif (! $this->state->removedReplacement && $head === 3) {
                $this->state->database->deleteForeignHead(3);
                $this->state->removedReplacement = true;
            }
        }

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $clock = static fn (): float => 0.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/under-fence-'.$replacement, $waiter, $clock);
    $state->database = $database;
    expect($database->admit())->toBe(1);
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/under-fence-'.$replacement.'.sqlite', $state, $replacement) extends PDO
    {
        public function __construct(string $path, private object $state, private string $replacement)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE' && ++$this->state->begins === 1) {
                $this->state->database->deleteExact(2);
                if ($this->replacement === 'foreign') {
                    expect($this->state->database->admit())->toBe(3);
                }
            }

            return $result;
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    $expected = $replacement === 'missing' ? 3 : 4;
    expect($lock->acquire())->toBe($expected)
        ->and($state->begins)->toBe(2);
    $app->rollBack();
    $database->deleteExact($expected);
})->with(['missing', 'foreign']);

it('switches to queued ownership when a ticket appears between the two direct reads', function () {
    $state = (object) ['database' => null, 'injected' => false, 'pending' => false];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void
        {
            if ($this->state->pending) {
                $this->state->database->deleteForeignHead(1);
                $this->state->pending = false;
            }
        }

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $clock = static fn (): float => 0.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/between-reads-lock', $waiter, $clock);
    $state->database = $database;
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/between-reads.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE' && ! $this->state->injected) {
                $this->state->database->admit();
                $this->state->injected = true;
                $this->state->pending = true;
            }

            return $result;
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire())->toBe(2);
    $app->rollBack();
    $database->deleteExact(2);
});

it('does not replay an unknown direct-fence rollback after the second head read changes', function () {
    $state = (object) ['database' => null, 'rollbacks' => 0];
    $clock = static fn (): float => 0.0;
    $waiter = new PollingWaiter;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/direct-rollback-unknown-lock', $waiter, $clock);
    $state->database = $database;
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/direct-rollback-unknown.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->database->admit();
            }

            return $result;
        }

        public function rollBack(): bool
        {
            $this->state->rollbacks++;
            throw new RuntimeException('unknown direct rollback');
        }
    };
    $events = [];
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e) use (&$events): void {
        $events[] = 'unknown';
    }, static function () use (&$events): void {
        $events[] = 'disconnect';
    }, $clock);

    expect(fn () => $lock->acquire())->toThrow(RuntimeException::class, 'unknown direct rollback')
        ->and($state->rollbacks)->toBe(1)
        ->and($events)->toBe(['unknown', 'disconnect'])
        ->and($database->readHead())->toBe(1);
    $database->deleteExact(1);
});

it('does not replay unknown queued revalidation rollback and cleans its own ticket once', function () {
    $state = (object) ['database' => null, 'deletedForeign' => false, 'deletes' => 0, 'rollbacks' => 0];
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void
        {
            if (! $this->state->deletedForeign) {
                $this->state->database->deleteForeignHead(1);
                $this->state->deletedForeign = true;
            }
        }

        public function block(?float $deadline, callable $monotonic): void {}
    };
    $clock = static fn (): float => 0.0;
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/queued-rollback-unknown-lock', $waiter, $clock, static fn (string $path): PDO => new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
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
    });
    $state->database = $database;
    $database->admit();
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/queued-rollback-unknown.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->database->deleteExact(2);
            }

            return $result;
        }

        public function rollBack(): bool
        {
            $this->state->rollbacks++;
            throw new RuntimeException('unknown queued rollback');
        }
    };
    $events = [];
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e) use (&$events): void {
        $events[] = 'unknown';
    }, static function () use (&$events): void {
        $events[] = 'disconnect';
    }, $clock);

    expect(fn () => $lock->acquire())->toThrow(RuntimeException::class, 'unknown queued rollback')
        ->and($state->rollbacks)->toBe(1)
        ->and($events)->toBe(['unknown', 'disconnect'])
        ->and($state->deletes)->toBe(3)
        ->and($database->readHead())->toBeNull();
});

it('linearizes ticketless ownership when a ticket appears after the second direct read executes', function () {
    $state = (object) ['headReads' => 0];
    $clock = static fn (): float => 0.0;
    $waiter = new PollingWaiter;
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/after-reads-lock',
        $waiter,
        $clock,
        static fn (string $path): PDO => new class($path, $state) extends PDO
        {
            public function __construct(string $path, private object $state)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
            {
                $statement = $fetchMode === null ? parent::query($query) : parent::query($query, $fetchMode, ...$fetchModeArgs);
                if (str_starts_with($query, 'SELECT ticket') && ++$this->state->headReads === 2) {
                    parent::exec('INSERT INTO tickets DEFAULT VALUES');
                }

                return $statement;
            }
        },
    );
    $app = new PDO('sqlite:'.$GLOBALS['sqliteFairTestRunDirectory'].'/after-reads.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire())->toBeNull()
        ->and($database->readHead())->toBe(1);
    $app->rollBack();
    $database->deleteExact(1);
});

it('propagates one absolute deadline unchanged through lock database and fair waiter blocks', function () {
    $path = $GLOBALS['sqliteFairTestRunDirectory'].'/queued-repeat.sqlite';
    $blocker = new PDO('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $blocker->exec('BEGIN IMMEDIATE');
    $state = (object) ['now' => 10.0, 'headFailures' => 0, 'begins' => 0, 'blocks' => 0, 'deadlines' => [], 'beginsAtBlock' => []];
    $app = new class($path, $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            parent::exec('PRAGMA busy_timeout=9999');
        }

        public function exec(string $statement): int|false
        {
            if ($statement === 'BEGIN IMMEDIATE') {
                $this->state->begins++;
            }

            return parent::exec($statement);
        }
    };
    $waiter = new class($blocker, $state) implements Waiter
    {
        public function __construct(private PDO $blocker, private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->beginsAtBlock[] = $this->state->begins;
            $this->state->deadlines[] = $deadline;
            if (++$this->state->blocks === 2) {
                $this->blocker->rollBack();
            }
        }
    };
    $clock = static function () use ($state): float {
        return $state->now += 0.01;
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/queued-repeat-lock',
        $waiter,
        $clock,
        static fn (string $lockPath): PDO => new class($lockPath, $state) extends PDO
        {
            public function __construct(string $lockPath, private object $state)
            {
                parent::__construct('sqlite:'.$lockPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function query(string $query, ?int $fetchMode = null, mixed ...$fetchModeArgs): PDOStatement|false
            {
                if (str_starts_with($query, 'SELECT ticket') && $this->state->headFailures++ < 2) {
                    $exception = new PDOException('busy absolute-deadline head read');
                    $exception->errorInfo = ['HY000', 5, 'ignored'];

                    throw $exception;
                }

                return $fetchMode === null
                    ? parent::query($query)
                    : parent::query($query, $fetchMode, ...$fetchModeArgs);
            }
        },
    );
    $lock = new FairSQLiteLock($app, $database, $waiter, 10.0, static function (Throwable $e): void {}, static function (): void {}, $clock);

    expect($lock->acquire(25.0))->toBe(1)
        ->and($state->beginsAtBlock)->toBe([0, 2])
        ->and($state->begins)->toBe(3)
        ->and($state->deadlines)->toBe([25.0, 25.0])
        ->and((int) $app->query('PRAGMA busy_timeout')->fetchColumn())->toBe(9999);
    $app->rollBack();
    $database->deleteExact(1);
});

it('keeps recovery primary errors and cleans one own ticket after unknown fence rollback', function (string $mode) {
    $state = (object) ['now' => 0.0, 'foreignAttempts' => 0, 'ownCleanups' => 0, 'rollbacks' => 0];
    $clock = static fn (): float => $state->now;
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->now = 2.0;
        }
    };
    $database = new LockDatabase(
        $GLOBALS['sqliteFairTestRunDirectory'].'/recovery-rollback-'.$mode,
        $waiter,
        $clock,
        static fn (string $path): PDO => new class($path, $state, $mode) extends PDO
        {
            public function __construct(string $path, private object $state, private string $mode)
            {
                parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            }

            public function prepare(string $query, array $options = []): PDOStatement|false
            {
                if (str_contains($query, ':observedForeignHead')) {
                    $this->state->foreignAttempts++;
                    if ($this->mode === 'lock-error') {
                        throw new RuntimeException('primary foreign delete');
                    }
                }
                if (str_contains($query, ':ownTicket')) {
                    $this->state->ownCleanups++;
                }

                return parent::prepare($query, $options);
            }
        },
    );
    expect($database->admit())->toBe(1);
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/recovery-rollback-'.$mode.'.sqlite', $state) extends PDO
    {
        public function __construct(string $path, private object $state)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function rollBack(): bool
        {
            $this->state->rollbacks++;
            throw new RuntimeException('unknown recovery rollback');
        }
    };
    $events = [];
    $lock = new FairSQLiteLock($app, $database, $waiter, 1.0, static function (Throwable $e) use (&$events): void {
        $events[] = 'unknown';
    }, static function () use (&$events): void {
        $events[] = 'disconnect';
    }, $clock);
    $expectedPrimary = $mode === 'lock-error' ? 'primary foreign delete' : 'unknown recovery rollback';

    expect(fn () => $lock->acquire())->toThrow(RuntimeException::class, $expectedPrimary)
        ->and($state->foreignAttempts)->toBe(1)
        ->and($state->rollbacks)->toBe(1)
        ->and($events)->toBe(['unknown', 'disconnect'])
        ->and($state->ownCleanups)->toBe(1)
        ->and($database->readHead())->toBe($mode === 'lock-error' ? 1 : null);
})->with(['lock-error', 'success']);

it('resets stale observation when fenced revalidation changes or vanishes', function (string $revalidation) {
    Log::spy();
    $loggedRecoveryHeads = [];
    Log::shouldReceive('debug')->zeroOrMoreTimes()->andReturnUsing(
        static function (string $message, array $context) use (&$loggedRecoveryHeads): void {
            if ($message === 'Fair SQLite transition.' && ($context['event'] ?? null) === 'stale_head_recovered') {
                $loggedRecoveryHeads[] = $context['head_ticket'] ?? null;
            }
        },
    );
    $state = (object) ['now' => 0.0, 'begins' => 0, 'blocks' => 0, 'beginsAtBlock' => [], 'database' => null];
    $clock = static fn (): float => $state->now;
    $waiter = new class($state) implements Waiter
    {
        public function __construct(private object $state) {}

        public function arm(): void {}

        public function drain(): void {}

        public function block(?float $deadline, callable $monotonic): void
        {
            $this->state->beginsAtBlock[] = $this->state->begins;
            $this->state->now = ++$this->state->blocks * 2.0;
        }
    };
    $database = new LockDatabase($GLOBALS['sqliteFairTestRunDirectory'].'/revalidation-'.$revalidation, $waiter, $clock);
    $state->database = $database;
    $database->admit();
    if ($revalidation === 'changed') {
        $database->admit();
    }
    $ownTicket = $revalidation === 'changed' ? 3 : 2;
    $app = new class($GLOBALS['sqliteFairTestRunDirectory'].'/revalidation-'.$revalidation.'.sqlite', $state, $revalidation, $ownTicket) extends PDO
    {
        public function __construct(string $path, private object $state, private string $revalidation, private int $ownTicket)
        {
            parent::__construct('sqlite:'.$path, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            $result = parent::exec($statement);
            if ($statement === 'BEGIN IMMEDIATE' && ++$this->state->begins === 1) {
                $this->state->database->deleteForeignHead(1);
                if ($this->revalidation === 'vanished') {
                    $this->state->database->deleteExact($this->ownTicket);
                }
            }

            return $result;
        }
    };
    $lock = new FairSQLiteLock($app, $database, $waiter, 1.0, static function (Throwable $e): void {}, static function (): void {}, $clock, true);
    $returned = $lock->acquire();

    expect($state->beginsAtBlock)->toBe($revalidation === 'changed' ? [0, 1] : [0])
        ->and($returned)->toBe($revalidation === 'changed' ? 3 : 3)
        ->and($state->begins)->toBe($revalidation === 'changed' ? 3 : 2);
    expect($loggedRecoveryHeads)->not->toContain(1);
    $app->rollBack();
    $database->deleteExact($returned);
})->with(['changed', 'vanished']);
