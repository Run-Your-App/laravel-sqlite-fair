<?php

declare(strict_types=1);

use Illuminate\Database\DatabaseTransactionsManager;
use Illuminate\Database\DeadlockException;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Database\Events\TransactionBeginning;
use Illuminate\Database\Events\TransactionCommitted;
use Illuminate\Database\Events\TransactionCommitting;
use Illuminate\Database\Events\TransactionRolledBack;
use Illuminate\Events\Dispatcher;
use Illuminate\Foundation\Testing\DatabaseTransactionsManager as TestingDatabaseTransactionsManager;
use Illuminate\Foundation\Testing\RefreshDatabase;
use Illuminate\Support\Facades\Exceptions;
use Illuminate\Support\Facades\Log;
use Orchestra\Testbench\TestCase;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnection;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnector;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteServiceProvider;
use RunYourApp\LaravelSqliteFair\Wait\WaiterFactory;

class FairSQLiteRefreshDatabaseTestCase extends TestCase
{
    use RefreshDatabase;

    private ?Closure $refreshDatabaseTeardownAssertion = null;

    protected function tearDown(): void
    {
        $assertion = $this->refreshDatabaseTeardownAssertion;

        parent::tearDown();

        $assertion?->__invoke();
    }

    public function afterRefreshDatabaseTeardown(Closure $callback): void
    {
        $this->refreshDatabaseTeardownAssertion = $callback;
    }

    /** @return list<class-string> */
    protected function getPackageProviders($app): array
    {
        return [FairSQLiteServiceProvider::class];
    }

    protected function defineEnvironment($app): void
    {
        $workspace = $GLOBALS['sqliteFairTestRunDirectory'].'/refresh-database';
        if (! is_dir($workspace)) {
            mkdir($workspace, 0775, true);
        }
        $database = $workspace.'/app.sqlite';
        if (! is_file($database)) {
            touch($database);
        }
        $setup = new PDO('sqlite:'.$database, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $setup->exec('CREATE TABLE IF NOT EXISTS refresh_examples (value TEXT NOT NULL)');
        $setup = null;

        $app['config']->set('database.default', 'testing');
        $app['config']->set('database.connections.testing', [
            'driver' => 'fair-sqlite',
            'database' => $database,
            'prefix' => '',
            'lock_directory' => $workspace.'/lock',
            'stale_head_seconds' => 10.0,
            'wait_strategy' => 'polling',
            'debug' => false,
        ]);
        $app['config']->set('sqlite-fair', [
            'lock_directory' => $workspace.'/lock',
            'stale_head_seconds' => 10.0,
            'wait_strategy' => 'polling',
            'debug' => false,
        ]);
    }
}

uses(FairSQLiteRefreshDatabaseTestCase::class);

beforeEach(function (): void {
    $this->workspace = $GLOBALS['sqliteFairTestRunDirectory'].'/connection-'.str_replace('.', '-', uniqid('', true));
    mkdir($this->workspace, 0775, true);
    $this->databasePath = $this->workspace.'/app.sqlite';
    touch($this->databasePath);
    $this->lockDirectory = $this->workspace.'/lock';
    config()->set('sqlite-fair', [
        'lock_directory' => $this->lockDirectory,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
        'debug' => false,
    ]);

    $this->connection = (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $this->databasePath,
        'prefix' => '',
        'lock_directory' => $this->lockDirectory,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
    ], 'connection-'.str_replace('.', '-', uniqid('', true)));
    $this->connection->setEventDispatcher(new Dispatcher());
    $this->connection->unprepared('CREATE TABLE examples (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)');
});

test('native connection startup prepares its missing lock directory before arming the waiter', function (): void {
    $capabilities = WaiterFactory::capabilities();
    if (! in_array($capabilities['platform'], ['linux', 'darwin'], true) || ! $capabilities['native_available']) {
        $this->markTestSkipped('Native startup directory ordering belongs to supported Linux and Darwin hosts.');
    }

    $workspace = $this->workspace.'/native-startup';
    mkdir($workspace, 0775, true);
    $databasePath = $workspace.'/app.sqlite';
    $lockDirectory = $workspace.'/missing-lock-directory';
    touch($databasePath);

    $connection = (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $databasePath,
        'prefix' => '',
        'lock_directory' => $lockDirectory,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'native',
    ], 'native-startup-'.str_replace('.', '-', uniqid('', true)));

    expect($connection)->toBeInstanceOf(FairSQLiteConnection::class)
        ->and(is_dir($lockDirectory))->toBeTrue();
});

test('refresh database uses the public fair transaction lifecycle on a file database', function (): void {
    $connection = app('db')->connection('testing');
    $databasePath = config('database.connections.testing.database');
    expect($databasePath)->toBeString();

    $connection->statement('INSERT INTO refresh_examples (value) VALUES (?)', ['rolled-back-by-trait']);
    $outside = new PDO('sqlite:'.$databasePath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

    expect($connection)->toBeInstanceOf(FairSQLiteConnection::class)
        ->and($connection->transactionLevel())->toBe(1)
        ->and($connection->getPdo()->inTransaction())->toBeTrue()
        ->and(app('db.transactions'))->toBeInstanceOf(TestingDatabaseTransactionsManager::class)
        ->and($connection->table('refresh_examples')->where('value', 'rolled-back-by-trait')->count())->toBe(1)
        ->and((int) $outside->query("SELECT COUNT(*) FROM refresh_examples WHERE value = 'rolled-back-by-trait'")->fetchColumn())->toBe(0);

    $this->afterRefreshDatabaseTeardown(function () use ($connection, $databasePath): void {
        $observer = new PDO('sqlite:'.$databasePath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

        expect($connection->transactionLevel())->toBe(0)
            ->and($connection->getRawPdo())->toBeNull()
            ->and((int) $observer->query("SELECT COUNT(*) FROM refresh_examples WHERE value = 'rolled-back-by-trait'")->fetchColumn())->toBe(0);
    });
});

test('manual outer transactions hold one fair lifecycle through commit and rollback', function (): void {
    $events = [];
    $dispatcher = $this->connection->getEventDispatcher();
    $dispatcher?->listen([
        TransactionBeginning::class,
        TransactionCommitting::class,
        TransactionCommitted::class,
        TransactionRolledBack::class,
    ], function (object $event) use (&$events): void {
        $events[] = $event::class;
    });

    $this->connection->beginTransaction();
    expect($this->connection->transactionLevel())->toBe(1);
    $this->connection->statement('INSERT INTO examples (value) VALUES (?)', ['committed']);
    $this->connection->commit();

    $this->connection->beginTransaction();
    $this->connection->statement('INSERT INTO examples (value) VALUES (?)', ['rolled-back']);
    $this->connection->rollBack();

    expect($this->connection->transactionLevel())->toBe(0)
        ->and($this->connection->table('examples')->pluck('value')->all())->toBe(['committed'])
        ->and($events)->toBe([
            TransactionBeginning::class,
            TransactionCommitting::class,
            TransactionCommitted::class,
            TransactionBeginning::class,
            TransactionRolledBack::class,
        ]);
});

test('outer lifecycle orders pdo cleanup manager and event for ticketless and queued outcomes', function (string $mode, string $outcome): void {
    $workspace = $this->workspace.'/ordered-'.$mode.'-'.$outcome;
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $log = [];
    $record = static function (string $entry) use (&$log): void {
        $log[] = $entry;
    };
    $pdo = new class('sqlite:'.$appPath, $record, $mode === 'queued') extends PDO
    {
        private readonly Closure $record;

        public function __construct(string $dsn, callable $record, private bool $busyOnFirstBegin)
        {
            parent::__construct($dsn, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            $this->record = $record(...);
        }

        public function exec(string $statement): int|false
        {
            if (str_starts_with($statement, 'BEGIN IMMEDIATE')) {
                if ($this->busyOnFirstBegin) {
                    $this->busyOnFirstBegin = false;
                    ($this->record)('pdo-begin-busy');
                    $exception = new PDOException('database is busy');
                    $exception->errorInfo = ['HY000', 5, 'database is busy'];

                    throw $exception;
                }
                ($this->record)('pdo-begin');
            }

            return parent::exec($statement);
        }

        public function commit(): bool
        {
            ($this->record)('pdo-commit');

            return parent::commit();
        }

        public function rollBack(): bool
        {
            ($this->record)('pdo-rollback');

            return parent::rollBack();
        }
    };
    $name = 'ordered-'.$mode.'-'.$outcome.'-'.str_replace('.', '-', uniqid('', true));
    $lockPath = $workspace.'/lock';
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite', 'name' => $name, 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $lockPath, 'stale_head_seconds' => 10.0, 'wait_strategy' => 'polling',
    ], $appPath, $lockPath);
    $connection->setTransactionManager(new class([], $record, $lockPath) extends DatabaseTransactionsManager
    {
        private readonly Closure $record;

        public function __construct(array $connections, callable $record, private readonly string $lockPath)
        {
            parent::__construct($connections);
            $this->record = $record(...);
        }

        public function begin($connection, $level)
        {
            ($this->record)('manager-begin-'.$level.'-tickets-'.$this->ticketCount());

            return parent::begin($connection, $level);
        }

        public function commit($connection, $levelBeingCommitted, $newTransactionLevel)
        {
            ($this->record)('cleanup-observed-'.$this->ticketCount());
            ($this->record)('manager-commit-'.$levelBeingCommitted.'-'.$newTransactionLevel);

            return parent::commit($connection, $levelBeingCommitted, $newTransactionLevel);
        }

        public function rollback($connection, $newTransactionLevel)
        {
            ($this->record)('cleanup-observed-'.$this->ticketCount());
            ($this->record)('manager-rollback-'.$newTransactionLevel);

            return parent::rollback($connection, $newTransactionLevel);
        }

        private function ticketCount(): int
        {
            $observer = new PDO('sqlite:'.$this->lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);

            return (int) $observer->query('SELECT COUNT(*) FROM tickets')->fetchColumn();
        }
    });
    $dispatcher = new Dispatcher();
    $dispatcher->listen(TransactionBeginning::class, static fn () => $record('event-begin'));
    $dispatcher->listen(TransactionCommitting::class, static fn () => $record('event-committing'));
    $dispatcher->listen(TransactionCommitted::class, static fn () => $record('event-committed'));
    $dispatcher->listen(TransactionRolledBack::class, static fn () => $record('event-rollback'));
    $connection->setEventDispatcher($dispatcher);
    $connection->beforeStartingTransaction(static fn () => $record('before-start'));

    $connection->beginTransaction();
    $outcome === 'commit' ? $connection->commit() : $connection->rollBack();

    $begin = $mode === 'queued'
        ? ['before-start', 'pdo-begin-busy', 'pdo-begin', 'manager-begin-1-tickets-1', 'event-begin']
        : ['before-start', 'pdo-begin', 'manager-begin-1-tickets-0', 'event-begin'];
    expect($log)->toBe($outcome === 'commit' ? [...$begin,
        'event-committing', 'pdo-commit', 'cleanup-observed-0', 'manager-commit-1-0', 'event-committed',
    ] : [
        ...$begin, 'pdo-rollback', 'cleanup-observed-0', 'manager-rollback-0', 'event-rollback',
    ]);
})->with([
    'ticketless commit' => ['ticketless', 'commit'],
    'ticketless rollback' => ['ticketless', 'rollback'],
    'queued commit' => ['queued', 'commit'],
    'queued rollback' => ['queued', 'rollback'],
]);

test('callback transactions return results and support nested savepoints', function (): void {
    $result = $this->connection->transaction(function (FairSQLiteConnection $connection): string {
        $connection->statement('INSERT INTO examples (value) VALUES (?)', ['outer']);
        $connection->transaction(function (FairSQLiteConnection $nested): void {
            $nested->statement('INSERT INTO examples (value) VALUES (?)', ['nested']);
        });

        return 'result';
    });

    expect($result)->toBe('result')
        ->and($this->connection->table('examples')->pluck('value')->all())->toBe(['outer', 'nested']);
});

test('manual nested rollback targets preserve or release the outer fair lifecycle exactly', function (): void {
    $this->connection->beginTransaction();
    $this->connection->statement('INSERT INTO examples (value) VALUES (?)', ['outer']);
    $this->connection->beginTransaction();
    $this->connection->statement('INSERT INTO examples (value) VALUES (?)', ['nested']);

    $this->connection->rollBack(-1);
    $this->connection->rollBack(2);
    expect($this->connection->transactionLevel())->toBe(2);

    $this->connection->rollBack();
    expect($this->connection->transactionLevel())->toBe(1);
    $this->connection->commit();
    expect($this->connection->table('examples')->pluck('value')->all())->toBe(['outer']);

    $this->connection->beginTransaction();
    $this->connection->beginTransaction();
    $this->connection->statement('INSERT INTO examples (value) VALUES (?)', ['discarded']);
    $this->connection->rollBack(0);
    expect($this->connection->transactionLevel())->toBe(0)
        ->and($this->connection->table('examples')->pluck('value')->all())->toBe(['outer']);
});

test('callback signature and return phpdoc remain compatible with laravel', function (): void {
    $method = new ReflectionMethod(FairSQLiteConnection::class, 'transaction');

    expect($method->getParameters())->toHaveCount(2)
        ->and($method->getParameters()[0]->getType()?->__toString())->toBe(Closure::class)
        ->and($method->getParameters()[1]->getDefaultValue())->toBe(1)
        ->and($method->getDocComment())->toContain('@template TReturn', '@return TReturn');
});

test('callback concurrency retries only after a completed outer rollback', function (): void {
    $attempts = 0;
    $result = $this->connection->transaction(function (FairSQLiteConnection $connection) use (&$attempts): string {
        $attempts++;
        if ($attempts === 1) {
            throw new PDOException('database is locked');
        }
        $connection->statement('INSERT INTO examples (value) VALUES (?)', ['retried']);

        return 'result';
    }, 2);

    expect($result)->toBe('result')
        ->and($attempts)->toBe(2)
        ->and($this->connection->table('examples')->value('value'))->toBe('retried');
});

test('nested callback concurrency becomes a deadlock exception without callback retry', function (): void {
    $attempts = 0;
    $this->connection->beginTransaction();
    try {
        $this->connection->transaction(function () use (&$attempts): never {
            $attempts++;
            throw new PDOException('database is locked');
        }, 2);
        $this->fail('Nested concurrency should not retry.');
    } catch (DeadlockException) {
    }

    expect($attempts)->toBe(1)
        ->and($this->connection->transactionLevel())->toBe(1);
    $this->connection->rollBack();
});

test('wait expiry before acquisition invokes the callback zero times', function (): void {
    $holder = new PDO('sqlite:'.$this->databasePath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $holder->exec('PRAGMA busy_timeout=0');
    $holder->exec('BEGIN IMMEDIATE');
    $calls = 0;

    try {
        $this->connection->withWaitTimeout(0.01, function (FairSQLiteConnection $connection) use (&$calls): void {
            $connection->transaction(function () use (&$calls): void {
                $calls++;
            });
        });
        $this->fail('The writer wait should time out.');
    } catch (FairWaitTimeoutException) {
    } finally {
        $holder->rollBack();
    }

    expect($calls)->toBe(0);
});

test('normal writes are fair while reads remain ticket free', function (): void {
    expect($this->connection->affectingStatement('INSERT INTO examples (value) VALUES (?)', ['statement']))->toBe(1)
        ->and($this->connection->unprepared("INSERT INTO examples (value) VALUES ('unprepared')"))->toBeTrue()
        ->and($this->connection->select('SELECT value FROM examples ORDER BY id'))->toHaveCount(2);
});

test('leading transaction control sql is rejected before state changes', function (string $sql): void {
    $this->connection->unprepared($sql);
})->with([
    'begin' => ' BEGIN IMMEDIATE',
    'commit' => "\ncommit",
    'rollback' => "\tRoLlBaCk",
])->throws(LogicException::class);

test('pretend writes and transactions only populate the query log', function (): void {
    $lock = new PDO('sqlite:'.$this->lockDirectory.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $ticketsBefore = (int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn();
    $pdoTransactionBefore = $this->connection->getPdo()->inTransaction();
    $queries = $this->connection->pretend(function (FairSQLiteConnection $connection): void {
        $connection->transaction(function (FairSQLiteConnection $transaction): void {
            $transaction->statement('INSERT INTO examples (value) VALUES (?)', ['pretend']);
        });
    });

    expect($queries)->toHaveCount(1)
        ->and($queries[0]['query'])->toContain('INSERT INTO examples')
        ->and($this->connection->table('examples')->count())->toBe(0)
        ->and($this->connection->transactionLevel())->toBe(0)
        ->and((int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn())->toBe($ticketsBefore)
        ->and($this->connection->getPdo()->inTransaction())->toBe($pdoTransactionBefore);
});

test('eloquent and builder writes share queued implicit fairness while reads stay ticket free', function (): void {
    Exceptions::fake();
    $workspace = $this->workspace.'/eloquent-builder';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $lockPath = $workspace.'/lock';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath, $lockPath) extends PDO
    {
        public int $beginAttempts = 0;

        /** @var list<int> */
        public array $ticketsAtCommit = [];

        private bool $busyOnFirstBegin = true;

        public function __construct(string $dsn, private readonly string $lockPath)
        {
            parent::__construct($dsn, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        }

        public function exec(string $statement): int|false
        {
            if (str_starts_with($statement, 'BEGIN IMMEDIATE')) {
                $this->beginAttempts++;
                if ($this->busyOnFirstBegin) {
                    $this->busyOnFirstBegin = false;
                    $exception = new PDOException('database is busy');
                    $exception->errorInfo = ['HY000', 5, 'database is busy'];

                    throw $exception;
                }
            }

            return parent::exec($statement);
        }

        public function commit(): bool
        {
            $lock = new PDO('sqlite:'.$this->lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
            $this->ticketsAtCommit[] = (int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn();

            return parent::commit();
        }
    };
    $installCleanupFailure = null;
    $pdo->sqliteCreateFunction('install_cleanup_failure', static function () use (&$installCleanupFailure): int {
        if (! $installCleanupFailure instanceof PDO) {
            throw new RuntimeException('The lock observer was not installed.');
        }
        $installCleanupFailure->exec(
            "CREATE TRIGGER fail_implicit_cleanup BEFORE DELETE ON tickets BEGIN SELECT RAISE(FAIL, 'implicit cleanup denied'); END",
        );

        return 1;
    });
    $pdo->exec(
        'CREATE TRIGGER install_implicit_cleanup_failure AFTER INSERT ON writes '
        ."WHEN NEW.value = 'cleanup-failure' BEGIN SELECT install_cleanup_failure(); END",
    );
    $name = 'eloquent-builder-'.str_replace('.', '-', uniqid('', true));
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite', 'name' => $name, 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $lockPath, 'stale_head_seconds' => 0.001, 'wait_strategy' => 'polling',
    ], $appPath, $lockPath);
    config()->set('database.connections.'.$name, [
        'driver' => $name,
        'database' => $appPath,
    ]);
    app('db')->extend($name, static fn (): FairSQLiteConnection => $connection);
    $model = new class extends Model
    {
        public $timestamps = false;

        protected $table = 'writes';

        protected $guarded = [];
    };
    $model->setConnection($name);

    $created = $model->newQuery()->create(['value' => 'eloquent']);
    expect($created->getAttribute('value'))->toBe('eloquent')
        ->and($pdo->beginAttempts)->toBe(2)
        ->and($pdo->ticketsAtCommit)->toBe([1]);

    expect($connection->table('writes')->insert(['value' => 'builder']))->toBeTrue()
        ->and($pdo->ticketsAtCommit)->toBe([1, 0]);
    $lock = new PDO('sqlite:'.$lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $ticketsBeforeRead = (int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn();
    expect($connection->table('writes')->orderBy('id')->pluck('value')->all())->toBe(['eloquent', 'builder'])
        ->and((int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn())->toBe($ticketsBeforeRead)
        ->and($connection->getPdo()->inTransaction())->toBeFalse();

    $lock->exec('INSERT INTO tickets DEFAULT VALUES');
    $installCleanupFailure = $lock;
    $result = $connection->table('writes')->insert(['value' => 'cleanup-failure']);

    expect($result)->toBeTrue()
        ->and($connection->table('writes')->where('value', 'cleanup-failure')->count())->toBe(1)
        ->and($connection->transactionLevel())->toBe(0)
        ->and($connection->getPdo()->inTransaction())->toBeFalse()
        ->and((int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn())->toBe(1)
        ->and($pdo->ticketsAtCommit)->toBe([1, 0, 1]);
    Exceptions::assertReported(PDOException::class);
});

test('run nontransactional requires exactly one write and returns its callback result', function (): void {
    $result = $this->connection->runNonTransactional(function (FairSQLiteConnection $connection): string {
        $connection->unprepared("INSERT INTO examples (value) VALUES ('nontransactional')");

        return 'kept';
    });

    expect($result)->toBe('kept')
        ->and($this->connection->table('examples')->value('value'))->toBe('nontransactional');
});

test('run nontransactional returns persisted success after cleanup failure and restores its local scope', function (): void {
    Exceptions::fake();
    $workspace = $this->workspace.'/nontransactional-cleanup';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $lockPath = $workspace.'/lock';
    $pdo = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $pdo->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $pdo->sqliteCreateFunction('install_nontransactional_cleanup_failure', static function () use ($lockPath): int {
        $lock = new PDO('sqlite:'.$lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $lock->exec(
            "CREATE TRIGGER fail_nontransactional_cleanup BEFORE DELETE ON tickets BEGIN SELECT RAISE(FAIL, 'nontransactional cleanup denied'); END",
        );

        return 1;
    });
    $pdo->exec(
        'CREATE TRIGGER install_nontransactional_cleanup_failure AFTER INSERT ON writes '
        ."WHEN NEW.value = 'nontransactional-cleanup' BEGIN SELECT install_nontransactional_cleanup_failure(); END",
    );
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite', 'name' => 'nontransactional-cleanup-'.str_replace('.', '-', uniqid('', true)),
        'database' => $appPath, 'prefix' => '', 'lock_directory' => $lockPath,
        'stale_head_seconds' => 0.001, 'wait_strategy' => 'polling',
    ], $appPath, $lockPath);

    $result = $connection->runNonTransactional(function (FairSQLiteConnection $active): string {
        $active->statement('INSERT INTO writes (value) VALUES (?)', ['nontransactional-cleanup']);

        return 'kept-result';
    });
    $lock = new PDO('sqlite:'.$lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    expect($result)->toBe('kept-result')
        ->and($connection->table('writes')->where('value', 'nontransactional-cleanup')->count())->toBe(1)
        ->and($connection->transactionLevel())->toBe(0)
        ->and($connection->getPdo()->inTransaction())->toBeFalse()
        ->and((int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn())->toBe(1);
    Exceptions::assertReported(PDOException::class);

    $lock->exec('DROP TRIGGER fail_nontransactional_cleanup');
    $connection->beginTransaction();
    $connection->rollBack();
    expect($connection->statement('INSERT INTO writes (value) VALUES (?)', ['after-cleanup-failure']))->toBeTrue()
        ->and($connection->table('writes')->orderBy('rowid')->pluck('value')->all())
        ->toBe(['nontransactional-cleanup', 'after-cleanup-failure'])
        ->and((int) $lock->query('SELECT COUNT(*) FROM tickets')->fetchColumn())->toBe(0);
});

test('run nontransactional rejects zero writes recursion transactions and second writes', function (string $branch): void {
    $this->connection->runNonTransactional(function (FairSQLiteConnection $connection) use ($branch): void {
        match ($branch) {
            'zero' => null,
            'recursive' => $connection->runNonTransactional(static fn (): null => null),
            'transaction' => $connection->beginTransaction(),
            'second' => (function () use ($connection): void {
                $connection->statement('INSERT INTO examples (value) VALUES (?)', ['first']);
                $connection->statement('INSERT INTO examples (value) VALUES (?)', ['second']);
            })(),
        };
    });
})->with(['zero', 'recursive', 'transaction', 'second'])->throws(LogicException::class);

test('wait timeout scopes preserve results and permit one top level fair operation', function (): void {
    $result = $this->connection->withWaitTimeout(1.0, function (FairSQLiteConnection $connection): string {
        $connection->statement('INSERT INTO examples (value) VALUES (?)', ['one']);

        return 'result';
    });

    expect($result)->toBe('result');

    $this->connection->withWaitTimeout(1.0, function (FairSQLiteConnection $connection): void {
        $connection->statement('INSERT INTO examples (value) VALUES (?)', ['two']);
        $connection->statement('INSERT INTO examples (value) VALUES (?)', ['three']);
    });
})->throws(LogicException::class);

test('wait timeout scopes reject successful callbacks without a fair operation and restore state', function (): void {
    try {
        $this->connection->withWaitTimeout(1.0, static fn (): string => 'unused');
        $this->fail('A successful wait-timeout scope without a fair operation should fail.');
    } catch (LogicException $exception) {
        expect($exception->getMessage())->toBe('A wait-timeout scope permits exactly one top-level fair operation.');
    }

    $result = $this->connection->withWaitTimeout(1.0, function (FairSQLiteConnection $connection): string {
        $connection->statement('INSERT INTO examples (value) VALUES (?)', ['after-zero-call']);

        return 'restored';
    });

    expect($result)->toBe('restored');
});

test('nested wait timeout uses the earliest absolute deadline and restores scope state', function (): void {
    $workspace = $this->workspace.'/controlled-nested-deadline';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $lockPath = $workspace.'/lock';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath) extends PDO
    {
        public int $businessSqlAttempts = 0;

        public function exec(string $statement): int|false
        {
            if (str_starts_with($statement, 'INSERT INTO writes')) {
                $this->businessSqlAttempts++;
            }

            return parent::exec($statement);
        }
    };
    $expiringScope = false;
    $scopeClockCalls = 0;
    $clockValues = [];
    $monotonic = static function () use (&$expiringScope, &$scopeClockCalls, &$clockValues): float {
        if (! $expiringScope) {
            return 10.0;
        }
        $scopeClockCalls++;
        $value = $scopeClockCalls >= 8 ? 10.25 : 10.0;
        $clockValues[] = $value;

        return $value;
    };
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite', 'name' => 'controlled-deadline-'.str_replace('.', '-', uniqid('', true)),
        'database' => $appPath, 'prefix' => '', 'lock_directory' => $lockPath,
        'stale_head_seconds' => 10.0, 'wait_strategy' => 'polling',
    ], $appPath, $lockPath, $monotonic);
    $connection->unprepared("INSERT INTO writes (value) VALUES ('bootstrap')");
    $pdo->businessSqlAttempts = 0;

    $holder = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $holder->exec('PRAGMA busy_timeout=0');
    $holder->exec('BEGIN IMMEDIATE');
    $expiringScope = true;
    $timeout = null;
    try {
        $connection->withWaitTimeout(2.0, function (FairSQLiteConnection $outer): void {
            $outer->withWaitTimeout(0.25, function (FairSQLiteConnection $inner): void {
                $inner->unprepared("INSERT INTO writes (value) VALUES ('timed-out')");
            });
        });
        $this->fail('The inner earlier deadline should expire.');
    } catch (FairWaitTimeoutException $exception) {
        $timeout = $exception;
    } finally {
        $holder->rollBack();
    }

    expect($timeout)->toBeInstanceOf(FairWaitTimeoutException::class)
        ->and($timeout?->getMessage())->toBe('The SQLite fair lock deadline expired.')
        ->and($clockValues)->toBe([10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.25])
        ->and(10.25)->toBeLessThan(12.0)
        ->and($pdo->businessSqlAttempts)->toBe(0)
        ->and($connection->transactionLevel())->toBe(0)
        ->and($connection->getPdo()->inTransaction())->toBeFalse()
        ->and($connection->table('writes')->where('value', 'timed-out')->count())->toBe(0);

    $expiringScope = false;
    $result = $connection->withWaitTimeout(1.0, function (FairSQLiteConnection $restored): string {
        $restored->unprepared("INSERT INTO writes (value) VALUES ('after-timeout')");

        return 'restored';
    });
    expect($result)->toBe('restored')
        ->and($pdo->businessSqlAttempts)->toBe(1)
        ->and($connection->table('writes')->where('value', 'after-timeout')->count())->toBe(1);
});

test('wait timeout rejects non finite and non positive values', function (float $seconds): void {
    $this->connection->withWaitTimeout($seconds, static fn (): null => null);
})->with([0.0, -1.0, INF, NAN])->throws(LogicException::class);

test('callback and pre business rollback unknown keep the original error and unfinished framework state', function (string $mode): void {
    Exceptions::fake();
    $workspace = $this->workspace.'/unknown-'.$mode;
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $lockPath = $workspace.'/lock';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath) extends PDO
    {
        public function rollBack(): bool
        {
            throw new PDOException('rollback outcome unknown');
        }
    };
    $weak = WeakReference::create($pdo);
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite',
        'name' => 'unknown-'.$mode.'-'.str_replace('.', '-', uniqid('', true)),
        'database' => $appPath,
        'prefix' => '',
        'lock_directory' => $lockPath,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
    ], $appPath, $lockPath);
    $pdo = null;
    $events = [];
    $dispatcher = new Dispatcher();
    $dispatcher->listen([
        TransactionBeginning::class,
        TransactionCommitted::class,
        TransactionRolledBack::class,
    ], function (object $event) use (&$events): void {
        $events[] = $event::class;
    });
    $connection->setEventDispatcher($dispatcher);
    $callbackRan = false;

    if ($mode === 'callback') {
        $manager = new DatabaseTransactionsManager([]);
        $connection->setTransactionManager($manager);
        try {
            $connection->transaction(function (FairSQLiteConnection $active) use (&$callbackRan): never {
                $active->afterCommit(function () use (&$callbackRan): void {
                    $callbackRan = true;
                });
                throw new RuntimeException('business primary');
            });
            $this->fail('The callback transaction should have failed.');
        } catch (RuntimeException $exception) {
            expect($exception->getMessage())->toBe('business primary');
        }
        expect($connection->transactionLevel())->toBe(1)
            ->and($events)->toBe([TransactionBeginning::class]);
    } else {
        $connection->setTransactionManager(new class([]) extends DatabaseTransactionsManager
        {
            public function begin($connection, $level)
            {
                throw new RuntimeException('manager primary');
            }
        });
        try {
            $connection->beginTransaction();
            $this->fail('The transaction manager should have failed.');
        } catch (RuntimeException $exception) {
            expect($exception->getMessage())->toBe('manager primary');
        }
        expect($connection->transactionLevel())->toBe(0)
            ->and($events)->toBe([]);
    }

    gc_collect_cycles();
    expect($connection->hasUnknownPdoOutcome())->toBeTrue()
        ->and($callbackRan)->toBeFalse()
        ->and($weak->get())->toBeNull();
    Exceptions::assertReported(PDOException::class);
})->with(['callback', 'prebusiness']);

test('callback commit unknown occurs after committing and never retries or finalizes framework state', function (): void {
    $workspace = $this->workspace.'/callback-commit-unknown';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath) extends PDO
    {
        public function commit(): bool
        {
            throw new PDOException('commit outcome unknown');
        }
    };
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite', 'name' => 'callback-commit-unknown', 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $workspace.'/lock', 'stale_head_seconds' => 10.0, 'wait_strategy' => 'polling',
    ], $appPath, $workspace.'/lock');
    $events = [];
    $dispatcher = new Dispatcher();
    $dispatcher->listen(TransactionBeginning::class, function () use (&$events): void {
        $events[] = 'begin';
    });
    $dispatcher->listen(TransactionCommitting::class, function () use (&$events): void {
        $events[] = 'committing';
    });
    $dispatcher->listen(TransactionCommitted::class, function () use (&$events): void {
        $events[] = 'committed';
    });
    $connection->setEventDispatcher($dispatcher);
    $attempts = 0;

    try {
        $connection->transaction(function () use (&$attempts): void {
            $attempts++;
        }, 2);
        $this->fail('Commit with unknown outcome should fail.');
    } catch (PDOException $exception) {
        expect($exception->getMessage())->toBe('commit outcome unknown');
    }

    expect($attempts)->toBe(1)
        ->and($connection->transactionLevel())->toBe(1)
        ->and($connection->hasUnknownPdoOutcome())->toBeTrue()
        ->and($events)->toBe(['begin', 'committing']);
});

test('savepoint rollback unknown preserves manager pending callback and event state', function (): void {
    $workspace = $this->workspace.'/savepoint-manager-unknown';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $pdo = new class('sqlite:'.$appPath) extends PDO
    {
        public function exec(string $statement): int|false
        {
            if (str_starts_with($statement, 'ROLLBACK TO SAVEPOINT')) {
                throw new PDOException('savepoint rollback outcome unknown');
            }

            return parent::exec($statement);
        }
    };
    $connection = new FairSQLiteConnection($pdo, $appPath, '', [
        'driver' => 'fair-sqlite', 'name' => 'savepoint-manager-unknown', 'database' => $appPath, 'prefix' => '',
        'lock_directory' => $workspace.'/lock', 'stale_head_seconds' => 10.0, 'wait_strategy' => 'polling',
    ], $appPath, $workspace.'/lock');
    $manager = new class([]) extends DatabaseTransactionsManager
    {
        public int $rollbackCalls = 0;

        public function rollback($connection, $newTransactionLevel)
        {
            $this->rollbackCalls++;

            return parent::rollback($connection, $newTransactionLevel);
        }
    };
    $connection->setTransactionManager($manager);
    $events = [];
    $dispatcher = new Dispatcher();
    $dispatcher->listen(TransactionRolledBack::class, function () use (&$events): void {
        $events[] = 'rollback';
    });
    $connection->setEventDispatcher($dispatcher);
    $pendingCallbackRan = false;
    $connection->beginTransaction();
    $connection->beginTransaction();
    $connection->afterCommit(function () use (&$pendingCallbackRan): void {
        $pendingCallbackRan = true;
    });

    try {
        $connection->rollBack(1);
        $this->fail('Savepoint rollback with unknown outcome should fail.');
    } catch (PDOException $exception) {
        expect($exception->getMessage())->toBe('savepoint rollback outcome unknown');
    }

    expect($connection->transactionLevel())->toBe(2)
        ->and($manager->rollbackCalls)->toBe(0)
        ->and($pendingCallbackRan)->toBeFalse()
        ->and($events)->toBe([]);
});

test('unknown identity blocks purge reconnect connector aliases and partial path collisions before sql', function (): void {
    $workspace = $this->workspace.'/guard-aliases';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    $lockPath = $workspace.'/lock';
    mkdir($lockPath, 0775, true);
    $setup = new PDO('sqlite:'.$appPath, options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $setup->exec('CREATE TABLE writes (value TEXT NOT NULL)');
    $setup = null;
    $pdo = new class('sqlite:'.$appPath) extends PDO
    {
        public function commit(): bool
        {
            throw new PDOException('commit outcome unknown');
        }
    };
    $name = 'guarded-'.str_replace('.', '-', uniqid('', true));
    $config = [
        'driver' => 'fair-sqlite',
        'name' => $name,
        'database' => $appPath,
        'prefix' => '',
        'lock_directory' => $lockPath,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
    ];
    $connection = new FairSQLiteConnection($pdo, $appPath, '', $config, $appPath, $lockPath);
    expect($connection->hasUnknownPdoOutcome())->toBeFalse()
        ->and(is_file($lockPath.'/lock.sqlite'))->toBeFalse();

    $connection->beginTransaction();
    try {
        $connection->commit();
    } catch (PDOException) {
    }
    expect($connection->hasUnknownPdoOutcome())->toBeTrue();

    config()->set('database.connections.'.$name, $config);
    app('db')->extend($name, static fn (): FairSQLiteConnection => $connection);
    expect(app('db')->connection($name))->toBe($connection);
    app('db')->purge($name);
    app('db')->forgetExtension($name);

    expect(fn () => app('db')->connection($name))->toThrow(FairSQLiteException::class)
        ->and(fn () => (new FairSQLiteConnector())->connect($config, $name))->toThrow(FairSQLiteException::class)
        ->and(fn () => FairSQLiteConnection::assertIdentityConfiguration($name, $appPath, $workspace.'/other-lock'))
        ->toThrow(FairSQLiteException::class)
        ->and(fn () => FairSQLiteConnection::assertIdentityConfiguration('other-name', $appPath, $lockPath))
        ->toThrow(FairSQLiteException::class);
});

test('laravel manager reconnect rebuilds fair coordination around the fresh eager pdo', function (): void {
    $workspace = $this->workspace.'/manager-reconnect';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    touch($appPath);
    $name = 'reconnect-'.str_replace('.', '-', uniqid('', true));
    config()->set('database.connections.'.$name, [
        'driver' => 'fair-sqlite',
        'database' => $appPath,
        'prefix' => '',
        'lock_directory' => $workspace.'/lock',
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
    ]);

    $connection = app('db')->connection($name);
    expect($connection)->toBeInstanceOf(FairSQLiteConnection::class);
    $connection->unprepared('CREATE TABLE writes (value TEXT NOT NULL)');
    $oldPdo = $connection->getPdo();
    $weak = WeakReference::create($oldPdo);
    $oldPdo = null;

    $reconnected = app('db')->reconnect($name);
    gc_collect_cycles();
    expect($reconnected)->toBe($connection)
        ->and($weak->get())->toBeNull();

    $connection->statement('INSERT INTO writes (value) VALUES (?)', ['after-reconnect']);
    expect($connection->table('writes')->value('value'))->toBe('after-reconnect');
});

test('queued cleanup failure preserves commit rollback and callback priorities', function (string $outcome): void {
    Exceptions::fake();
    $workspace = $this->workspace.'/cleanup-'.$outcome;
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    touch($appPath);
    $lockPath = $workspace.'/lock';
    $connection = (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $appPath,
        'prefix' => '',
        'lock_directory' => $lockPath,
        'stale_head_seconds' => 0.001,
        'wait_strategy' => 'polling',
    ], 'cleanup-'.$outcome.'-'.str_replace('.', '-', uniqid('', true)));
    $connection->unprepared('CREATE TABLE writes (value TEXT NOT NULL)');
    $connection->runNonTransactional(static function (FairSQLiteConnection $active): void {
        $active->statement('INSERT INTO writes (value) VALUES (?)', ['bootstrap']);
    });
    $lockPdo = new PDO('sqlite:'.$lockPath.'/lock.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
    $lockPdo->exec('INSERT INTO tickets DEFAULT VALUES');

    if (str_starts_with($outcome, 'callback-')) {
        $attempts = 0;
        try {
            $connection->transaction(function () use ($lockPdo, $outcome, &$attempts): never {
                $attempts++;
                $lockPdo->exec('DROP TABLE tickets');
                throw $outcome === 'callback-concurrency'
                    ? new PDOException('database is locked')
                    : new RuntimeException('business primary');
            }, 2);
            $this->fail('The callback should preserve its original failure.');
        } catch (Throwable $exception) {
            expect($exception->getMessage())->toBe(
                $outcome === 'callback-concurrency' ? 'database is locked' : 'business primary',
            );
        }

        expect($attempts)->toBe(1)
            ->and($connection->transactionLevel())->toBe(0)
            ->and($connection->table('writes')->pluck('value')->all())->toBe(['bootstrap']);
        Exceptions::assertReported(PDOException::class);

        return;
    }

    $connection->beginTransaction();
    $connection->statement('INSERT INTO writes (value) VALUES (?)', [$outcome]);
    $lockPdo->exec('DROP TABLE tickets');

    if ($outcome === 'commit') {
        $connection->commit();
        expect($connection->transactionLevel())->toBe(0)
            ->and($connection->table('writes')->pluck('value')->all())->toBe(['bootstrap', 'commit']);
        Exceptions::assertReported(PDOException::class);

        return;
    }

    try {
        $connection->rollBack();
        $this->fail('Rollback should expose its ticket cleanup failure.');
    } catch (PDOException) {
    }
    expect($connection->transactionLevel())->toBe(0)
        ->and($connection->table('writes')->pluck('value')->all())->toBe(['bootstrap']);
})->with(['commit', 'rollback', 'callback-concurrency', 'callback-business']);

test('debug logging stays silent for a normal direct write when disabled', function (): void {
    Log::spy();

    $this->connection->statement('INSERT INTO examples (value) VALUES (?)', ['quiet']);

    Log::shouldNotHaveReceived('debug');
});

test('debug logging reports representative bootstrap and ticket transitions', function (): void {
    $workspace = $this->workspace.'/debug-transitions';
    mkdir($workspace, 0775, true);
    $appPath = $workspace.'/app.sqlite';
    touch($appPath);

    Log::spy();
    $connection = (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $appPath,
        'prefix' => '',
        'lock_directory' => $workspace.'/lock',
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
        'debug' => true,
    ], 'debug-transitions-'.str_replace('.', '-', uniqid('', true)));
    $connection->runNonTransactional(static function (FairSQLiteConnection $active): void {
        $active->unprepared('CREATE TABLE debug_writes (value TEXT NOT NULL)');
    });

    Log::shouldHaveReceived('debug')->withArgs(
        static fn (string $message, array $context): bool => $message === 'Fair SQLite transition.'
            && ($context['event'] ?? null) === 'lock_database_bootstrap',
    );
    Log::shouldHaveReceived('debug')->withArgs(
        static fn (string $message, array $context): bool => $message === 'Fair SQLite transition.'
            && ($context['event'] ?? null) === 'ticket_created'
            && is_int($context['ticket'] ?? null),
    );
});
