<?php

declare(strict_types=1);

use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;
use RunYourApp\LaravelSqliteFair\Tests\Support\ProcessHarness;
use RunYourApp\LaravelSqliteFair\Wait\PollingWaiter;

it('uses one fixed child workspace and the package autoloader', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        expect($workspace)->toEndWith('/workspaces/sqlite-fair-process')
            ->and($harness->autoloadPath())->toBe(dirname(__DIR__, 2).'/vendor/autoload.php');
    });
});

it('boots concurrent children only through the package autoloader and fixed workspace', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $results = $harness->runChildren([
            ['scenario' => 'boot'],
            ['scenario' => 'boot'],
        ]);

        expect(array_column($results, 'exit_code'))->toBe([0, 0])
            ->and(array_column($results, 'stdout'))->toBe([$workspace, $workspace])
            ->and(array_column($results, 'stderr'))->toBe(['', '']);
    });
});

it('fails a blocked child at the harness deadline with its scenario name', function () {
    $harness = new ProcessHarness;
    $harness->run(function () use ($harness): void {
        expect(fn (): array => $harness->runChildren(
            [['scenario' => 'wait-for-missing-signal']],
            0.05,
        ))->toThrow(RuntimeException::class, 'wait-for-missing-signal');
    });
});

it('wakes a registered process barrier through its persisted sqlite signal', function () {
    $harness = new ProcessHarness;
    $harness->run(function () use ($harness): void {
        $results = $harness->runChildren([
            ['scenario' => 'signal-waiter'],
            ['scenario' => 'signal-sender'],
        ]);

        expect(array_column($results, 'exit_code'))->toBe([0, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and($results[0]['stdout'])->toBe('notified')
            ->and($results[1]['stdout'])->toBe('sent');
    });
});

it('waits before a ticket mutation while another process holds a lock database read transaction', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $database = new LockDatabase(
            $workspace.'/locks',
            new PollingWaiter,
            static fn (): float => hrtime(true) / 1e9,
        );
        expect($database->admit())->toBe(1);

        $results = $harness->runChildren([
            ['scenario' => 'lock-reader'],
            ['scenario' => 'ticket-mutator'],
        ]);

        expect(array_column($results, 'exit_code'))->toBe([0, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and($database->readHead())->toBeNull();
    });
});

it('keeps concurrent writers mutually exclusive and makes progress without notification events', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE mutual_writes (label TEXT NOT NULL UNIQUE)');
        $results = $harness->runChildren([
            ['scenario' => 'mutual-writer', 'arguments' => ['role' => 'holder', 'label' => 'one']],
            ['scenario' => 'mutual-writer', 'arguments' => ['role' => 'contender', 'label' => 'two']],
        ]);

        $failureOutput = array_map(
            static fn (array $result): string => mb_substr($result['stderr'], 0, 2000),
            array_values(array_filter($results, static fn (array $result): bool => $result['exit_code'] !== 0)),
        );
        expect(array_column($results, 'exit_code'))->toBe([0, 0], implode(PHP_EOL, $failureOutput));
        $coordination = new PDO('sqlite:'.$workspace.'/coordination.sqlite');
        $events = $coordination->query('SELECT event FROM events ORDER BY id')->fetchAll(PDO::FETCH_COLUMN);
        expect($events)->toHaveCount(4)
            ->and(str_starts_with($events[0], 'enter:'))->toBeTrue()
            ->and(str_starts_with($events[1], 'exit:'))->toBeTrue()
            ->and(str_starts_with($events[2], 'enter:'))->toBeTrue()
            ->and(str_starts_with($events[3], 'exit:'))->toBeTrue();
        $labels = $app->query('SELECT label FROM mutual_writes ORDER BY label')->fetchAll(PDO::FETCH_COLUMN);
        $database = new LockDatabase($workspace.'/locks', new PollingWaiter(), static fn (): float => 0.0);
        $coordination = new PDO('sqlite:'.$workspace.'/coordination.sqlite');
        expect($labels)->toBe(['one', 'two'])
            ->and($coordination->query("SELECT value FROM signals WHERE name = 'mutual-contender-queued'")->fetchColumn())->toBe('1')
            ->and($database->readHead())->toBeNull();
    });
});

it('recovers a stale committed head under an app fence and preserves progress', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $database = new LockDatabase($workspace.'/locks', new PollingWaiter, static fn (): float => 0.0);
        $stale = $database->admit();
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE writes (value TEXT NOT NULL)');
        $result = $harness->runChildren([['scenario' => 'stale-recovery']])[0];

        expect($result['exit_code'])->toBe(0, mb_substr($result['stderr'], 0, 2000))
            ->and($database->readHead())->toBeNull()
            ->and($app->query('SELECT value FROM writes')->fetchColumn())->toBe('recovered')
            ->and($stale)->toBe(1);
    });
});

it('lets two concurrent followers recover one stale head and each commit once', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $database = new LockDatabase($workspace.'/locks', new PollingWaiter, static fn (): float => 0.0);
        expect($database->admit())->toBe(1);
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE writes (value TEXT NOT NULL UNIQUE)');
        $results = $harness->runChildren([
            ['scenario' => 'stale-recovery', 'arguments' => ['label' => 'recoverer-one', 'peer' => 'recoverer-two']],
            ['scenario' => 'stale-recovery', 'arguments' => ['label' => 'recoverer-two', 'peer' => 'recoverer-one']],
        ]);

        $coordination = new PDO('sqlite:'.$workspace.'/coordination.sqlite');
        expect(array_column($results, 'exit_code'))->toBe([0, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and($database->readHead())->toBeNull()
            ->and($coordination->query("SELECT COUNT(*) FROM signals WHERE name IN ('stale-observed-recoverer-one', 'stale-observed-recoverer-two')")->fetchColumn())->toBe(2)
            ->and($app->query('SELECT value FROM writes ORDER BY value')->fetchAll(PDO::FETCH_COLUMN))
            ->toBe(['recoverer-one', 'recoverer-two']);
    });
});

it('rolls back a crashed pre-commit writer and lets a concurrent follower commit', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $database = new LockDatabase($workspace.'/locks', new PollingWaiter, static fn (): float => 0.0);
        expect($database->admit())->toBe(1);
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE writes (value TEXT NOT NULL)');
        $results = $harness->runChildren([
            ['scenario' => 'pre-commit-crash', 'arguments' => ['role' => 'crasher']],
            ['scenario' => 'pre-commit-crash', 'arguments' => ['role' => 'follower']],
        ]);

        expect(array_column($results, 'exit_code'))->toBe([23, 0], mb_substr($results[1]['stderr'], 0, 2000))
            ->and($app->query('SELECT value FROM writes')->fetchAll(PDO::FETCH_COLUMN))->toBe(['follower'])
            ->and($database->readHead())->toBeNull();
    });
});

it('requeues a reclaimed owner with a higher committed ticket', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $database = new LockDatabase($workspace.'/locks', new PollingWaiter, static fn (): float => 0.0);
        expect($database->admit())->toBe(1);
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE reclaimed_writes (value TEXT NOT NULL UNIQUE)');
        $results = $harness->runChildren([
            ['scenario' => 'reclaimed-ticket', 'arguments' => ['role' => 'acquirer']],
            ['scenario' => 'reclaimed-ticket', 'arguments' => ['role' => 'reclaimer']],
        ]);
        $coordination = new PDO('sqlite:'.$workspace.'/coordination.sqlite');

        expect(array_column($results, 'exit_code'))->toBe([0, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and($coordination->query("SELECT value FROM signals WHERE name = 'reclaimed'")->fetchColumn())->toBe('2')
            ->and($results[0]['stdout'])->toBe('3')
            ->and($app->query('SELECT value FROM reclaimed_writes')->fetchColumn())->toBe('acquired')
            ->and($database->readHead())->toBeNull();
    });
});

it('preserves a committed write when its owner crashes before ticket cleanup and recovers progress', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $database = new LockDatabase($workspace.'/locks', new PollingWaiter, static fn (): float => 0.0);
        $database->admit();
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE writes (value TEXT NOT NULL)');
        $results = $harness->runChildren([
            ['scenario' => 'committed-crash', 'arguments' => ['role' => 'writer']],
            ['scenario' => 'committed-crash', 'arguments' => ['role' => 'follower']],
        ]);

        expect(array_column($results, 'exit_code'))->toBe([24, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and($app->query('SELECT value FROM writes ORDER BY rowid')->fetchAll(PDO::FETCH_COLUMN))
            ->toBe(['committed-before-crash', 'follower-after-stale'])
            ->and($database->readHead())->toBeNull();
    });
});

it('acquires three committed non-reclaimed tickets in fifo order', function () {
    $harness = new ProcessHarness;
    $harness->run(function (string $workspace) use ($harness): void {
        $app = new PDO('sqlite:'.$workspace.'/app.sqlite', options: [PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION]);
        $app->exec('CREATE TABLE fifo (label TEXT NOT NULL, ticket INTEGER NOT NULL)');
        $results = $harness->runChildren([
            ['scenario' => 'fifo', 'arguments' => ['role' => 'holder']],
            ['scenario' => 'fifo', 'arguments' => ['role' => 'writer', 'label' => 'one', 'required_tickets' => 0]],
            ['scenario' => 'fifo', 'arguments' => ['role' => 'writer', 'label' => 'two', 'required_tickets' => 1]],
            ['scenario' => 'fifo', 'arguments' => ['role' => 'writer', 'label' => 'three', 'required_tickets' => 2]],
        ]);

        expect(array_column($results, 'exit_code'))->toBe([0, 0, 0, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and($app->query("SELECT label || ':' || ticket FROM fifo ORDER BY rowid")->fetchAll(PDO::FETCH_COLUMN))
            ->toBe(['one:1', 'two:2', 'three:3']);
    });
});

it('retires an unknown app commit identity and releases the app pdo after stack unwind', function () {
    $harness = new ProcessHarness;
    $harness->run(function () use ($harness): void {
        $result = $harness->runChildren([['scenario' => 'unknown-commit']])[0];

        expect($result['exit_code'])->toBe(0, $result['stderr'])
            ->and($result['stdout'])->toBe('retired-and-released')
            ->and($result['stderr'])->toBe('');
    });
});

it('isolates every unknown rollback outcome without finalizing laravel state', function () {
    $harness = new ProcessHarness;
    $harness->run(function () use ($harness): void {
        $modes = ['full', 'savepoint', 'nontransactional'];
        $results = $harness->runChildren(array_map(
            static fn (string $mode): array => ['scenario' => 'unknown-rollback', 'arguments' => ['mode' => $mode]],
            $modes,
        ));

        expect(array_column($results, 'exit_code'))->toBe([0, 0, 0], implode(PHP_EOL, array_column($results, 'stderr')))
            ->and(array_column($results, 'stdout'))->toBe($modes);
    });
});

it('preserves the original pre-business error when unknown rollback cleanup also fails once', function () {
    $harness = new ProcessHarness;
    $harness->run(function () use ($harness): void {
        $result = $harness->runChildren([['scenario' => 'cleanup-failure']])[0];

        expect($result['exit_code'])->toBe(0, $result['stderr'])
            ->and($result['stdout'])->toBe('original-priority-and-one-cleanup-report')
            ->and($result['stderr'])->toBe('');
    });
});
