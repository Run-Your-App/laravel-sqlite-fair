<?php

declare(strict_types=1);

use Illuminate\Database\SQLiteConnection;
use Orchestra\Testbench\TestCase;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnection;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnector;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteServiceProvider;

uses(TestCase::class);

beforeEach(function (): void {
    $this->workspace = $GLOBALS['sqliteFairTestRunDirectory'].'/connector-'.str_replace('.', '-', uniqid('', true));
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
});

test('manually isolated provider runtime registers the fair sqlite driver', function (): void {
    $this->app->register(FairSQLiteServiceProvider::class);
    config()->set('database.default', 'package');
    config()->set('database.connections.package', [
        'driver' => 'fair-sqlite',
        'database' => $this->databasePath,
        'prefix' => '',
        'lock_directory' => $this->lockDirectory,
        'wait_strategy' => 'polling',
        'stale_head_seconds' => 10.0,
        'debug' => false,
    ]);

    $connection = app('db')->connection('package');

    expect($connection)
        ->toBeInstanceOf(FairSQLiteConnection::class)
        ->and($connection->getName())->toBe('package')
        ->and($connection->getConfig('driver'))->toBe('fair-sqlite');
});

test('provider publishes exactly the package configuration with its dedicated tag', function (): void {
    $this->app->register(FairSQLiteServiceProvider::class);

    expect(FairSQLiteServiceProvider::pathsToPublish(FairSQLiteServiceProvider::class, 'sqlite-fair-config'))
        ->toBe([
            dirname(__DIR__, 2).'/config/sqlite-fair.php' => config_path('sqlite-fair.php'),
        ]);
});

test('platform path decisions cover posix wsl windows drives and unc without coercion', function (string $platform, string $path, bool $expected): void {
    expect(FairSQLiteConnector::isAbsolutePathForPlatform($path, $platform))->toBe($expected);
})->with([
    'posix absolute' => ['Darwin', '/var/lib/app.sqlite', true],
    'posix relative' => ['Darwin', 'var/lib/app.sqlite', false],
    'wsl posix absolute' => ['Linux', '/mnt/c/app.sqlite', true],
    'wsl rejects windows drive' => ['Linux', 'C:\\app.sqlite', false],
    'windows backslash drive' => ['Windows', 'C:\\app.sqlite', true],
    'windows slash drive' => ['Windows', 'C:/app.sqlite', true],
    'windows backslash unc' => ['Windows', '\\\\server\\share', true],
    'windows slash unc' => ['Windows', '//server/share', true],
    'windows drive relative' => ['Windows', 'C:app.sqlite', false],
    'windows relative' => ['Windows', 'app.sqlite', false],
    'windows empty' => ['Windows', '', false],
]);

test('memory databases bypass fair validation and retain the fair driver config', function (string $database): void {
    $connection = (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $database,
        'prefix' => '',
        'lock_directory' => '',
        'stale_head_seconds' => 'invalid',
        'wait_strategy' => 'invalid',
    ], 'memory');

    expect($connection)
        ->toBeInstanceOf(SQLiteConnection::class)
        ->not->toBeInstanceOf(FairSQLiteConnection::class)
        ->and($connection->getConfig('driver'))->toBe('fair-sqlite')
        ->and($connection->getName())->toBe('memory');
})->with([
    'anonymous memory' => ':memory:',
    'query memory' => 'file:shared?mode=memory&cache=shared',
    'ampersand memory' => 'file:shared?cache=shared&mode=memory',
]);

test('memory detection remains case sensitive', function (): void {
    (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => 'file:shared?MODE=memory',
    ], 'case-sensitive');
})->throws(FairSQLiteException::class);

test('file connections merge overrides and preserve upstream pdo options', function (): void {
    $connection = (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $this->databasePath,
        'prefix' => 'p_',
        'lock_directory' => $this->lockDirectory,
        'stale_head_seconds' => 2,
        'wait_strategy' => 'polling',
        'debug' => true,
        'options' => [PDO::ATTR_TIMEOUT => 7],
    ], 'file');

    expect($connection)
        ->toBeInstanceOf(FairSQLiteConnection::class)
        ->and($connection->getName())->toBe('file')
        ->and($connection->getConfig('stale_head_seconds'))->toBe(2)
        ->and($connection->getConfig('debug'))->toBeTrue()
        ->and($connection->getTablePrefix())->toBe('p_')
        ->and($connection->getConfig('options'))->toBe([PDO::ATTR_TIMEOUT => 7]);
});

test('file connections reject invalid fair configuration without coercion', function (string $key, mixed $value): void {
    (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $this->databasePath,
        'lock_directory' => $this->lockDirectory,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
        'debug' => false,
        $key => $value,
    ], 'invalid-'.str_replace('.', '-', uniqid('', true)));
})->with([
    'empty lock path' => ['lock_directory', ''],
    'null lock path' => ['lock_directory', null],
    'relative lock path' => ['lock_directory', 'relative/lock'],
    'string stale seconds' => ['stale_head_seconds', '10'],
    'zero stale seconds' => ['stale_head_seconds', 0],
    'nan stale seconds' => ['stale_head_seconds', NAN],
    'infinite stale seconds' => ['stale_head_seconds', INF],
    'unknown strategy' => ['wait_strategy', 'fallback'],
    'null strategy' => ['wait_strategy', null],
    'string debug' => ['debug', 'false'],
    'integer debug' => ['debug', 0],
    'null debug' => ['debug', null],
])->throws(FairSQLiteException::class);

test('file connections reject relative and drive-relative database paths', function (string $database): void {
    (new FairSQLiteConnector())->connect([
        'driver' => 'fair-sqlite',
        'database' => $database,
        'lock_directory' => $this->lockDirectory,
        'stale_head_seconds' => 10.0,
        'wait_strategy' => 'polling',
        'debug' => false,
    ], 'relative-'.str_replace('.', '-', uniqid('', true)));
})->with([
    'relative' => 'database/app.sqlite',
    'drive relative' => 'C:app.sqlite',
])->throws(FairSQLiteException::class);
