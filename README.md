# Laravel SQLite Fair

[![Tests](https://github.com/Run-Your-App/laravel-sqlite-fair/actions/workflows/tests.yml/badge.svg)](https://github.com/Run-Your-App/laravel-sqlite-fair/actions/workflows/tests.yml)
[![Coverage ≥ 90%](https://github.com/Run-Your-App/laravel-sqlite-fair/actions/workflows/coverage.yml/badge.svg?branch=main)](https://github.com/Run-Your-App/laravel-sqlite-fair/actions/workflows/coverage.yml)

Laravel SQLite Fair is a drop-in SQLite driver for Laravel applications where web requests, queue workers, scheduled commands, and CLI processes all write to the same database. Change the connection driver from `sqlite` to `fair-sqlite`, then keep using Eloquent, the query builder, transactions, queues, and scheduled commands as usual. When writes collide, the package gives participating writers a committed FIFO turn instead of leaving them to race into sporadic `database is locked` or `SQLITE_BUSY` errors.

An uncontended writer acquires SQLite's writer slot without creating a coordination ticket. The dedicated `lock.sqlite` queue starts only after another ticket or a busy writer slot reveals contention. Before business SQL begins, the driver verifies that the writer still owns its turn; abandoned waiting tickets are recoverable so one stopped process cannot block everyone else. No call-site PRAGMAs, retry wrappers, or lock helpers are required.

The package preserves Laravel's transaction behavior while adding fair cross-process coordination. On Linux, `auto` and `native` use Inotify wakeups; `polling` remains available explicitly. Every other host uses bounded polling with `auto`. A process paused longer than `stale_head_seconds` may lose its turn and is therefore outside the starvation-free guarantee.

## Requirements

- PHP 8.4 or later
- Laravel 12.61.1 or later, below Laravel 13
- PDO and PDO SQLite
- SQLite 3
- Linux with `auto` or `native`: `ext-inotify`

Linux requires Inotify when `wait_strategy` is `auto` or `native`; a missing `ext-inotify` extension is a startup error. On every non-Linux host, `auto` selects polling. Explicit `polling` works on every host, while `native` is Linux-only.

Every cooperating process for one file-backed application database must see the same database file and dedicated lock directory with correct SQLite locking, commit, and visibility semantics.

## Installation

Register the GitHub repository with Composer and install the package:

```bash
composer config repositories.laravel-sqlite-fair vcs https://github.com/Run-Your-App/laravel-sqlite-fair
composer require run-your-app/laravel-sqlite-fair:dev-main
```

Laravel discovers the package service provider automatically. Publish the package configuration to change its defaults:

```bash
php artisan vendor:publish --provider="RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteServiceProvider" --tag=sqlite-fair-config
```

The published `config/sqlite-fair.php` applies to every Fair SQLite connection. Values set directly on a connection override those defaults.

## Configuration

Use the `fair-sqlite` driver for a file-backed SQLite connection:

```php
'sqlite' => [
    'driver' => 'fair-sqlite',
    'database' => env('DB_DATABASE', database_path('database.sqlite')),
    'prefix' => '',

    'lock_directory' => storage_path('app/private/sqlite-fair'),
    'stale_head_seconds' => 10.0,
    'wait_strategy' => 'auto',
    'debug' => false,
];
```

| Option | Default | Purpose |
| --- | --- | --- |
| `lock_directory` | `storage_path('app/private/sqlite-fair')` | Complete directory containing `lock.sqlite` for exactly one application database. |
| `stale_head_seconds` | `10.0` | Time an unchanged front ticket may wait before fenced recovery starts. |
| `wait_strategy` | `auto` | Waiting backend: `auto`, `native`, or `polling`. |
| `debug` | `false` | Structured debug logging for contention and abnormal runtime transitions. |

Connection-local values override package defaults. `lock_directory` must be an absolute path; use a different directory for every application database and the same directory for all cooperating processes. Existing database and lock paths are resolved with `realpath()`, so symlink, `.` and `..` aliases identify the same files. Numeric and boolean options are strictly typed and are never coerced from strings.

The application remains responsible for SQLite settings such as `journal_mode`, `synchronous`, `foreign_keys`, `cache_size`, and `busy_timeout`. Fair SQLite temporarily makes its writer-fence attempt nonblocking and restores the connection's previous `busy_timeout`; it does not replace the application's tuning.

Laravel's case-sensitive memory forms—`:memory:` and database strings containing `?mode=memory` or `&mode=memory`—use upstream `SQLiteConnection`. Other database strings are file-backed. Memory connections do not create tickets, open `lock.sqlite`, or expose `FairSQLiteConnection` APIs, even though the configured driver key remains `fair-sqlite`.

## Waiting Strategies

`auto` uses Inotify on Linux and polling on every other host. `native` requires Linux with `ext-inotify`; it fails on other hosts instead of silently changing strategy. `polling` is available everywhere.

Polling starts each writer acquisition with a 100-microsecond delay, doubles the delay after each fully completed interval, and caps it at 100 milliseconds. Every wait is limited by the remaining acquisition deadline and returns to the lock owner for a fresh state check. The delay resets when the next writer acquisition begins.

## Normal Usage

An uncontended writer performs one nonblocking `BEGIN IMMEDIATE` and creates no ticket. After contention is observed, committed tickets are served FIFO without barging. Before business SQL starts, a queued writer confirms its own head ticket while holding SQLite's writer lock; a reclaimed or displaced writer rejoins at the back of the queue.

Use Eloquent, the query builder, and Laravel transaction callbacks normally:

```php
use Illuminate\Support\Facades\DB;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnection;

/** @var FairSQLiteConnection $connection */
$connection = DB::connection('sqlite');

$result = $connection->transaction(
    function (FairSQLiteConnection $connection): int {
        return $connection->table('accounts')
            ->where('id', 42)
            ->increment('login_count');
    },
);
```

The outer transaction acquires once; nested transactions use Laravel savepoints. Implicit Eloquent and query-builder writes, plus `statement()`, `affectingStatement()`, and `unprepared()`, use the same lifecycle. Group related writes in an outer transaction to avoid repeated acquisition. Read methods remain ticket-free and must receive read-only SQL.

Top-level concurrency retries occur only after a successful rollback and successful cleanup of any owned ticket. Once the commit path starts, the callback is never repeated because the commit result may be unknown. Manual `beginTransaction()`, `commit()`, and `rollBack()` are supported, but `transaction()` is recommended because it preserves the original business exception when rollback or ticket cleanup also fails.

Direct PDO or `SQLite3` writes, writes through Laravel read methods, and SQL blocks that manage their own transactions are outside the package guarantee. `statement()`, `affectingStatement()`, and `unprepared()` reject SQL beginning with `BEGIN`, `COMMIT`, or `ROLLBACK`.

## Limiting Queue Wait Time

Without an explicit timeout, admission waits until it succeeds or encounters a permanent error. `withWaitTimeout()` applies a deadline only before business SQL starts:

```php
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteConnection;

try {
    $updated = $connection->withWaitTimeout(
        2.5,
        function (FairSQLiteConnection $connection): int {
            return $connection->transaction(
                fn (FairSQLiteConnection $connection): int => $connection
                    ->table('jobs')
                    ->where('id', 42)
                    ->update(['reserved' => true]),
            );
        },
    );
} catch (FairWaitTimeoutException) {
    // Business SQL did not start. The caller decides whether to retry.
}
```

The callback must start exactly one top-level fair write or outer transaction. The deadline covers acquisition, ticket insertion, waiting, and stale-ticket recovery, but ends before business SQL and never interrupts running work. A timeout rolls back any uncommitted writer fence, attempts to remove only its own ticket, throws `FairWaitTimeoutException`, and never retries the callback. Nested timeout scopes use the earliest active deadline.

`FairWaitTimeoutException` extends `FairSQLiteException`.

## Non-Transactional Maintenance

SQLite commands such as `VACUUM` cannot run inside a transaction. `runNonTransactional()` accepts SQL plus optional bindings and keeps that nontransactional execution always ticketed:

```php
$vacuumed = $connection->runNonTransactional('VACUUM');

$changed = $connection->runNonTransactional(
    'DELETE FROM maintenance_queue WHERE id = ?',
    [$jobId],
);
```

The connection rejects blank SQL and leading `BEGIN`, `COMMIT`, or `ROLLBACK` before ticket acquisition and never exposes a caller callback after persistence. It does not parse, split, or normalize SQL; Laravel and PDO retain their raw-SQL semantics. The execution always uses a ticket and is never retried. Once PDO confirms the autocommit statement, query-listener and ticket-cleanup failures are reported without changing the successful result. Active transactions and reentrant fair writes throw `LogicException`.

## Failure and Crash Semantics

- Uncontended writers create no tickets. Once contention is observed, committed tickets are served FIFO.
- A lock-database commit with an unknown result is never replayed; the unusable handle is discarded.
- Failures before business SQL roll back an active writer fence and make one bounded attempt to remove only the writer's own ticket. Cleanup failures never replace the original exception.
- A crash before application commit lets SQLite roll back the transaction. A crash after business commit but before ticket deletion leaves a recoverable stale ticket; recovery never repeats the business callback.
- An unknown application-PDO outcome disconnects that PDO and blocks the same database identity for the rest of the current process. Before the next queued job, the package stops the current Laravel queue worker when any already-established Fair SQLite connection has entered that state.

## Debug Logging

`debug=false` is silent. Setting the connection option to the boolean `true` emits Laravel `debug` records with the message `Fair SQLite transition.` and a small structured context. Events cover every committed ticket creation and abnormal transitions such as numeric lock retries, error rollbacks, timeouts, first-time lock-database bootstrap, stale-head recovery, ticket requeue, native-waiter degradation, unknown PDO outcomes, and cleanup failures. Normal direct acquisition, queue/head reads, watcher arm/drain/block, wakeups, ticket consumption, successful commits, ordinary application rollbacks, and adapter startup remain silent.

Context is intentionally limited to the event name, process ID, ticket/head IDs, operation, adapter, and fallback. It never contains database or lock paths, SQL, payloads, exception messages, or user data. Logging is best-effort: a failing log handler is suppressed and cannot change lock ownership, retries, cleanup, or transaction outcomes. Because retry diagnostics can occur while fenced stale recovery still holds the application writer fence, operators who enable this opt-in switch should avoid slow synchronous remote handlers.

## Limits

- SQLite still has one writer slot. Coordination does not increase write throughput.
- FIFO covers tickets that are not reclaimed. Repeatedly paused writers do not have a starvation-freedom guarantee.
- Stale recovery applies only to waiting tickets. The driver never interrupts running business SQL, so keep transactions short and enforce request/job time limits.
- Only processes using the same `fair-sqlite` file-backed connection, Laravel write APIs, and dedicated lock directory participate.
- Read methods are caller-declared read-only; the package does not parse SQL to repair a caller bypass.

### Lock Schema

`lock.sqlite` is internal coordination data. The package never clears, replaces, or migrates it automatically. If its schema is incompatible, stop all writers, remove the dedicated lock directory, and restart the application.

## Testing

```bash
composer test
composer analyse
composer review
```

GitHub Actions runs on Linux with real Inotify support and exercises the explicit polling path in the same suite. Polling multi-process tests provide the safety and progress proof. Linux test runs must execute the real Inotify tests; missing native support on those hosts fails verification.

The dependency gates cover Laravel 12.61.1 and the latest supported Laravel 12 release.

## License

Laravel SQLite Fair is open-source software licensed under the MIT license.

Copyright (c) Run Your App.
