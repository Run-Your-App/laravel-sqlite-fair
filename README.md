# Laravel SQLite Fair

Laravel SQLite Fair lets an uncontended writer acquire SQLite's single application writer slot directly. When contention is observed, a second SQLite database, `lock.sqlite`, coordinates participating writers through committed FIFO tickets. SQLite remains this application's strategic production database; fair contention handling, native wakeups, and Laravel transaction parity are long-term production architecture. Abandoned waiting tickets can be recovered so one stopped waiter cannot block everyone else.

Before a writer executes SQL, the driver acquires the application database's write lock and verifies that the writer still owns its turn. Web requests, queue workers, scheduled commands, and CLI processes can therefore share one SQLite database without racing into sporadic `database is locked` or `SQLITE_BUSY` errors. A process paused longer than `stale_head_seconds` may lose its turn and is therefore outside the starvation-free guarantee.

The package is a drop-in replacement for Laravel's SQLite driver: install it, change the connection driver from `sqlite` to `fair-sqlite`, and keep using the same Eloquent models, query builder calls, transactions, queues, and scheduled commands. Uncontended writes create no tickets: the driver reads the queue, acquires SQLite's writer slot once with nonblocking `BEGIN IMMEDIATE`, and reads the queue again while holding that fence. Only observed write contention starts ticketing. No call-site PRAGMAs, retry wrappers, or lock helpers are required.

## Requirements

- PHP 8.4 or later
- Laravel 12.61.1 or later, below Laravel 13
- PDO and PDO SQLite
- SQLite 3
- Linux, including WSL: `ext-inotify`
- macOS: usable PHP FFI with kqueue

Linux, including WSL, and macOS are first-class platforms. Missing `ext-inotify` on Linux/WSL or missing FFI/kqueue on macOS is a runtime requirement failure rather than a reason to skip verification. Only native Windows uses polling and is supported on a best-effort basis.

Every cooperating process for one file-backed application database must see the same database file and dedicated lock directory with correct SQLite locking, commit, and visibility semantics.

## Installation

The package is maintained in its own Git repository. Applications that keep a local checkout at `packages/laravel-sqlite-fair` can register it as a Composer path repository:

```json
{
    "repositories": [
        {
            "type": "path",
            "url": "packages/laravel-sqlite-fair",
            "options": {
                "symlink": false
            }
        }
    ]
}
```

Then install the development package version from the application root:

```bash
composer require run-your-app/laravel-sqlite-fair:dev-main
```

Laravel then discovers the package service provider automatically. Publish the package configuration when the application needs deployment-specific values:

```bash
php artisan vendor:publish --provider="RunYourApp\LaravelSqliteFair\Laravel\FairSQLiteServiceProvider" --tag=sqlite-fair-config
```

The published `config/sqlite-fair.php` becomes the application's single owner for package defaults. Connection-local values remain available only when one named database connection intentionally needs to override those application defaults.

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

    'foreign_key_constraints' => env('DB_FOREIGN_KEYS', true),
    'busy_timeout' => env(
        'DB_SQLITE_BUSY_TIMEOUT',
        env('APP_ENV') === 'production' ? 10_000 : 1_000,
    ),
];
```

| Option | Default | Purpose |
| --- | --- | --- |
| `lock_directory` | `storage_path('app/private/sqlite-fair')` | Complete directory containing `lock.sqlite` for exactly one application database. |
| `stale_head_seconds` | `10.0` | Time an unchanged front ticket may wait before fenced recovery starts. |
| `wait_strategy` | `auto` | Waiting backend: `auto`, `native`, or `polling`. |
| `debug` | `false` | Structured debug logging for contention and abnormal runtime transitions. |

Connection-local values override package defaults. `lock_directory` must be a non-empty absolute path, but the directory does not need to exist yet. The lock owner creates it, then the connector applies `realpath()` to the existing application database and lock directory. This resolves symlinks plus `.` and `..` segments so filesystem aliases cannot bypass the process-local safety identity. The canonical application-database path replaces the configured value in the effective Laravel connection configuration and is passed to PDO; the canonical lock-directory path is used by the lock owner. The connector does not derive a lock directory or walk parent directories to synthesize one. `stale_head_seconds` must be a finite positive integer or float, `wait_strategy` must be exactly `auto`, `native`, or `polling`, and `debug` must be a real boolean. No value is coerced from strings or integers. Use a different `lock_directory` for every application database, and use the same directory for all cooperating writers of that database.

This package does not own application-database tuning PRAGMAs. The application remains responsible for settings such as `journal_mode`, `synchronous`, `foreign_keys`, cache sizing, ICU functions, and its normal `busy_timeout`. The Fair driver preserves Laravel's connection configuration and PDO setup instead of installing a second tuning owner.

Immediately around each application `BEGIN IMMEDIATE`, the driver reads the currently active `busy_timeout`, sets it to `0`, performs exactly one nonblocking attempt, and restores the exact value it read on every exit. It does not impose or validate an application tuning value. A read, zeroing, or restore failure aborts before business SQL; an unknown fence rollback outcome activates the process-local Unknown-PDO-outcome guard. The driver does not run `PRAGMA optimize`.

Laravel's case-sensitive memory forms—`:memory:` and database strings containing `?mode=memory` or `&mode=memory`—use upstream `SQLiteConnection`. Other database strings are file-backed. Memory connections do not create tickets, open `lock.sqlite`, or expose `FairSQLiteConnection` APIs, even though the configured driver key remains `fair-sqlite`.

## Waiting Strategies

`polling` always polls and periodically checks the complete queue and application fence state.

`auto` uses Inotify on Linux, including WSL, FFI-kqueue on macOS, and polling only on native Windows. A missing or unusable native capability while selecting or starting the adapter is an error, including under `auto`. Only an adapter failure after it has successfully started and armed may degrade `auto` to polling. `native` requires Inotify on Linux/WSL or kqueue on macOS and rejects native Windows.

The macOS adapter uses a deliberately small FFI binding because PHP 8.4 has no built-in kqueue, FSEvents, or vnode-watch API, and the installed framework packages expose no equivalent filesystem watcher. PECL EvStat periodically calls `stat()` and cannot meet this driver's arm/drain/condition-recheck/100-ms contract; PECL Event exposes readiness for an existing descriptor but does not open a directory with `O_EVTONLY` or register `EVFILT_VNODE`. External watcher processes would add a second runtime. `KqueueWaiter` therefore owns the sole C declaration, constants, capability probe, and system calls; `WaiterFactory` owns only platform and strategy policy.

Notifications only reduce wake-up latency. Polling and periodic queue-condition plus application-fence loops provide progress when events are missing, delayed, coalesced, or lost.

The native adapters watch the configured `lock_directory`, including SQLite DELETE-journal create, write, and delete activity. Each wait step checks its exact queue condition, arms or rearms the directory watch, drains already buffered events, checks the same condition again, and then blocks for at most 0.1 seconds. The outer acquisition loop retries the applicable application fence after that bounded block. This ordering prevents an event between a queue check and watch arming from becoming a lost wake-up without claiming a second fence attempt before the block.

## Normal Usage

Uncontended writes are the normal lifecycle. The driver first reads the ticket head. If the queue is empty, it performs exactly one nonblocking `BEGIN IMMEDIATE`, then reads the head again while holding that writer fence. A second empty read is the linearization point: the write proceeds with no ticket. In normal uncontended operation, `lock.sqlite` is only read and never mutated.

Ticketing begins only when the first read finds an existing queue, SQLite reports writer contention on that single begin attempt, or a committed ticket appears between the two reads. In the last case the driver rolls back the fence and joins behind the visible ticket. A ticket committed before the second read wins; a ticket committed after that read follows the already-linearized writer. Once contention is observed, the writer never retries direct acquisition and committed tickets are served FIFO without barging.

In queued mode, a writer behind a foreign head only waits and rechecks; it does not attempt the application writer fence during normal waiting. Only after the same foreign head reaches the stale threshold may fenced recovery begin. A writer whose own ticket is the head may attempt the application fence, then must reread and confirm that the exact same ticket is still the head while holding that fence. If its ticket was reclaimed, disappeared, or lost head position, it rolls back the fence, commits a new ticket at the back, and waits again. Business SQL starts only after this under-fence exact-own-head check. The original absolute deadline continues through waiting, recovery, and requeue.

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

The outer transaction receives one application database fence and receives a ticket only when contention was observed. Nested transactions use Laravel savepoints. The callback result is returned unchanged. Implicit Eloquent and query-builder writes, plus `statement()`, `affectingStatement()`, and `unprepared()`, use the same lifecycle. Each standalone write is wrapped by explicit `BEGIN IMMEDIATE` and commit, which replaces SQLite's implicit single-statement transaction rather than adding a second business commit. Group related writes in an outer Laravel transaction so acquisition is paid once. Read methods remain ticket-free and must receive read-only SQL.

The runtime override keeps Laravel's public signature `transaction(Closure $callback, $attempts = 1)` without narrowing the attempts parameter or adding a runtime return declaration; its generic PHPDoc preserves the callback and result type. Fair admission and `beginTransaction()` run outside the callback `try`, as in Laravel 12.61. An admission, fence, deadline, or other begin failure therefore escapes immediately and does not invoke or retry the callback.

Callback attempts are repeated only at the top level when Laravel classifies the callback exception as a concurrency failure before the commit path starts. A retry is allowed only after PDO rollback and, when the attempt owned a ticket, exact ticket deletion completed successfully. If either required operation fails, its exception is reported as secondary, the original callback exception remains primary, and no new acquisition, attempt, or callback begins. A failed exact deletion can leave only the writer's own stale ticket for normal recovery; a failed rollback leaves the connection and fence state unsafe for retry. A nested callback uses a savepoint under the existing outer acquisition state; when Laravel classifies a concurrency failure there, the nested level and transaction manager are rolled back and Laravel's `DeadlockException` is thrown without repeating the callback or creating a second savepoint. Once the `committing` event has been emitted or the PDO commit path has started, the callback is never repeated—even when PDO reports a commit-time concurrency error or the commit result is unknown. This is a deliberate safety deviation from Laravel's commit-concurrency retry because repeating the callback could duplicate already committed work.

Manual Laravel transaction lifecycle calls use the same fair outer lifecycle:

```php
$connection->beginTransaction();

try {
    $connection->table('events')
        ->where('id', 42)
        ->update(['title' => 'Summer event']);

    $connection->table('tasks')->insert([
        'event_id' => 42,
        'title' => 'Prepare seating',
    ]);

} catch (Throwable $throwable) {
    try {
        $connection->rollBack();
    } catch (Throwable $rollbackThrowable) {
        report($rollbackThrowable);
    }

    throw $throwable;
}

// Keep commit outside the business-error catch: a commit-time exception can
// represent an unknown outcome and must not trigger an automatic rollback/retry.
$connection->commit();
```

The outer `beginTransaction()` runs Laravel's `beforeStartingTransaction` callbacks, acquires the application writer fence through the lifecycle above, optionally joins the ticket queue when contention was observed, and only then increments Laravel's transaction level, registers the transaction manager, and emits `beganTransaction`. Nested manual transactions use Laravel savepoints and do not acquire another ticket.

The outer `commit()` emits `committing`, commits PDO, deletes its exact ticket only when one exists, decrements the level, commits the transaction manager, and emits `committed`. `rollBack(null)` targets the current level minus one. A target below zero or at or above the current level is a complete no-op. Target level zero performs the full PDO rollback, deletes only a non-null outer ticket, sets the level to zero, rolls the transaction manager back, and emits `rollingBack`. A positive target rolls back to Laravel's matching savepoint, updates manager and event state, and retains the outer acquisition state. Public `rollBack()` follows Laravel and may throw its rollback or cleanup failure; the nested catch in the example reports that secondary failure without masking the original business exception. A PDO exception while rolling back a savepoint has an unknown physical outer-transaction outcome: the current Laravel level and pending transaction-manager callbacks remain untouched, the PDO is disconnected, an own ticket is cleaned once if present, and the process-local guard prevents continuation in that unsafe runtime context. Runtime recycling is automatic as described below. `transaction()` remains the recommended compact API because it owns that error-priority handling itself.

Direct PDO or `SQLite3` writes, writes issued through Laravel read methods, and SQL blocks that manage their own transactions are outside the package guarantee.

For `statement()`, `affectingStatement()`, and `unprepared()`, SQL whose leading token is `BEGIN`, `COMMIT`, or `ROLLBACK` is rejected. The driver does not split SQL or classify read, write, PRAGMA, or embedded transaction-control tokens. Embedded transaction control remains outside the caller contract and is never retried after a possible partial commit.

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

The callback must start exactly one top-level fair write or one outer fair transaction and receives the active `FairSQLiteConnection`. Its result is returned unchanged. Nested timeout scopes share the top-level call counter and use the earliest active deadline.

The deadline starts before the first head read and covers direct fence acquisition plus, only after contention is observed, ticket insertion, queue waiting, and stale-head recovery. It ends before business SQL and never interrupts running SQL. A timeout while holding a ticketless fence rolls back only that fence. A timeout after ticket admission additionally makes one best-effort exact attempt to remove only its own ticket. Both throw `FairWaitTimeoutException` and never retry the callback.

Each application-fence attempt reads the connection's active `PRAGMA busy_timeout`, temporarily sets it to `0`, performs one nonblocking `BEGIN IMMEDIATE`, and restores the exact value read in `finally` on success, lock contention, and permanent failure. During direct acquisition, numeric SQLite `BUSY` or `LOCKED` starts ticket admission immediately and the direct attempt is never retried. Once queued, the waiter rechecks the exact queue condition around arm and drain, blocks for at most 0.1 seconds, and then lets the acquisition loop retry the applicable fence under the same absolute deadline. Read or zeroing failures stop before the begin attempt. If restoration fails after a fence began, the driver rolls that fence back. After a known rollback it retries the exact idempotent restore outside the transaction; if this second restore also fails, the unsafe application PDO is disconnected before the original restore failure escapes. An unsuccessful rollback instead activates the process-local Unknown-PDO-outcome guard. The package never continues on a PDO whose busy timeout could not be restored.

`FairWaitTimeoutException` extends `FairSQLiteException`.

## Non-Transactional Maintenance

SQLite commands such as `VACUUM` cannot run inside a transaction. `runNonTransactional()` provides an always-ticketed scope for exactly one such write:

```php
$vacuumed = $connection->runNonTransactional(
    fn (FairSQLiteConnection $connection): bool => $connection->unprepared('VACUUM'),
);
```

The callback receives the active `FairSQLiteConnection` and its result is returned unchanged. It must execute exactly one `statement()`, `affectingStatement()`, or `unprepared()` call. An active transaction, recursion, a transaction start inside the callback, zero writes, or a second write attempt throws `LogicException`.

This narrow maintenance API always uses a ticket because nontransactional SQL cannot hold an application transaction fence across the queue revalidation-to-command boundary. If the first command succeeded before a second write was attempted, that first command remains committed. The callback is never retried. SQLite owns physical exclusion for the command. A stale-ticket recovery race before SQLite acquires its own lock can weaken queue order for this command, but does not bypass SQLite's physical writer serialization.

## Failure and Crash Semantics

- An empty queue plus a successful direct fence and a second empty head read produces no ticket. If a ticket exists before the first read, appears between the reads, or SQLite's writer slot is busy, the writer joins the queue exactly once. With two or three simultaneous writers, at most one can hold a ticketless fence; all others enter committed FIFO order.

- A newly opened lock-database handle first sets `busy_timeout=0`, then sets and validates `journal_mode=DELETE` and `synchronous=NORMAL` outside any transaction. `configurePersistentPragmas()` runs once for that newly opened PDO handle, not for ticket creation, reads, consumption, or cleanup. Only handle invalidation followed by a later `open()` runs it again. Only after that idempotent setup may schema bootstrap execute `BEGIN EXCLUSIVE`; bootstrap commits before the final schema and PRAGMA validation. A lock-database commit exception has an unknown outcome, invalidates the handle, and is never replayed or reopened in the same operation.
- Lock-database bootstrap, ticket admission, and normal exact deletion are separate mutation units. Numeric `BUSY` or `LOCKED` while acquiring their lock-database transaction may wait and retry. After that transaction is active, a pre-commit statement failure may repeat the whole unit only when it is numeric `BUSY` or `LOCKED`, rollback succeeded, and commit has not begun. A non-lock error or rollback failure aborts immediately.
- Fenced stale-head deletion is its own mutation unit. After the application fence revalidates the observed foreign head, it may delete only that exact foreign ticket while keeping the application fence held through a known delete commit. After a known successful delete, the recovering writer retains its own committed ticket and queue position, rolls back the application fence, and returns to the queued head check with the same deadline; it does not delete or replace its own ticket on this success path. A terminal delete failure or unknown delete-commit outcome is never replayed. Only that terminal abort rolls back or disconnects the fence and attempts own-ticket cleanup once. No business SQL runs during recovery.
- Any lock-database commit exception has an unknown outcome and is never replayed. The package discards the unusable lock-database connection. Admission exposes a ticket ID only after a known successful commit, so it never inserts a second ticket after a possible commit. Bootstrap and exact deletion likewise never repeat after commit may have happened.
- A read-only head lookup may retry only that statement for numeric `BUSY` or `LOCKED`. Abort/timeout ticket deletion is different: it performs one nonblocking exact attempt and never enters a normal operation retry loop.
- Before business SQL starts, every queue-read, recovery, fence, or revalidation failure follows one abort path: an active application fence is rolled back first, then the package makes a bounded best-effort exact deletion only when it owns a non-null ticket. Cleanup failure is reported and never replaces the original failure.
- Stale age is process-local monotonic observation state. Rechecking the same head, arming a watcher, draining events, or waiting does not restart its timer; the timer resets only when the head changes, the queue becomes empty, or recovery of that head has finished. No observation time is stored in `lock.sqlite`.
- A crash before application commit leaves SQLite to roll back the uncommitted transaction and release its writer lock.
- A crash of a ticketless writer before commit leaves SQLite to roll back and creates no stale-recovery work. A queued writer that crashes after application commit but before ticket deletion leaves committed business data plus a stale ticket. Recovery removes the stale ticket; it does not repeat the business callback.
- A paused writer whose ticket was reclaimed must fail its head revalidation and obtain a new ticket before running SQL.
- A business or SQL exception remains the primary exception. Ticket-cleanup failure does not replace it.
- After a successful rollback, ticket-cleanup failure is reported, but Laravel's target transaction level, transaction manager rollback, and `rollingBack` event are still finalized. Public `rollBack()` throws the cleanup failure only after that state is final; `transaction()` reports it as secondary and rethrows the original callback exception.
- A PDO exception from outer commit, full rollback, application-fence rollback, or savepoint rollback leaves the physical database outcome unknown. The driver marks the deterministic identity—Laravel connection config name plus canonical application-database and lock-directory paths—in a process-local in-memory guard, disconnects the application PDO, attempts own-ticket cleanup once when a ticket exists, and keeps Laravel's transaction level, transaction-manager record, and pending callbacks unchanged. No DB row, marker file, repair state, or manual check is created. Existing and newly resolved aliases fail before PDO open or SQL in that same unsafe process; purge or reconnect cannot bypass the guard. Normal web requests and one-shot Artisan commands end with their execution context. A Laravel queue `Looping` hook automatically marks the worker for clean exit before another job, and the repository's existing queue wrapper starts a fresh worker after its normal three-second cycle. Scheduled commands run in fresh `schedule:work` child contexts. No signal, `exit()`, cache restart, or operator intervention is required.
- After a successful commit, ticketless acquisition performs no lock-database cleanup. For a queued writer, an already-missing exact ticket is allowed; permanent cleanup failure is reported critically, the local scope is cleared, and committed success is returned without retry. The same queued rule applies after a known successful implicit write or `runNonTransactional()` command.

## Destructive Restore and Wipe Boundary

Online application-database restore and full storage wipe are destructive operator tools outside Laravel SQLite Fair's safety guarantee.

A wipe removes the lock directory only when that directory is inside the wiped storage contents, including the default location. An absolute lock directory configured outside the wiped storage tree remains untouched. A restore that replaces only the application database can leave the existing lock database and its tickets in place; stale tickets are recovered normally. A writer that was already running during restore can still write to the replacement database afterward. The package does not add a drain, barrier, reset state, acknowledgment protocol, or automatic lock reset for these tools.

For a separate operator-controlled replacement, stop every writer outside the application, replace or remove the application database and its dedicated lock directory together, then start the processes again. This is not what `MaintenanceService::restoreManualBackup()` does; that application method remains deliberately destructive and does not stop writers or replace the lock directory.

## Runtime Debug Logging

`debug=false` is silent. Setting the connection option to the boolean `true` emits Laravel `debug` records with the message `Fair SQLite transition.` and a small structured context. Events cover every committed ticket creation and abnormal transitions such as numeric lock retries, error rollbacks, timeouts, first-time lock-database bootstrap, stale-head recovery, ticket requeue, native-waiter degradation, unknown PDO outcomes, and cleanup failures. Normal direct acquisition, queue/head reads, watcher arm/drain/block, wakeups, ticket consumption, successful commits, ordinary application rollbacks, and adapter startup remain silent.

Context is intentionally limited to the event name, process ID, ticket/head IDs, operation, adapter, and fallback. It never contains database or lock paths, SQL, payloads, exception messages, or user data. Logging is best-effort: a failing log handler is suppressed and cannot change lock ownership, retries, cleanup, or transaction outcomes. Because retry diagnostics can occur while fenced stale recovery still holds the application writer fence, operators who enable this opt-in switch should avoid slow synchronous remote handlers.

The package does not publish observability callbacks, custom events, metrics, counters, storage, or repair APIs. Separately, this repository's `DevelopmentServiceProvider` registers once per Fair connection and observes only Laravel's standard outer transaction lifecycle. It reports writer acquisition above 1 ms. Its 50 ms interval ends when its listener receives Laravel's outer committed or rolled-back event. Business SQL, PDO commit or rollback, and optional Fair ticket cleanup are included because the connection performs them before emitting that event; later listener runtime is not included. Accepted maintenance and scenario seeding can suppress these reports through `DevelopmentSafeguards`.

## Limits

- SQLite still has one writer slot. Coordination does not increase write throughput.
- FIFO covers tickets that are not reclaimed. Repeatedly paused writers do not have a starvation-freedom guarantee.
- Stale recovery applies only to waiting tickets. Once a writer holds the application fence, SQLite owns the physical lock; the driver never interrupts running business SQL. Keep transactions short and use the application's request/job process limits; this repository retains development warnings for writer wait above 1 ms and complete outer Fair lifecycle above 50 ms.
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

Polling multi-process tests always provide the safety and progress proof. Linux runs, including WSL, must execute the real Inotify test; macOS runs must execute the real kqueue test; native Windows proves polling. The wrong OS skips only the adapter that cannot exist there, while a missing native capability on Linux/WSL or macOS fails verification.

The dependency gates cover Laravel 12.61.1 and the latest supported Laravel 12 release.

## License

Laravel SQLite Fair is open-source software licensed under the MIT license.

Copyright (c) Run Your App.
