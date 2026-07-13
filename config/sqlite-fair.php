<?php

declare(strict_types=1);

return [
    /*
    |--------------------------------------------------------------------------
    | Lock Directory
    |--------------------------------------------------------------------------
    |
    | Absolute directory that contains this database's private `lock.sqlite`
    | ticket database. Native waiters observe this directory for wake hints.
    |
    | Give every application database its own directory, and configure the same
    | directory in every cooperating web, queue, scheduler, and CLI process.
    | The directory may be absent at startup; the package creates it lazily.
    |
    */
    'lock_directory' => storage_path('app/private/sqlite-fair'),

    /*
    |--------------------------------------------------------------------------
    | Stale Queue Head Threshold
    |--------------------------------------------------------------------------
    |
    | Positive, finite number of seconds an unchanged foreign queue head must
    | remain visible before another writer may verify it under the application
    | writer fence and recover it. This is not a ticket lifetime or heartbeat.
    |
    | Use an integer or float. Numeric strings, zero, negative values, NAN, and
    | infinity are rejected instead of being coerced.
    |
    */
    'stale_head_seconds' => 10.0,

    /*
    |--------------------------------------------------------------------------
    | Writer Wait Strategy
    |--------------------------------------------------------------------------
    |
    | `auto` selects Inotify on Linux/WSL, FFI-kqueue on macOS, and polling on
    | native Windows. `native` requires the host-native adapter and fails when
    | that capability is unavailable. `polling` always uses bounded polling.
    |
    */
    'wait_strategy' => 'auto',

    /*
    |--------------------------------------------------------------------------
    | Runtime Debug Logging
    |--------------------------------------------------------------------------
    |
    | Set this boolean to true for lean, structured diagnostics when contention
    | creates a ticket or the runtime retries, rolls back, times out, bootstraps,
    | degrades a native waiter, or enters another abnormal transition.
    |
    | Normal direct writes, wakeups, ticket consumption, successful commits,
    | and other expected queue progress remain silent. Logging failures never
    | change lock ownership or application outcomes.
    |
    */
    'debug' => false,
];
