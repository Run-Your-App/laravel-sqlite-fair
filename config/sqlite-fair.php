<?php

declare(strict_types=1);

return [
    /*
    |--------------------------------------------------------------------------
    | Lock Directory
    |--------------------------------------------------------------------------
    |
    | This absolute directory stores the private lock.sqlite ticket database
    | and is watched by the native waiter. Each file-backed application
    | database must resolve to one dedicated lock directory.
    |
    */
    'lock_directory' => storage_path('app/private/sqlite-fair'),

    /*
    |--------------------------------------------------------------------------
    | Stale Queue Head Threshold
    |--------------------------------------------------------------------------
    |
    | This positive finite number of seconds controls when an unchanged foreign
    | queue head may be checked under the real application writer fence and
    | recovered. Strings and non-finite numbers are never coerced.
    |
    */
    'stale_head_seconds' => 10.0,

    /*
    |--------------------------------------------------------------------------
    | Writer Wait Strategy
    |--------------------------------------------------------------------------
    |
    | Supported values are "auto", "native", and "polling". Auto selects the
    | host-native waiter where required, native rejects unsupported hosts, and
    | polling explicitly uses bounded polling.
    |
    */
    'wait_strategy' => 'auto',

    /*
    |--------------------------------------------------------------------------
    | Runtime Debug Logging
    |--------------------------------------------------------------------------
    |
    | Enable this boolean only when structured diagnostics for contention and
    | abnormal lock transitions are needed. Normal acquisition, wakeup, ticket
    | consumption, commit, and rollback paths remain silent.
    |
    */
    'debug' => false,
];
