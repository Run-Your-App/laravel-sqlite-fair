<?php

declare(strict_types=1);

use Illuminate\Support\Facades\Log;
use RunYourApp\LaravelSqliteFair\Support\FairSQLiteDebug;

// This dependency rule detects direct facade imports and static references; it does not inspect service-locator strings.
arch('runtime diagnostics import the log facade only through the package debug owner')
    ->expect('RunYourApp\LaravelSqliteFair')
    ->not->toUse(Log::class)
    ->ignoring(FairSQLiteDebug::class);
