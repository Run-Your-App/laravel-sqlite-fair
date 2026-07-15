<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Support;

use Illuminate\Support\Facades\Log;
use Throwable;

/**
 * Emits the package's optional structured runtime diagnostics.
 *
 * Fair SQLite components call this method for contention and abnormal transitions.
 * Diagnostics are deliberately best effort: a missing Laravel log binding or a
 * failing logger must never alter ticket state, application fences, transaction state,
 * cleanup, or the exception observed by application code.
 *
 * @internal
 */
final class FairSQLiteDebug
{
    /**
     * Writes one secret-free debug event when diagnostics are enabled.
     *
     * @param  bool  $enabled  Whether the connection enabled Fair SQLite diagnostics.
     * @param  string  $event  Stable event identifier for the abnormal transition.
     * @param  array<string, int|string>  $context  Minimal identifiers needed to understand the transition.
     * @return void Logging is attempted at most once and never changes runtime behavior.
     */
    public static function log(bool $enabled, string $event, array $context = []): void
    {
        if (! $enabled) {
            return;
        }

        try {
            Log::debug('Fair SQLite transition.', ['event' => $event, 'pid' => getmypid(), ...$context]);
        } catch (Throwable) {
            // Optional diagnostics must never affect database coordination.
        }
    }
}
