<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use InvalidArgumentException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;

/**
 * Selects the wait adapter for the configured strategy and current host.
 *
 * `FairSQLiteConnection` uses this factory once per physical PDO lifecycle.
 * `InotifyWaiter` handles Linux filesystem notifications, while `PollingWaiter`
 * serves every other host and explicit polling requests.
 *
 * @internal
 */
final class WaiterFactory
{
    /**
     * Reports the selected wait adapter for one operating-system family.
     *
     * Runtime capability checks consume this read-only matrix. The optional
     * Inotify boolean lets tests describe unavailable Linux support; production calls omit it so
     * Linux checks the real extension while every non-Linux host reports polling.
     *
     * @param  string|null  $osFamily  PHP operating-system family, or null for the active host.
     * @param  bool|null  $inotify  Test override for Linux Inotify availability.
     * @return array{platform: 'linux'|'other', waiter: 'inotify'|'polling', available: bool} Selected host policy and whether its required adapter is available.
     *
     * @internal Package tests and runtime capability checks use this deterministic matrix.
     */
    public static function capabilities(?string $osFamily = null, ?bool $inotify = null): array
    {
        $family = mb_strtolower($osFamily ?? PHP_OS_FAMILY);
        $platform = $family === 'linux' ? 'linux' : 'other';

        return [
            'platform' => $platform,
            'waiter' => $platform === 'linux' ? 'inotify' : 'polling',
            'available' => $platform === 'linux' ? ($inotify ?? function_exists('inotify_init')) : true,
        ];
    }

    /**
     * Creates the concrete wait adapter for a prepared lock directory.
     *
     * Explicit polling is available on every host. Auto requires Inotify on Linux
     * and uses polling elsewhere. Native is a Linux-only request and keeps every
     * Inotify startup or native-mode runtime failure fatal.
     *
     * @param  'auto'|'native'|'polling'|string  $strategy  Requested wait policy.
     * @param  string  $directory  Existing absolute lock directory observed by the Linux Inotify adapter.
     * @param  bool  $debug  Whether abnormal adapter transitions emit structured debug logs.
     * @return Waiter The selected and fully initialized wait adapter.
     *
     * @throws InvalidArgumentException When the strategy is not supported.
     * @throws FairSQLiteException When the host lacks the requested native capability or adapter startup fails.
     */
    public static function make(string $strategy, string $directory, bool $debug = false): Waiter
    {
        if ($strategy === 'polling') {
            return new PollingWaiter();
        }
        if (! in_array($strategy, ['auto', 'native'], true)) {
            throw new InvalidArgumentException("Unknown SQLite fair wait strategy [{$strategy}].");
        }

        $capabilities = self::capabilities();
        if ($capabilities['platform'] === 'other') {
            if ($strategy === 'native') {
                throw new FairSQLiteException('Native waiting is supported only on Linux; select polling or auto.');
            }

            return new PollingWaiter();
        }
        if (! $capabilities['available']) {
            throw new FairSQLiteException('Native Linux waiting is unavailable because the inotify extension is missing.');
        }

        return new InotifyWaiter($directory, $strategy === 'auto', debug: $debug);
    }
}
