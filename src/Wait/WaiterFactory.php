<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use InvalidArgumentException;
use RuntimeException;

/**
 * Selects the wait adapter for the configured strategy and current host.
 *
 * FairSQLiteConnection uses this factory once per physical PDO lifecycle. The
 * factory owns platform policy, while each native adapter owns its capability
 * probe and operating-system boundary.
 *
 * @internal
 */
final class WaiterFactory
{
    /**
     * Reports the native wait capability for one operating-system family.
     *
     * Runtime capability checks consume this read-only matrix. Optional booleans
     * are deterministic test seams; production calls omit them so Linux checks
     * Inotify and Darwin delegates its complete C-ABI probe to KqueueWaiter.
     *
     * @param  string|null  $osFamily  PHP operating-system family, or null for the active host.
     * @param  bool|null  $inotify  Test override for Linux Inotify availability.
     * @param  bool|null  $ffi  Test override for Darwin kqueue availability.
     * @return array{platform: 'linux'|'darwin'|'windows'|'unsupported', native_available: bool}
     *
     * @internal Package tests and runtime capability checks use this deterministic matrix.
     */
    public static function capabilities(?string $osFamily = null, ?bool $inotify = null, ?bool $ffi = null): array
    {
        $family = mb_strtolower($osFamily ?? PHP_OS_FAMILY);
        $platform = match ($family) {
            'linux' => 'linux',
            'darwin' => 'darwin',
            'windows' => 'windows',
            default => 'unsupported',
        };

        return [
            'platform' => $platform,
            'native_available' => match ($platform) {
                'linux' => $inotify ?? function_exists('inotify_init'),
                'darwin' => $ffi ?? KqueueWaiter::isSupported(),
                'windows' => false,
                default => false,
            },
        ];
    }

    /**
     * Creates the concrete wait adapter for a prepared lock directory.
     *
     * Explicit polling always succeeds through PollingWaiter. Auto uses the native
     * Linux or Darwin adapter and allows only post-arm degradation; native keeps
     * every adapter failure fatal. Windows auto intentionally selects polling.
     *
     * @param  'auto'|'native'|'polling'|string  $strategy  Requested wait policy.
     * @param  string  $directory  Existing absolute lock directory observed by native adapters.
     * @return Waiter
     *
     * @throws InvalidArgumentException When the strategy is not supported.
     * @throws RuntimeException When the host lacks the requested native capability or adapter startup fails.
     */
    public static function make(string $strategy, string $directory): Waiter
    {
        if ($strategy === 'polling') {
            return new PollingWaiter();
        }
        if (! in_array($strategy, ['auto', 'native'], true)) {
            throw new InvalidArgumentException("Unknown SQLite fair wait strategy [{$strategy}].");
        }

        $capabilities = self::capabilities();
        if ($capabilities['platform'] === 'windows') {
            if ($strategy === 'native') {
                throw new RuntimeException('Native waiting is not supported on Windows; select polling or auto.');
            }

            return new PollingWaiter();
        }
        if (! $capabilities['native_available']) {
            throw new RuntimeException("Native {$capabilities['platform']} waiting is unavailable.");
        }

        return match ($capabilities['platform']) {
            'linux' => new InotifyWaiter($directory, $strategy === 'auto'),
            'darwin' => new KqueueWaiter($directory, $strategy === 'auto'),
            default => throw new RuntimeException('The current operating system has no supported wait strategy.'),
        };
    }
}
