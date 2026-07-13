<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Laravel;

use Illuminate\Database\Connectors\SQLiteConnector;
use Illuminate\Database\SQLiteConnection;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Lock\LockDatabase;

/**
 * Build validated Laravel SQLite connections.
 *
 * Memory databases stay on Laravel's upstream lifecycle. File-backed databases
 * receive validated fair configuration and deterministic application/lock path
 * identities before the application PDO is opened.
 */
final class FairSQLiteConnector
{
    /**
     * Create the configured Laravel connection.
     *
     * @param  array<string, mixed>  $config  Laravel connection values plus optional fair overrides.
     * @param  string  $name  Stable Laravel connection name used by the process-local identity guard.
     * @return SQLiteConnection The upstream memory connection or the file-backed fair connection.
     *
     * @throws FairSQLiteException When configuration, paths, or identity are invalid.
     */
    public function connect(array $config, string $name): SQLiteConnection
    {
        $database = $config['database'] ?? null;
        if (! is_string($database)) {
            throw new FairSQLiteException('The SQLite database path must be a string.');
        }

        $config['name'] = $name;
        $config['prefix'] ??= '';
        if (! is_string($config['prefix'])) {
            throw new FairSQLiteException('The SQLite table prefix must be a string.');
        }
        $prefix = $config['prefix'];

        // Laravel memory DSNs deliberately bypass every fair-only capability and configuration check.
        if ($this->isMemoryDatabase($database)) {
            $pdo = (new SQLiteConnector())->connect($config);

            return new SQLiteConnection($pdo, $database, $prefix, $config);
        }

        $fairConfig = $this->fairConfig($config);
        $appPath = $this->canonicalExistingPath($database, 'database');
        $lockPath = $this->canonicalLockDirectory($fairConfig['lock_directory']);

        FairSQLiteConnection::assertIdentityConfiguration($name, $appPath, $lockPath);
        FairSQLiteConnection::assertIdentityIsUsable($name, $appPath, $lockPath);

        $config = array_replace($config, $fairConfig, [
            'database' => $appPath,
            'name' => $name,
        ]);
        $pdo = (new SQLiteConnector())->connect($config);

        return new FairSQLiteConnection($pdo, $appPath, $prefix, $config, $appPath, $lockPath);
    }

    private function isMemoryDatabase(string $database): bool
    {
        return $database === ':memory:'
            || str_contains($database, '?mode=memory')
            || str_contains($database, '&mode=memory');
    }

    /**
     * Merge and validate fair-only configuration without coercion.
     *
     * @param  array<string, mixed>  $connectionConfig  Connection values that may override package defaults.
     * @return array{lock_directory: string, stale_head_seconds: float|int, wait_strategy: 'auto'|'native'|'polling', debug: bool} Validated fair runtime values.
     *
     * @throws FairSQLiteException When defaults are missing or any value has an invalid type or range.
     */
    private function fairConfig(array $connectionConfig): array
    {
        $defaults = config('sqlite-fair');
        if (! is_array($defaults)) {
            throw new FairSQLiteException('The SQLite fair package configuration is missing.');
        }

        $values = array_replace($defaults, array_intersect_key($connectionConfig, array_flip([
            'lock_directory',
            'stale_head_seconds',
            'wait_strategy',
            'debug',
        ])));
        $lockDirectory = $values['lock_directory'] ?? null;
        $staleHeadSeconds = $values['stale_head_seconds'] ?? null;
        $waitStrategy = $values['wait_strategy'] ?? null;
        $debug = $values['debug'] ?? null;

        if (! is_string($lockDirectory) || $lockDirectory === '') {
            throw new FairSQLiteException('The SQLite fair lock directory must be a non-empty absolute path.');
        }
        if ((! is_int($staleHeadSeconds) && ! is_float($staleHeadSeconds))
            || ! is_finite((float) $staleHeadSeconds)
            || $staleHeadSeconds <= 0) {
            throw new FairSQLiteException('The SQLite fair stale-head seconds must be a finite positive number.');
        }
        if (! is_string($waitStrategy) || ! in_array($waitStrategy, ['auto', 'native', 'polling'], true)) {
            throw new FairSQLiteException('The SQLite fair wait strategy must be auto, native, or polling.');
        }
        if (! is_bool($debug)) {
            throw new FairSQLiteException('The SQLite fair debug option must be a boolean.');
        }

        return [
            'lock_directory' => $lockDirectory,
            'stale_head_seconds' => $staleHeadSeconds,
            'wait_strategy' => $waitStrategy,
            'debug' => $debug,
        ];
    }

    private function canonicalExistingPath(string $path, string $label): string
    {
        $this->assertAbsolutePath($path, $label);
        $canonical = realpath($path);
        if ($canonical === false) {
            throw new FairSQLiteException("The SQLite {$label} path [{$path}] does not exist.");
        }

        return $canonical;
    }

    /** Resolve the single lock-directory filesystem boundary. */
    private function canonicalLockDirectory(string $path): string
    {
        $this->assertAbsolutePath($path, 'lock directory');
        LockDatabase::prepareDirectory($path);
        $canonical = realpath($path);
        if ($canonical === false) {
            throw new FairSQLiteException("The SQLite lock directory path [{$path}] could not be resolved.");
        }

        return $canonical;
    }

    private function assertAbsolutePath(string $path, string $label): void
    {
        if (! self::isAbsolutePathForPlatform($path, PHP_OS_FAMILY)) {
            throw new FairSQLiteException("The SQLite {$label} path must be absolute.");
        }
    }

    /**
     * Decide whether a path is absolute for an operating-system family.
     *
     * Production and tests share this side-effect-free decision so POSIX, WSL,
     * Windows drive, and Windows UNC semantics cannot drift.
     *
     * @param  string  $path  Candidate path without normalization.
     * @param  string  $osFamily  PHP OS family such as Linux, Darwin, or Windows.
     * @return bool True only when the original path is absolute on that platform.
     *
     * @internal
     */
    public static function isAbsolutePathForPlatform(string $path, string $osFamily): bool
    {
        if ($path === '') {
            return false;
        }

        return mb_strtolower($osFamily) === 'windows'
            ? preg_match('/^(?:[A-Za-z]:[\\\\\/]|\\\\\\\\[^\\\\\/]+[\\\\\/][^\\\\\/]+|\/\/[^\/]+\/[^\/]+)/', $path) === 1
            : str_starts_with($path, '/');
    }
}
