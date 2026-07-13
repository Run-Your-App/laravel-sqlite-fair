<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Laravel;

use Illuminate\Database\DatabaseManager;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Support\ServiceProvider;

/**
 * Registers the Fair SQLite driver with a Laravel application.
 *
 * Laravel Composer discovery loads this provider. It merges the package defaults and attaches the `fair-sqlite`
 * connection factory to the application database manager, including when another provider resolved that manager
 * earlier in the same application boot.
 */
final class FairSQLiteServiceProvider extends ServiceProvider
{
    /**
     * Adds the package configuration and `fair-sqlite` connection factory to Laravel.
     *
     * An already-resolved database manager is extended immediately. Otherwise the factory is attached when Laravel
     * first resolves that manager, so both boot orders create connections through `FairSQLiteConnector`.
     *
     * @return void The provider registration is applied directly to Laravel's container.
     */
    public function register(): void
    {
        $this->mergeConfigFrom(dirname(__DIR__, 2).'/config/sqlite-fair.php', 'sqlite-fair');

        $register = static function (DatabaseManager $databaseManager): void {
            $databaseManager->extend(
                'fair-sqlite',
                fn (array $config, string $name): SQLiteConnection => (new FairSQLiteConnector())->connect($config, $name),
            );
        };

        if ($this->app->resolved('db')) {
            $register($this->app->make('db'));

            return;
        }

        $this->app->resolving('db', $register);
    }

    /**
     * Makes the package configuration available to Laravel applications.
     *
     * Applications may publish the documented package defaults to `config/sqlite-fair.php` with the
     * `sqlite-fair-config` tag and then keep their deployment-specific lock directory and diagnostics setting there.
     *
     * @return void The publish mapping is registered directly with Laravel.
     */
    public function boot(): void
    {
        $this->publishes([
            dirname(__DIR__, 2).'/config/sqlite-fair.php' => $this->app->configPath('sqlite-fair.php'),
        ], 'sqlite-fair-config');
    }
}
