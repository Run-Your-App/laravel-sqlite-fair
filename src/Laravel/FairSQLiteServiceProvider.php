<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Laravel;

use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Database\DatabaseManager;
use Illuminate\Database\SQLiteConnection;
use Illuminate\Queue\Events\Looping;
use Illuminate\Queue\Worker;
use Illuminate\Support\ServiceProvider;
use Override;

/**
 * Registers Fair SQLite integration for Laravel applications.
 *
 * Laravel Composer discovery loads this provider. It merges the package defaults
 * and attaches the `fair-sqlite` connection factory to the application database
 * manager. It also retires a queue worker before the worker accepts more work
 * after any established Fair SQLite connection reports an unknown PDO outcome.
 */
final class FairSQLiteServiceProvider extends ServiceProvider
{
    /**
     * Registers the package configuration and `fair-sqlite` connection factory.
     *
     * An already-resolved database manager is extended immediately. Otherwise the factory is attached when Laravel
     * first resolves that manager, so both boot orders create connections through `FairSQLiteConnector`.
     *
     * @return void The provider registration is applied directly to Laravel's container.
     */
    #[Override]
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
     * Publishes configuration and retires poisoned Laravel queue workers.
     *
     * Applications may publish the documented package defaults to `config/sqlite-fair.php` with the
     * `sqlite-fair-config` tag. Before each queued job, the listener inspects only connections that Laravel already
     * established. It stops the current worker when one of those connections has an unknown PDO outcome, without
     * resolving a configured connection or opening a database solely for this check.
     *
     * @return void The publish mapping and queue-loop listener are registered directly with Laravel.
     */
    public function boot(): void
    {
        $this->publishes([
            dirname(__DIR__, 2).'/config/sqlite-fair.php' => $this->app->configPath('sqlite-fair.php'),
        ], 'sqlite-fair-config');

        $this->app->make(Dispatcher::class)->listen(Looping::class, function (): ?bool {
            $databaseManager = $this->app->make(DatabaseManager::class);

            foreach ($databaseManager->getConnections() as $connection) {
                if (! $connection instanceof FairSQLiteConnection || ! $connection->hasUnknownPdoOutcome()) {
                    continue;
                }

                /** @var Worker $worker */
                $worker = $this->app->make('queue.worker');
                $worker->shouldQuit = true;

                return false;
            }

            return null;
        });
    }
}
