<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use Illuminate\Support\Facades\Log;
use RuntimeException;
use Throwable;

/**
 * Uses Linux directory events as bounded wake hints for lock-state checks.
 *
 * WaiterFactory selects this adapter for Linux and WSL. It owns one Inotify
 * stream and directory watch, while FairSQLiteLock remains responsible for every
 * queue and application-fence decision before and after a wakeup.
 *
 * @internal
 */
final class InotifyWaiter implements Waiter
{
    /** @var resource */
    private $handle;

    private int $watch = -1;

    private bool $degraded = false;

    private bool $armedOnce = false;

    private ?PollingWaiter $polling = null;

    /** @var callable */
    private $select;

    /**
     * Opens and arms the Linux directory-event boundary.
     *
     * Auto mode may degrade only after this initial arm succeeds. The optional
     * select callable is an internal deterministic seam for adapter failure tests.
     *
     * @param  string  $directory  Existing absolute lock directory to observe.
     * @param  bool  $allowPostArmPolling  Whether a later native failure may switch this adapter to polling.
     * @param  null|callable(array<int, resource>&, array<int, resource>&, array<int, resource>&, int, int): (int|false)  $select  Internal stream-select seam.
     * @param  bool  $debug  Whether post-arm degradation emits a structured debug log.
     * @return void The adapter owns an armed Inotify stream and directory watch.
     *
     * @throws RuntimeException When Inotify is unavailable or the initial watch cannot be armed.
     */
    public function __construct(private readonly string $directory, private readonly bool $allowPostArmPolling = false, ?callable $select = null, private readonly bool $debug = false)
    {
        if (! function_exists('inotify_init')) {
            throw new RuntimeException('The inotify extension is required for native Linux waiting.');
        }
        $handle = inotify_init();
        stream_set_blocking($handle, false);
        $this->handle = $handle;
        $this->select = $select ?? static fn (array &$read, array &$write, array &$except, int $seconds, int $microseconds): int|false => @stream_select($read, $write, $except, $seconds, $microseconds);
        $this->arm();
    }

    /**
     * Removes the directory watch and closes the owned Inotify stream.
     *
     * @return void The owned watch and stream have been released best-effort.
     */
    public function __destruct()
    {
        if ($this->watch >= 0) {
            @inotify_rm_watch($this->handle, $this->watch);
        }
        @fclose($this->handle);
    }

    /**
     * Registers or refreshes the directory watch before the second state check.
     *
     * The watch covers create, delete, modify, move and completed-write events for
     * SQLite's DELETE-journal lifecycle. A post-arm failure degrades auto mode;
     * startup and native-mode failures remain fatal.
     *
     * @return void The native watch is armed, or the permitted polling fallback is active.
     *
     * @throws RuntimeException When the watch cannot be registered and degradation is not allowed.
     */
    public function arm(): void
    {
        if ($this->degraded) {
            return;
        }
        if ($this->watch >= 0) {
            @inotify_rm_watch($this->handle, $this->watch);
        }
        $watch = @inotify_add_watch($this->handle, $this->directory, IN_CREATE | IN_DELETE | IN_MODIFY | IN_MOVED_FROM | IN_MOVED_TO | IN_CLOSE_WRITE);
        if ($watch === false) {
            if ($this->allowPostArmPolling && $this->armedOnce) {
                $this->degraded = true;
                $this->debugDegradation('arm');

                return;
            }
            throw new RuntimeException('The lock directory inotify watch could not be armed.');
        }
        $this->watch = $watch;
        $this->armedOnce = true;
    }

    /**
     * Consumes every Inotify event buffered before the second state check.
     *
     * The stream is nonblocking, so this method only clears already available
     * hints. Degraded adapters have no native events to consume.
     *
     * @return void Every currently buffered Inotify event has been consumed.
     */
    public function drain(): void
    {
        if (! $this->degraded) {
            while (inotify_read($this->handle) !== false) {
            }
        }
    }

    /**
     * Waits for one Inotify event or the bounded interval to finish.
     *
     * The wait lasts no longer than one tenth of a second or the supplied absolute
     * deadline. Auto mode switches permanently to polling after a post-arm select
     * failure; native mode reports that failure to the lock owner.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for the standard bounded interval.
     * @param  callable(): float  $monotonic  Returns the current monotonic time in seconds.
     * @return void The event, bounded interval, or supplied deadline ended the wait.
     *
     * @throws RuntimeException When stream selection fails and degradation is not allowed.
     */
    public function block(?float $deadline, callable $monotonic): void
    {
        if ($this->degraded) {
            ($this->polling ??= new PollingWaiter())->block($deadline, $monotonic);

            return;
        }
        $seconds = $deadline === null ? 0.1 : max(0.0, min(0.1, $deadline - $monotonic()));
        if ($seconds === 0.0) {
            return;
        }
        $read = [$this->handle];
        $write = [];
        $except = [];
        $select = $this->select;
        if ($select($read, $write, $except, 0, (int) ($seconds * 1_000_000)) === false) {
            if (! $this->allowPostArmPolling) {
                throw new RuntimeException('The inotify wait failed after the watch was armed.');
            }
            $this->degraded = true;
            $this->debugDegradation('block');
            ($this->polling ??= new PollingWaiter())->block($deadline, $monotonic);
        }
    }

    /** Emit the single diagnostic allowed for automatic native degradation. */
    private function debugDegradation(string $operation): void
    {
        if ($this->debug) {
            try {
                Log::debug('Fair SQLite transition.', ['event' => 'waiter_degraded', 'pid' => getmypid(), 'adapter' => 'inotify', 'operation' => $operation, 'fallback' => 'polling']);
            } catch (Throwable) {
                // Optional diagnostics must never change waiter degradation.
            }
        }
    }
}
