<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use FFI;
use RuntimeException;
use RunYourApp\LaravelSqliteFair\Support\FairSQLiteDebug;
use Throwable;

/**
 * Uses Darwin vnode events as bounded wake hints for lock-state checks.
 *
 * PHP 8.4 and Laravel expose no kqueue, FSEvents, or vnode-watch API. This class
 * therefore owns one deliberately small FFI boundary instead of starting an
 * external watcher process or adding a second runtime. `WaiterFactory` delegates
 * Darwin capability detection here so the C declaration, constants, descriptors,
 * and system calls cannot drift across owners.
 *
 * The adapter observes only the lock directory and treats events as wake hints.
 * `FairSQLiteLock` remains the sole owner of queue and writer-fence decisions.
 *
 * @internal
 */
final class KqueueWaiter implements Waiter
{
    private const C_DEFINITIONS = <<<'C'
        typedef unsigned long uintptr_t;
        typedef long intptr_t;
        typedef unsigned int uint32_t;

        struct kevent {
            uintptr_t ident;
            short filter;
            unsigned short flags;
            uint32_t fflags;
            intptr_t data;
            void *udata;
        };

        struct timespec {
            long tv_sec;
            long tv_nsec;
        };

        int kqueue(void);
        int kevent(int, const struct kevent *, int, struct kevent *, int, const struct timespec *);
        int open(const char *, int, ...);
        int close(int);
        C;

    private const EVFILT_VNODE = -4;

    private const EV_ADD = 0x0001;

    private const EV_ENABLE = 0x0004;

    private const EV_CLEAR = 0x0020;

    private const NOTE_WRITE = 0x00000002;

    private const NOTE_DELETE = 0x00000001;

    private const O_EVTONLY = 0x00008000;

    private FFI $ffi;

    private int $queue = -1;

    private int $directoryHandle = -1;

    private bool $degraded = false;

    private bool $armedOnce = false;

    private ?PollingWaiter $polling = null;

    /** @var callable(FFI, string, mixed ...): int */
    private $systemCall;

    /**
     * Opens and arms the Darwin directory-event boundary.
     *
     * Construction owns the queue and directory descriptors until destruction.
     * Auto mode may degrade only after this initial arm succeeds; native mode keeps
     * every later registration or wait failure fatal.
     *
     * @param  string  $directory  Existing absolute lock directory to observe.
     * @param  bool  $allowPostArmPolling  Whether a later native failure may switch this adapter to polling.
     * @param  null|callable(FFI, string, mixed ...): int  $systemCall  Internal deterministic system-call seam.
     * @param  bool  $debug  Whether post-arm degradation emits a structured debug log.
     * @return void The adapter owns an armed kqueue and directory descriptor.
     *
     * @throws RuntimeException When FFI, kqueue, the directory descriptor, or initial registration is unavailable.
     */
    public function __construct(
        string $directory,
        private readonly bool $allowPostArmPolling = false,
        ?callable $systemCall = null,
        private readonly bool $debug = false,
    ) {
        if (! class_exists(FFI::class)) {
            throw new RuntimeException('FFI is required for native Darwin waiting.');
        }

        $this->ffi = self::createFfi();
        $this->systemCall = $systemCall ?? self::callSystem(...);
        $this->queue = $this->integerCall('kqueue');
        $this->directoryHandle = $this->integerCall('open', $directory, self::O_EVTONLY);
        if ($this->queue < 0 || $this->directoryHandle < 0) {
            $this->close();
            throw new RuntimeException('The lock directory kqueue handles could not be opened.');
        }

        try {
            $this->arm();
            $this->drain();
        } catch (Throwable $exception) {
            $this->close();
            throw $exception;
        }
    }

    /**
     * Closes the directory and kqueue descriptors owned by this adapter.
     *
     * Cleanup is best effort: each descriptor is released independently, cleanup
     * failures are diagnostics only, and destruction never masks an active error.
     *
     * @return void Descriptor ownership has been released by this adapter.
     */
    public function __destruct()
    {
        $this->close();
    }

    /**
     * Checks whether the active process can create and close a Darwin kqueue.
     *
     * WaiterFactory uses this read-only probe for Darwin policy selection. It uses
     * the same C declarations and call validation as live KqueueWaiter instances;
     * every FFI restriction or native failure becomes a false capability result.
     *
     * @return bool True when the process can complete the kqueue probe.
     *
     * @internal Runtime capability checks consume this through WaiterFactory.
     */
    public static function isSupported(): bool
    {
        if (! class_exists(FFI::class)) {
            return false;
        }

        try {
            $ffi = self::createFfi();
            $queue = self::callSystem($ffi, 'kqueue');
            if ($queue < 0) {
                return false;
            }

            return self::callSystem($ffi, 'close', $queue) >= 0;
        } catch (Throwable) {
            return false;
        }
    }

    /**
     * Registers or refreshes the vnode watch before the second lock-state check.
     *
     * Re-registration uses edge-clearing directory notifications. A failure after
     * one successful arm switches auto mode permanently to polling; startup and
     * native-mode failures throw without creating a fallback path.
     *
     * @return void The vnode watch is armed, or the permitted polling fallback is active.
     *
     * @throws RuntimeException When the vnode watch cannot be registered and degradation is not allowed.
     */
    public function arm(): void
    {
        if ($this->degraded) {
            return;
        }
        $change = $this->event(self::EV_ADD | self::EV_ENABLE | self::EV_CLEAR);
        if ($this->integerCall('kevent', $this->queue, FFI::addr($change), 1, null, 0, null) < 0) {
            if ($this->allowPostArmPolling && $this->armedOnce) {
                $this->degraded = true;
                FairSQLiteDebug::log($this->debug, 'waiter_degraded', ['adapter' => 'kqueue', 'operation' => 'arm', 'fallback' => 'polling']);

                return;
            }
            throw new RuntimeException('The lock directory kqueue watch could not be armed.');
        }
        $this->armedOnce = true;
    }

    /**
     * Waits for one vnode event or the bounded interval to finish.
     *
     * The wait lasts no longer than one tenth of a second or the supplied absolute
     * deadline. Auto mode switches permanently to polling after a post-arm native
     * failure and begins polling in the next wait cycle; native mode reports that
     * failure to the lock owner.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for the standard bounded interval.
     * @param  callable(): float  $monotonic  Returns the current monotonic time in seconds.
     * @return void The event, bounded interval, or supplied deadline ended the wait.
     *
     * @throws RuntimeException When the native wait fails and degradation is not allowed.
     */
    public function block(?float $deadline, callable $monotonic): void
    {
        if ($this->degraded) {
            ($this->polling ??= new PollingWaiter)->block($deadline, $monotonic);

            return;
        }
        $seconds = $deadline === null ? 0.1 : max(0.0, min(0.1, $deadline - $monotonic()));
        if ($seconds === 0.0) {
            return;
        }

        $event = $this->ffi->new('struct kevent');
        $timeout = $this->ffi->new('struct timespec');
        $timeout->tv_sec = 0;
        $timeout->tv_nsec = (int) ($seconds * 1_000_000_000);
        if ($this->integerCall('kevent', $this->queue, null, 0, FFI::addr($event), 1, FFI::addr($timeout)) < 0) {
            if ($this->allowPostArmPolling) {
                $this->degraded = true;
                FairSQLiteDebug::log($this->debug, 'waiter_degraded', ['adapter' => 'kqueue', 'operation' => 'block', 'fallback' => 'polling']);

                return;
            }
            throw new RuntimeException('The kqueue wait failed after the watch was armed.');
        }
    }

    /**
     * Consumes every vnode event buffered before the second state check.
     *
     * A zero-valued timespec keeps this operation nonblocking. Degraded adapters
     * have no native queue to consume and therefore return without side effects.
     *
     * @return void Every vnode event currently buffered by kqueue has been consumed.
     *
     * @throws RuntimeException When the native event queue cannot be read.
     */
    public function drain(): void
    {
        if ($this->degraded) {
            return;
        }
        $event = $this->ffi->new('struct kevent');
        $timeout = $this->ffi->new('struct timespec');
        $timeout->tv_sec = 0;
        $timeout->tv_nsec = 0;
        while (true) {
            $result = $this->integerCall('kevent', $this->queue, null, 0, FFI::addr($event), 1, FFI::addr($timeout));
            if ($result > 0) {
                continue;
            }
            if ($result === 0) {
                return;
            }
            if ($this->allowPostArmPolling) {
                $this->degraded = true;
                FairSQLiteDebug::log($this->debug, 'waiter_degraded', ['adapter' => 'kqueue', 'operation' => 'drain', 'fallback' => 'polling']);

                return;
            }

            throw new RuntimeException('The kqueue event queue could not be drained.');
        }
    }

    /**
     * Creates the sole Darwin C-ABI binding used by capability and runtime paths.
     *
     *
     * @throws RuntimeException When the process cannot load the declared libc symbols or C data structures.
     */
    private static function createFfi(): FFI
    {
        try {
            return FFI::cdef(self::C_DEFINITIONS);
        } catch (Throwable $exception) {
            throw new RuntimeException('The Darwin kqueue C ABI could not be loaded.', 0, $exception);
        }
    }

    /**
     * Calls one declared Darwin function and requires its integer result.
     *
     * @param  mixed  ...$arguments  Native arguments matching the selected declaration.
     *
     * @throws RuntimeException When the function is unavailable or returns an unexpected PHP type.
     */
    private static function callSystem(FFI $ffi, string $function, mixed ...$arguments): int
    {
        $callback = [$ffi, $function];
        if (! is_callable($callback)) {
            throw new RuntimeException("The kqueue function [{$function}] is not callable.");
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) {
            throw new RuntimeException("The kqueue function [{$function}] returned an invalid result.");
        }

        return $result;
    }

    private function event(int $flags): FFI\CData
    {
        $event = $this->ffi->new('struct kevent');
        $event->ident = $this->directoryHandle;
        $event->filter = self::EVFILT_VNODE;
        $event->flags = $flags;
        $event->fflags = self::NOTE_WRITE | self::NOTE_DELETE;

        return $event;
    }

    private function close(): void
    {
        $this->closeDescriptor($this->directoryHandle, 'close_directory');
        $this->closeDescriptor($this->queue, 'close_queue');
    }

    /** Releases one descriptor without allowing cleanup to mask another failure. */
    private function closeDescriptor(int &$descriptor, string $operation): void
    {
        $ownedDescriptor = $descriptor;
        $descriptor = -1;
        if ($ownedDescriptor < 0) {
            return;
        }

        try {
            if ($this->integerCall('close', $ownedDescriptor) < 0) {
                FairSQLiteDebug::log($this->debug, 'cleanup_failed', ['adapter' => 'kqueue', 'operation' => $operation]);
            }
        } catch (Throwable) {
            FairSQLiteDebug::log($this->debug, 'cleanup_failed', ['adapter' => 'kqueue', 'operation' => $operation]);
        }
    }

    private function integerCall(string $function, mixed ...$arguments): int
    {
        $systemCall = $this->systemCall;

        return $systemCall($this->ffi, $function, ...$arguments);
    }
}
