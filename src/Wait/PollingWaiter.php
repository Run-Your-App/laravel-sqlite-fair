<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use RuntimeException;

/**
 * Provides bounded polling between complete lock-state checks.
 *
 * WaiterFactory selects this adapter explicitly and on every non-Linux host.
 * InotifyWaiter also uses it after an allowed post-arm degradation. Each writer
 * acquisition starts at 100 microseconds and backs off to at most 100 milliseconds.
 *
 * @internal
 */
final class PollingWaiter implements Waiter
{
    private const int INITIAL_MICROSECONDS = 100;

    private const int MAXIMUM_MICROSECONDS = 100_000;

    private int $intervalMicroseconds = self::INITIAL_MICROSECONDS;

    /** @var callable(int, int): (array{seconds: int, nanoseconds: int}|bool) */
    private $sleep;

    /**
     * Creates the polling adapter and its sleep boundary.
     *
     * Production uses time_nanosleep(). Package tests may inject the same callable
     * shape to observe requested intervals without waiting on wall-clock time.
     *
     * @param  null|callable(int, int): (array{seconds: int, nanoseconds: int}|bool)  $sleep  Internal deterministic sleep seam.
     * @return void The adapter is ready at the initial polling interval.
     */
    public function __construct(?callable $sleep = null)
    {
        $this->sleep = $sleep ?? static fn (int $seconds, int $nanoseconds): array|bool => time_nanosleep($seconds, $nanoseconds);
    }

    /**
     * Starts polling for a new writer acquisition.
     *
     * FairSQLiteLock calls this once per acquisition so a prior contended writer
     * cannot leave the next writer at a long polling interval.
     *
     * @return void The next complete wait starts at 100 microseconds.
     */
    public function beginContention(): void
    {
        $this->intervalMicroseconds = self::INITIAL_MICROSECONDS;
    }

    /**
     * Leaves the polling interval prepared for the second state check.
     *
     * Polling has no external event source to register, so this method intentionally
     * keeps the current backoff interval unchanged.
     *
     * @return void The current polling interval remains ready for the state check.
     */
    public function arm(): void {}

    /**
     * Leaves the polling adapter unchanged because it buffers no wake events.
     *
     * @return void No buffered event state exists to consume.
     */
    public function drain(): void {}

    /**
     * Sleeps for the current bounded polling interval.
     *
     * A complete interval doubles the next wait up to 100 milliseconds. Deadline-
     * shortened sleeps, expired deadlines, signal interruptions and failures leave
     * the interval unchanged so only real completed backoff steps advance it.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for the standard bounded interval.
     * @param  callable(): float  $monotonic  Returns the current monotonic time in seconds.
     * @return void The interval ended or the caller must immediately recheck lock state.
     *
     * @throws RuntimeException When time_nanosleep() reports a terminal failure.
     */
    public function block(?float $deadline, callable $monotonic): void
    {
        $sleepMicroseconds = $this->intervalMicroseconds;
        if ($deadline !== null) {
            $remainingMicroseconds = (int) floor(($deadline - $monotonic()) * 1_000_000);
            if ($remainingMicroseconds < 1) {
                return;
            }
            $sleepMicroseconds = min($sleepMicroseconds, $remainingMicroseconds);
        }

        $sleep = $this->sleep;
        $result = $sleep(0, $sleepMicroseconds * 1_000);
        if ($result === false) {
            throw new RuntimeException('The polling wait interval could not be completed.');
        }
        if (is_array($result) || $sleepMicroseconds !== $this->intervalMicroseconds) {
            return;
        }

        $this->intervalMicroseconds = min($this->intervalMicroseconds * 2, self::MAXIMUM_MICROSECONDS);
    }
}
