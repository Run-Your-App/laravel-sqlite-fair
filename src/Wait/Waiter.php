<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

/**
 * Coordinates a bounded wake hint between complete lock-state checks.
 *
 * FairSQLiteLock calls this protocol after checking the ticket queue and the
 * application writer fence. Implementations may consume native directory events
 * or a polling interval, but an event never replaces the second state check.
 *
 * @internal
 */
interface Waiter
{
    /**
     * Arms the adapter before the lock owner performs its second state check.
     *
     * Implementations must either prepare their next wake hint or leave an
     * already selected polling strategy ready. A native startup failure is an
     * error unless the adapter has already armed successfully and its auto mode
     * explicitly permits degradation.
     *
     * @return void
     *
     * @throws \RuntimeException When the adapter cannot arm its wait boundary.
     */
    public function arm(): void;

    /**
     * Removes wake hints that arrived before the second state check.
     *
     * The lock owner calls this immediately after arm() so buffered events cannot
     * create a lost-wakeup window between native registration and state recheck.
     *
     * @return void
     *
     * @throws \RuntimeException When buffered native events cannot be consumed.
     */
    public function drain(): void;

    /**
     * Waits for one wake hint or for the bounded polling interval to end.
     *
     * The implementation waits for at most one tenth of a second and must shorten
     * that interval when the supplied absolute monotonic deadline occurs first.
     * Returning never asserts that lock state changed; the caller must recheck it.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for the standard bounded interval.
     * @param  callable(): float  $monotonic  Returns the current monotonic time in seconds.
     * @return void
     *
     * @throws \RuntimeException When the selected wait operation cannot complete.
     */
    public function block(?float $deadline, callable $monotonic): void;
}
