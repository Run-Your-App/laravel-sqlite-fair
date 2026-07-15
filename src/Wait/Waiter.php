<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;

/**
 * Coordinates a bounded wake hint between complete lock-state checks.
 *
 * FairSQLiteLock starts this protocol once per writer acquisition. Whenever lock
 * state requires waiting, it arms the adapter, drains buffered hints, checks the
 * complete state again and then blocks. A wake event never replaces that recheck.
 *
 * @internal
 */
interface Waiter
{
    /**
     * Starts a new writer-acquisition wait cycle.
     *
     * FairSQLiteLock calls this exactly once before each direct or queued
     * acquisition. Implementations reset only per-acquisition wait state here;
     * they must not inspect or change the ticket queue or application database.
     *
     * @return void The adapter is ready to begin a new acquisition cycle.
     */
    public function beginContention(): void;

    /**
     * Arms the adapter before `FairSQLiteLock` performs its second state check.
     *
     * Implementations must either prepare their next wake hint or leave an
     * already selected polling strategy ready. A native startup failure is an
     * error unless the adapter has already armed successfully and its auto mode
     * explicitly permits degradation.
     *
     * @return void The adapter is ready for the caller's immediate second state check.
     *
     * @throws FairSQLiteException When the adapter cannot arm its filesystem notification or polling state.
     */
    public function arm(): void;

    /**
     * Removes wake hints that arrived before the second state check.
     *
     * `FairSQLiteLock` calls this immediately after `arm()` so buffered events cannot
     * create a lost-wakeup window between native registration and state recheck.
     *
     * @return void Every wake hint already buffered by the adapter has been consumed.
     *
     * @throws FairSQLiteException When buffered native filesystem events cannot be consumed.
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
     * @return void The bounded wait ended; callers must perform a complete state check.
     *
     * @throws FairSQLiteException When the selected native or polling wait cannot complete.
     */
    public function block(?float $deadline, callable $monotonic): void;
}
