<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Lock;

use PDO;
use PDOStatement;
use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Support\FairSQLiteDebug;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;
use Throwable;

/**
 * Acquires the SQLite writer fence for one physical application PDO.
 *
 * `FairSQLiteConnection` calls this state machine before executing business writes.
 * It attempts the application `BEGIN IMMEDIATE` fence directly while the ticket
 * queue is empty, then preserves FIFO order through LockDatabase after contention.
 * It also observes and recovers stale foreign heads only while holding the real
 * application writer fence.
 *
 * A successful acquisition returns while that application fence remains held. The
 * caller receives null for direct acquisition or its committed queue ticket and is
 * responsible for the later business commit, rollback, and ticket cleanup.
 *
 * @internal
 */
final class FairSQLiteLock
{
    /** @var callable(): float */
    private $monotonic;

    /** @var callable(Throwable): void */
    private $onUnknownAppPdoOutcome;

    /** @var callable(): void */
    private $disconnect;

    private ?int $observedHeadTicket = null;

    private ?float $observedSinceMonotonic = null;

    /**
     * Creates the per-connection writer-acquisition state machine.
     *
     * @param  PDO  $appPdo  Application PDO on which the real writer fence is acquired.
     * @param  LockDatabase  $lockDatabase  Private FIFO ticket database for this application database.
     * @param  Waiter  $waiter  Shared native or polling wait adapter.
     * @param  float  $staleHeadSeconds  Positive seconds before fenced stale-head revalidation.
     * @param  callable(Throwable): void  $onUnknownAppPdoOutcome  Marks an indeterminate app-PDO outcome.
     * @param  callable(): void  $disconnect  Disconnects an application PDO whose timeout or transaction outcome is unsafe.
     * @param  (callable(): float)|null  $monotonic  Optional monotonic clock used by deterministic tests.
     * @param  bool  $debug  Whether abnormal transitions emit structured Laravel debug logs.
     * @return void The instance is ready to coordinate the supplied application PDO.
     *
     * @throws FairSQLiteException When the stale-head threshold is not positive.
     *
     * @internal
     */
    public function __construct(
        private readonly PDO $appPdo,
        private readonly LockDatabase $lockDatabase,
        private readonly Waiter $waiter,
        private readonly float $staleHeadSeconds,
        callable $onUnknownAppPdoOutcome,
        callable $disconnect,
        ?callable $monotonic = null,
        private readonly bool $debug = false,
    ) {
        if ($staleHeadSeconds <= 0.0) {
            throw new FairSQLiteException('SQLite fair stale-head seconds must be positive.');
        }
        $this->monotonic = $monotonic ?? static fn (): float => hrtime(true) / 1e9;
        $this->onUnknownAppPdoOutcome = $onUnknownAppPdoOutcome;
        $this->disconnect = $disconnect;
    }

    /**
     * Acquires the application writer fence using direct-first fairness.
     *
     * The method first proves that the ticket queue is empty. Contention joins the
     * committed FIFO queue, and stale tickets are removed only after fenced revalidation.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return int|null The committed queue ticket, or null for an uncontended direct acquisition.
     *
     * @throws Throwable When acquisition, recovery, timeout, or abort cleanup fails.
     */
    public function acquire(?float $deadline = null): ?int
    {
        return $this->acquireWithMode(false, $deadline);
    }

    /**
     * Acquires the application writer fence through the FIFO queue.
     *
     * This forced path always creates a committed ticket and returns only while
     * that ticket is the queue head and the application writer fence is held.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return int The committed queue ticket held by the caller.
     *
     * @throws Throwable When queued acquisition, recovery, timeout, or abort cleanup fails.
     */
    public function acquireQueued(?float $deadline = null): int
    {
        $ticket = $this->acquireWithMode(true, $deadline);
        if ($ticket === null) {
            throw new FairSQLiteException('Forced queued SQLite fair acquisition returned without a ticket.');
        }

        return $ticket;
    }

    /**
     * Runs the direct-first or forced-queued writer-acquisition state machine.
     *
     * Direct mode returns only after proving the queue remained empty across the
     * application fence. Queued mode admits once, follows the committed FIFO head,
     * requeues a reclaimed own ticket, and returns only while owning both the head
     * ticket and application writer fence. Any failure triggers pre-business abort.
     *
     * @param  bool  $forceQueued  Whether to skip direct acquisition and join the queue immediately.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     * @return int|null The committed owned ticket, or null after direct acquisition.
     *
     * @throws Throwable When acquisition or its pre-business abort cannot complete normally.
     */
    private function acquireWithMode(bool $forceQueued, ?float $deadline): ?int
    {
        $this->waiter->beginContention();
        $clock = $this->monotonic;
        $ticket = null;
        $fenceHeld = false;

        try {
            if (! $forceQueued) {
                $head = $this->lockDatabase->readHead($deadline);
                if ($head === null) {
                    $direct = $this->tryAppFence();
                    if ($direct) {
                        $fenceHeld = true;
                        if ($this->lockDatabase->readHead($deadline) === null) {
                            $this->resetObservation();

                            return null;
                        }
                        $fenceHeld = false;
                        $this->rollbackFenceKnown();
                    }
                }
            }

            $ticket = $this->lockDatabase->admit($deadline);

            while (true) {
                $this->assertBeforeDeadline($deadline);
                $head = $this->lockDatabase->readHead($deadline);
                if ($head === null || $head > $ticket) {
                    if ($head === null) {
                        $this->resetObservation();
                    }
                    $ticket = $this->requeueLostTicket($ticket, $deadline);

                    continue;
                }
                if ($head !== $ticket) {
                    $this->observe($head);
                    if ($clock() - (float) $this->observedSinceMonotonic >= $this->staleHeadSeconds) {
                        $this->recoverStaleHead($head, $deadline);

                        continue;
                    }
                    $this->waitAfterStateCheck(
                        fn (): bool => $this->lockDatabase->readHead($deadline) !== $head,
                        $deadline,
                    );

                    continue;
                }

                $this->resetObservation();
                if (! $this->tryAppFence()) {
                    $this->waitAfterStateCheck(
                        fn (): bool => $this->lockDatabase->readHead($deadline) !== $ticket,
                        $deadline,
                    );

                    continue;
                }
                $fenceHeld = true;
                if ($this->lockDatabase->readHead($deadline) === $ticket) {
                    return $ticket;
                }

                $fenceHeld = false;
                $this->rollbackFenceKnown();
                $ticket = $this->requeueLostTicket($ticket, $deadline);
            }
        } catch (Throwable $exception) {
            $this->abortBeforeBusiness($fenceHeld, $ticket);
            throw $exception;
        }
    }

    /**
     * Aborts a pre-business acquisition without replaying uncertain work.
     *
     * Rollback and emergency ticket-cleanup failures are reported and converted to
     * the connection's unknown-outcome handling; this method does not rethrow
     * them because it is already running while another acquisition failure escapes.
     *
     * @param  bool  $fenceHeld  Whether this attempt still holds the application writer fence.
     * @param  int|null  $ownTicket  Committed ticket to clean once, or null for direct acquisition.
     * @return void Rollback and owned-ticket cleanup have each been attempted at most once.
     */
    public function abortBeforeBusiness(bool $fenceHeld, ?int $ownTicket): void
    {
        if ($fenceHeld) {
            try {
                $this->appPdo->rollBack();
                FairSQLiteDebug::log($this->debug, 'lock_rollback', ['operation' => 'pre_business_abort']);
            } catch (Throwable $rollback) {
                $this->markUnknownOutcome($rollback);
                report($rollback);
            }
        }

        if ($ownTicket !== null) {
            try {
                $this->cleanupOwnTicket($ownTicket);
            } catch (Throwable $cleanup) {
                FairSQLiteDebug::log($this->debug, 'cleanup_failed', ['operation' => 'abort']);
                report($cleanup);
            }
        }
    }

    /**
     * Performs the one permitted nonblocking cleanup for an owned ticket.
     *
     * @param  int  $ownTicket  Committed ticket owned by the aborting connection.
     * @return void The cleanup attempt completed with a known outcome.
     *
     * @throws Throwable When the one cleanup attempt fails.
     */
    public function cleanupOwnTicket(int $ownTicket): void
    {
        $this->lockDatabase->cleanupExact($ownTicket);
    }

    /**
     * Removes a stale foreign head only after fenced revalidation.
     *
     * Failure to obtain the application writer fence proves an active writer may
     * still own the ticket, so this method waits without deleting it. After fencing,
     * the same ticket must still be queue head before LockDatabase removes it.
     */
    private function recoverStaleHead(int $observedHead, ?float $deadline): void
    {
        if (! $this->tryAppFence()) {
            $clock = $this->monotonic;
            $this->waitAfterStateCheck(
                fn (): bool => $this->lockDatabase->readHead($deadline) !== $observedHead,
                $deadline,
            );

            return;
        }

        $recovered = false;
        try {
            if ($this->lockDatabase->readHead($deadline) === $observedHead) {
                $this->lockDatabase->deleteForeignHead($observedHead, $deadline);
                $recovered = true;
            }
        } catch (Throwable $exception) {
            if ($this->appPdo->inTransaction()) {
                try {
                    $this->appPdo->rollBack();
                    FairSQLiteDebug::log($this->debug, 'lock_rollback', ['operation' => 'recovery_failure']);
                } catch (Throwable $rollback) {
                    $this->markUnknownOutcome($rollback);
                    report($rollback);
                }
            }
            throw $exception;
        }

        $this->rollbackFenceKnown();
        $this->resetObservation();
        if ($recovered) {
            FairSQLiteDebug::log($this->debug, 'stale_head_recovered', ['head_ticket' => $observedHead]);
        }
    }

    /**
     * Replaces a reclaimed own ticket by joining the committed queue tail.
     *
     * The absent ticket is never deleted again; only the new committed ticket is
     * returned to the acquisition state machine.
     */
    private function requeueLostTicket(int $ticket, ?float $deadline): int
    {
        // An absent/reclaimed ticket is not deleted again; a fresh committed ticket joins the tail.
        $newTicket = $this->lockDatabase->admit($deadline);
        FairSQLiteDebug::log($this->debug, 'ticket_requeued', ['lost_ticket' => $ticket, 'new_ticket' => $newTicket]);

        return $newTicket;
    }

    /**
     * Attempts BEGIN IMMEDIATE once and restores the caller's busy timeout.
     *
     * Numeric BUSY or LOCKED means the fence was not acquired and returns false.
     * Every other failure escapes. After a restore failure, this method first
     * rolls back a newly acquired fence and retries the idempotent restore outside
     * that transaction. A second restore failure disconnects the unsafe PDO; an
     * unsuccessful rollback remains an unknown PDO outcome.
     */
    private function tryAppFence(): bool
    {
        $value = $this->appQuery('PRAGMA busy_timeout')->fetchColumn();
        if ($value === false || ! is_numeric($value)) {
            throw new FairSQLiteException('The application SQLite busy_timeout could not be read.');
        }
        $busyTimeout = (int) $value;
        $this->appPdo->exec('PRAGMA busy_timeout=0');
        $began = false;

        try {
            try {
                $this->appPdo->exec('BEGIN IMMEDIATE');
                $began = true;
            } catch (Throwable $exception) {
                if (! LockDatabase::isBusyOrLocked($exception)) {
                    throw $exception;
                }
                FairSQLiteDebug::log($this->debug, 'lock_retry', ['operation' => 'app_fence']);
            }
        } finally {
            try {
                $this->appPdo->exec('PRAGMA busy_timeout='.$busyTimeout);
            } catch (Throwable $restore) {
                $rollbackKnown = true;
                if ($began) {
                    try {
                        $this->appPdo->rollBack();
                        FairSQLiteDebug::log($this->debug, 'lock_rollback', ['operation' => 'busy_timeout_restore']);
                    } catch (Throwable $rollback) {
                        $rollbackKnown = false;
                        $this->markUnknownOutcome($rollback);
                        report($rollback);
                    }
                }
                if ($rollbackKnown) {
                    try {
                        $this->appPdo->exec('PRAGMA busy_timeout='.$busyTimeout);
                    } catch (Throwable $secondRestore) {
                        report($secondRestore);
                        try {
                            ($this->disconnect)();
                        } catch (Throwable $disconnect) {
                            report($disconnect);
                        }
                    }
                }
                throw $restore;
            }
        }

        return $began;
    }

    private function rollbackFenceKnown(): void
    {
        try {
            $this->appPdo->rollBack();
        } catch (Throwable $exception) {
            $this->markUnknownOutcome($exception);
            throw $exception;
        }
    }

    private function observe(int $head): void
    {
        if ($this->observedHeadTicket === $head) {
            return;
        }
        $clock = $this->monotonic;
        $this->observedHeadTicket = $head;
        $this->observedSinceMonotonic = $clock();
    }

    private function resetObservation(): void
    {
        $this->observedHeadTicket = null;
        $this->observedSinceMonotonic = null;
    }

    private function assertBeforeDeadline(?float $deadline): void
    {
        $clock = $this->monotonic;
        if ($deadline !== null && $clock() >= $deadline) {
            FairSQLiteDebug::log($this->debug, 'wait_timeout', ['operation' => 'writer_wait']);
            throw new FairWaitTimeoutException('The SQLite fair writer wait deadline expired.');
        }
    }

    /**
     * Arms the waiter, performs the required second state check, then blocks.
     *
     * @param  callable(): bool  $stateChanged  Rechecks the exact queue condition after native arming.
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for an unbounded wait.
     */
    private function waitAfterStateCheck(callable $stateChanged, ?float $deadline): void
    {
        $clock = $this->monotonic;
        $this->waiter->arm();
        $this->waiter->drain();
        if ($stateChanged()) {
            return;
        }
        $this->waiter->block($deadline, $clock);
    }

    private function markUnknownOutcome(Throwable $rollback): void
    {
        FairSQLiteDebug::log($this->debug, 'unknown_pdo_outcome', ['operation' => 'app_rollback']);
        try {
            ($this->onUnknownAppPdoOutcome)($rollback);
        } catch (Throwable $guardFailure) {
            report($guardFailure);
        }
        try {
            ($this->disconnect)();
        } catch (Throwable $disconnectFailure) {
            report($disconnectFailure);
        }
    }

    private function appQuery(string $sql): PDOStatement
    {
        $statement = $this->appPdo->query($sql);
        if ($statement === false) {
            throw new FairSQLiteException('The application SQLite query could not be prepared.');
        }

        return $statement;
    }
}
