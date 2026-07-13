<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Lock;

use PDO;
use PDOStatement;
use RuntimeException;
use RunYourApp\LaravelSqliteFair\Exceptions\FairWaitTimeoutException;
use RunYourApp\LaravelSqliteFair\Wait\Waiter;
use Throwable;

/**
 * Acquires the application SQLite writer fence directly until contention is seen,
 * then preserves FIFO order through the private ticket database.
 *
 * A successful call returns while the application `BEGIN IMMEDIATE` fence is held.
 * The caller receives `null` for direct ownership or its committed queue ticket.
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

    public function __construct(
        private readonly PDO $appPdo,
        private readonly LockDatabase $lockDatabase,
        private readonly Waiter $waiter,
        private readonly float $staleHeadSeconds,
        callable $onUnknownAppPdoOutcome,
        callable $disconnect,
        ?callable $monotonic = null,
    ) {
        if ($staleHeadSeconds <= 0.0) {
            throw new RuntimeException('SQLite fair stale-head seconds must be positive.');
        }
        $this->monotonic = $monotonic ?? static fn (): float => hrtime(true) / 1e9;
        $this->onUnknownAppPdoOutcome = $onUnknownAppPdoOutcome;
        $this->disconnect = $disconnect;
    }

    /** @return int|null The committed queue ticket, or null for uncontended direct ownership. */
    public function acquire(?float $deadline = null): ?int
    {
        return $this->acquireWithMode(false, $deadline);
    }

    /** @return int The committed queue ticket. */
    public function acquireQueued(?float $deadline = null): int
    {
        $ticket = $this->acquireWithMode(true, $deadline);
        if ($ticket === null) {
            throw new RuntimeException('Forced queued SQLite fair acquisition returned without a ticket.');
        }

        return $ticket;
    }

    private function acquireWithMode(bool $forceQueued, ?float $deadline): ?int
    {
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

    /** Ends a held pre-business fence first, then performs one nonblocking own-ticket cleanup. */
    public function abortBeforeBusiness(bool $fenceHeld, ?int $ownTicket): void
    {
        if ($fenceHeld) {
            try {
                $this->appPdo->rollBack();
            } catch (Throwable $rollback) {
                $this->markUnknownOutcome($rollback);
                report($rollback);
            }
        }

        if ($ownTicket !== null) {
            try {
                $this->cleanupOwnTicket($ownTicket);
            } catch (Throwable $cleanup) {
                report($cleanup);
            }
        }
    }

    /** Performs the one permitted nonblocking cleanup attempt for an owned ticket. */
    public function cleanupOwnTicket(int $ownTicket): void
    {
        $this->lockDatabase->cleanupExact($ownTicket);
    }

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

        try {
            if ($this->lockDatabase->readHead($deadline) === $observedHead) {
                $this->lockDatabase->deleteForeignHead($observedHead, $deadline);
            }
        } catch (Throwable $exception) {
            if ($this->appPdo->inTransaction()) {
                try {
                    $this->appPdo->rollBack();
                } catch (Throwable $rollback) {
                    $this->markUnknownOutcome($rollback);
                    report($rollback);
                }
            }
            throw $exception;
        }

        $this->rollbackFenceKnown();
        $this->resetObservation();
    }

    private function requeueLostTicket(int $ticket, ?float $deadline): int
    {
        // An absent/reclaimed ticket is not deleted again; a fresh committed ticket joins the tail.
        return $this->lockDatabase->admit($deadline);
    }

    /** Executes exactly one BEGIN attempt with the caller's active busy_timeout restored in all branches. */
    private function tryAppFence(): bool
    {
        $value = $this->appQuery('PRAGMA busy_timeout')->fetchColumn();
        if ($value === false || ! is_numeric($value)) {
            throw new RuntimeException('The application SQLite busy_timeout could not be read.');
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
            }
        } finally {
            try {
                $this->appPdo->exec('PRAGMA busy_timeout='.$busyTimeout);
            } catch (Throwable $restore) {
                if ($began) {
                    try {
                        $this->appPdo->rollBack();
                    } catch (Throwable $rollback) {
                        $this->markUnknownOutcome($rollback);
                        report($rollback);
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
            throw new FairWaitTimeoutException('The SQLite fair writer wait deadline expired.');
        }
    }

    /** @param callable(): bool $stateChanged */
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
            throw new RuntimeException('The application SQLite query could not be prepared.');
        }

        return $statement;
    }
}
