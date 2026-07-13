<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Wait;

use RuntimeException;

/**
 * Provides bounded polling through an owned selectable socket pair.
 *
 * WaiterFactory selects this adapter explicitly and for native Windows. Native
 * adapters also use it after an allowed post-arm degradation. The sockets provide
 * an interruptible interval without adding process sleeps or filesystem signals.
 *
 * @internal
 */
final class PollingWaiter implements Waiter
{
    /** @var array{0: resource, 1: resource} */
    private array $sockets;

    /**
     * Creates the connected local sockets used for every polling interval.
     *
     * @return void
     *
     * @throws RuntimeException When the loopback server, address, client, or accepted socket cannot be created.
     */
    public function __construct()
    {
        $server = @stream_socket_server('tcp://127.0.0.1:0', $errorCode, $errorMessage);
        if ($server === false) {
            throw new RuntimeException("The polling wait server could not be created: {$errorMessage} ({$errorCode}).");
        }
        $address = stream_socket_get_name($server, false);
        if ($address === false) {
            fclose($server);
            throw new RuntimeException('The polling wait server address could not be read.');
        }
        $writeSocket = @stream_socket_client('tcp://'.$address, $errorCode, $errorMessage);
        $readSocket = @stream_socket_accept($server, 1);
        fclose($server);
        if ($writeSocket === false || $readSocket === false) {
            if (is_resource($writeSocket)) {
                fclose($writeSocket);
            }
            throw new RuntimeException("The polling wait connection could not be created: {$errorMessage} ({$errorCode}).");
        }
        $this->sockets = [$readSocket, $writeSocket];
    }

    /**
     * Closes both sockets owned by this polling adapter.
     *
     * @return void
     */
    public function __destruct()
    {
        fclose($this->sockets[0]);
        fclose($this->sockets[1]);
    }

    /**
     * Leaves the always-ready polling interval prepared for the second state check.
     *
     * Polling has no external event source to register, so this method intentionally
     * keeps the existing socket pair unchanged.
     *
     * @return void
     */
    public function arm(): void {}

    /**
     * Leaves the polling adapter unchanged because it buffers no wake events.
     *
     * @return void
     */
    public function drain(): void {}

    /**
     * Waits for the bounded polling interval on the owned read socket.
     *
     * The socket is intentionally never written during normal operation, so select
     * returns after at most one tenth of a second or the earlier absolute deadline.
     *
     * @param  float|null  $deadline  Absolute monotonic deadline, or null for the standard bounded interval.
     * @param  callable(): float  $monotonic  Returns the current monotonic time in seconds.
     * @return void
     *
     * @throws RuntimeException When stream selection cannot complete.
     */
    public function block(?float $deadline, callable $monotonic): void
    {
        $seconds = $deadline === null ? 0.1 : max(0.0, min(0.1, $deadline - $monotonic()));
        if ($seconds === 0.0) {
            return;
        }
        $read = [$this->sockets[0]];
        $write = [];
        $except = [];
        if (@stream_select($read, $write, $except, 0, (int) ($seconds * 1_000_000)) === false) {
            throw new RuntimeException('The polling wait interval could not be completed.');
        }
    }
}
