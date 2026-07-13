<?php

declare(strict_types=1);

use FFI\CData;
use RunYourApp\LaravelSqliteFair\Wait\KqueueWaiter;

it('arms and consumes real single and coalesced kqueue directory events on darwin', function (int $events) {
    if (PHP_OS_FAMILY !== 'Darwin') {
        $this->markTestSkipped('The real kqueue smoke belongs to Darwin hosts.');
    }

    expect(class_exists(FFI::class))->toBeTrue()
        ->and(KqueueWaiter::isSupported())->toBeTrue();
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/kqueue-'.$events;
    mkdir($directory, 0775, true);
    $blockedResult = null;
    $injected = false;
    $systemCall = static function (FFI $ffi, string $function, mixed ...$arguments) use (&$blockedResult, &$injected, $directory, $events): int {
        $callback = [$ffi, $function];
        if (! is_callable($callback)) {
            throw new RuntimeException('Real kqueue callback is unavailable.');
        }
        if ($function === 'kevent' && $arguments[5] instanceof CData && $arguments[5]->tv_nsec > 0 && ! $injected) {
            $injected = true;
            for ($event = 0; $event < $events; $event++) {
                touch($directory.'/event-'.$event);
            }
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) {
            throw new RuntimeException('Real kqueue callback returned a non-integer.');
        }
        if ($injected && $function === 'kevent' && $arguments[5] instanceof CData && $arguments[5]->tv_nsec > 0) {
            $blockedResult = $result;
        }

        return $result;
    };
    $waiter = new KqueueWaiter($directory, false, $systemCall);
    $waiter->arm();
    $waiter->drain();
    $waiter->block(hrtime(true) / 1e9 + 0.1, static fn (): float => hrtime(true) / 1e9);

    expect($blockedResult)->toBeGreaterThan(0);
})->with([1, 3]);

it('degrades only auto after a deterministic post-arm kqueue failure', function (bool $auto) {
    if (PHP_OS_FAMILY !== 'Darwin') {
        $this->markTestSkipped('The deterministic kqueue failure proof belongs to Darwin hosts.');
    }
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/kqueue-failure-'.($auto ? 'auto' : 'native');
    mkdir($directory, 0775, true);
    $armCalls = 0;
    $systemCall = static function (FFI $ffi, string $function, mixed ...$arguments) use (&$armCalls): int {
        if ($function === 'kevent' && $arguments[1] instanceof CData && $arguments[2] === 1 && ++$armCalls > 1) {
            return -1;
        }
        $callback = [$ffi, $function];
        if (! is_callable($callback)) {
            throw new RuntimeException('Injected kqueue callback is not callable.');
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) {
            throw new RuntimeException('Injected kqueue callback returned a non-integer.');
        }

        return $result;
    };
    $waiter = new KqueueWaiter($directory, $auto, $systemCall);

    if ($auto) {
        $waiter->arm();
        $waiter->block(0.0, static fn (): float => 0.0);
        expect(true)->toBeTrue();
    } else {
        expect(fn () => $waiter->arm())->toThrow(RuntimeException::class);
    }
})->with([true, false]);

it('degrades only auto after a deterministic post-arm kqueue drain failure', function (bool $auto) {
    if (PHP_OS_FAMILY !== 'Darwin') {
        $this->markTestSkipped('The deterministic kqueue drain failure proof belongs to Darwin hosts.');
    }
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/kqueue-drain-failure-'.($auto ? 'auto' : 'native');
    mkdir($directory, 0775, true);
    $drainCalls = 0;
    $keventCalls = 0;
    $systemCall = static function (FFI $ffi, string $function, mixed ...$arguments) use (&$drainCalls, &$keventCalls): int {
        if ($function === 'kevent') {
            $keventCalls++;
        }
        if ($function === 'kevent'
            && $arguments[1] === null
            && $arguments[2] === 0
            && $arguments[5] instanceof CData
            && $arguments[5]->tv_nsec === 0
            && ++$drainCalls > 1) {
            return -1;
        }
        $callback = [$ffi, $function];
        if (! is_callable($callback)) {
            throw new RuntimeException('Injected kqueue callback is not callable.');
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) {
            throw new RuntimeException('Injected kqueue callback returned a non-integer.');
        }

        return $result;
    };
    $waiter = new KqueueWaiter($directory, $auto, $systemCall);

    if ($auto) {
        $waiter->drain();
        $nativeCallsAfterFailure = $keventCalls;
        $waiter->arm();
        expect($drainCalls)->toBe(2)
            ->and($keventCalls)->toBe($nativeCallsAfterFailure);
    } else {
        expect(fn () => $waiter->drain())->toThrow(RuntimeException::class, 'could not be drained')
            ->and($drainCalls)->toBe(2);
    }
})->with([true, false]);

it('closes both descriptors when initial native startup registration fails', function () {
    if (PHP_OS_FAMILY !== 'Darwin') {
        $this->markTestSkipped('The deterministic kqueue startup failure proof belongs to Darwin hosts.');
    }
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/kqueue-startup-failure';
    mkdir($directory, 0775, true);
    $handles = [];
    $closed = [];
    $systemCall = static function (FFI $ffi, string $function, mixed ...$arguments) use (&$handles, &$closed): int {
        if ($function === 'kevent' && $arguments[1] instanceof CData && $arguments[2] === 1) {
            return -1;
        }
        if ($function === 'close') {
            $closed[] = $arguments[0];
        }
        $callback = [$ffi, $function];
        if (! is_callable($callback)) {
            throw new RuntimeException('Injected kqueue callback is not callable.');
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) {
            throw new RuntimeException('Injected kqueue callback returned a non-integer.');
        }
        if (in_array($function, ['kqueue', 'open'], true)) {
            $handles[$function] = $result;
        }

        return $result;
    };

    expect(fn () => new KqueueWaiter($directory, false, $systemCall))
        ->toThrow(RuntimeException::class, 'could not be armed')
        ->and($closed)->toBe([$handles['open'], $handles['kqueue']]);
});

it('degrades auto after a native block failure without starting a second wait in the same block call', function (bool $auto) {
    if (PHP_OS_FAMILY !== 'Darwin') {
        $this->markTestSkipped('The deterministic kqueue block failure proof belongs to Darwin hosts.');
    }
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/kqueue-block-failure-'.($auto ? 'auto' : 'native');
    mkdir($directory, 0775, true);
    $nativeCalls = 0;
    $blockingCalls = 0;
    $systemCall = static function (FFI $ffi, string $function, mixed ...$arguments) use (&$nativeCalls, &$blockingCalls): int {
        $nativeCalls++;
        if ($function === 'kevent'
            && $arguments[1] === null
            && $arguments[2] === 0
            && $arguments[5] instanceof CData
            && $arguments[5]->tv_nsec > 0) {
            $blockingCalls++;

            return -1;
        }
        $callback = [$ffi, $function];
        if (! is_callable($callback)) {
            throw new RuntimeException('Injected kqueue callback is not callable.');
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) {
            throw new RuntimeException('Injected kqueue callback returned a non-integer.');
        }

        return $result;
    };
    $waiter = new KqueueWaiter($directory, $auto, $systemCall);
    $clockCalls = 0;
    $monotonic = static function () use (&$clockCalls): float {
        if (++$clockCalls > 1) {
            throw new RuntimeException('A second wait started in the same block call.');
        }

        return 0.0;
    };

    if (! $auto) {
        expect(fn () => $waiter->block(10.0, $monotonic))->toThrow(RuntimeException::class, 'kqueue wait failed')
            ->and($clockCalls)->toBe(1)
            ->and($blockingCalls)->toBe(1);

        return;
    }

    $waiter->block(10.0, $monotonic);
    $nativeCallsAfterFailure = $nativeCalls;
    $waiter->arm();
    $waiter->drain();
    $waiter->block(0.0, static fn (): float => 0.0);

    expect($clockCalls)->toBe(1)
        ->and($blockingCalls)->toBe(1)
        ->and($nativeCalls)->toBe($nativeCallsAfterFailure);
})->with([true, false]);
