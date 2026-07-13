<?php

declare(strict_types=1);

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
        if (! is_callable($callback)) { throw new RuntimeException('Real kqueue callback is unavailable.'); }
        if ($function === 'kevent' && $arguments[5] instanceof FFI\CData && $arguments[5]->tv_nsec > 0 && ! $injected) {
            $injected = true;
            for ($event = 0; $event < $events; $event++) { touch($directory.'/event-'.$event); }
        }
        $result = $callback(...$arguments);
        if (! is_int($result)) { throw new RuntimeException('Real kqueue callback returned a non-integer.'); }
        if ($injected && $function === 'kevent' && $arguments[5] instanceof FFI\CData && $arguments[5]->tv_nsec > 0) { $blockedResult = $result; }
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
        if ($function === 'kevent' && $arguments[1] instanceof FFI\CData && $arguments[2] === 1 && ++$armCalls > 1) {
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
