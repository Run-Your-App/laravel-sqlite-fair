<?php

declare(strict_types=1);

use RunYourApp\LaravelSqliteFair\Exceptions\FairSQLiteException;
use RunYourApp\LaravelSqliteFair\Wait\InotifyWaiter;

it('arms and consumes real single and coalesced inotify directory events on linux', function (int $events) {
    if (PHP_OS_FAMILY !== 'Linux') {
        $this->markTestSkipped('The real inotify smoke belongs to Linux hosts.');
    }

    expect(function_exists('inotify_init'))->toBeTrue();
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/inotify-'.$events;
    mkdir($directory, 0775, true);
    $blockedResult = null;
    $select = static function (array &$read, array &$write, array &$except, int $seconds, int $microseconds) use (&$blockedResult, $directory, $events): int|false {
        for ($event = 0; $event < $events; $event++) {
            touch($directory.'/event-'.$event);
        }
        $blockedResult = stream_select($read, $write, $except, $seconds, $microseconds);

        return $blockedResult;
    };
    $waiter = new InotifyWaiter($directory, false, $select);
    $waiter->arm();
    $waiter->drain();
    $waiter->block(hrtime(true) / 1e9 + 0.1, static fn (): float => hrtime(true) / 1e9);

    expect($blockedResult)->toBeGreaterThan(0);
})->with([1, 3]);

it('degrades only auto after a post-arm inotify failure', function (bool $auto) {
    if (PHP_OS_FAMILY !== 'Linux') {
        $this->markTestSkipped('The deterministic inotify failure proof belongs to Linux hosts.');
    }
    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/inotify-failure-'.($auto ? 'auto' : 'native');
    mkdir($directory, 0775, true);
    $waiter = new InotifyWaiter($directory, $auto);
    rmdir($directory);

    if ($auto) {
        $waiter->arm();
        $waiter->block(0.0, static fn (): float => 0.0);
        expect(true)->toBeTrue();
    } else {
        expect(fn () => $waiter->arm())->toThrow(FairSQLiteException::class);
    }
})->with([true, false]);

it('returns to the lock state check immediately after an automatic block degradation', function () {
    if (PHP_OS_FAMILY !== 'Linux') {
        $this->markTestSkipped('The deterministic inotify failure proof belongs to Linux hosts.');
    }

    $directory = $GLOBALS['sqliteFairTestRunDirectory'].'/inotify-block-degradation';
    mkdir($directory, 0775, true);
    $monotonicCalls = 0;
    $waiter = new InotifyWaiter(
        $directory,
        true,
        static fn (array &$read, array &$write, array &$except, int $seconds, int $microseconds): false => false,
    );

    $waiter->block(1.0, static function () use (&$monotonicCalls): float {
        $monotonicCalls++;

        return 0.0;
    });

    expect($monotonicCalls)->toBe(1);
});
