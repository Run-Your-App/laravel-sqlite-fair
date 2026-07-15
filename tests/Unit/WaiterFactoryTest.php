<?php

declare(strict_types=1);

use RunYourApp\LaravelSqliteFair\Wait\InotifyWaiter;
use RunYourApp\LaravelSqliteFair\Wait\PollingWaiter;
use RunYourApp\LaravelSqliteFair\Wait\WaiterFactory;

it('reports the deterministic host capability matrix', function (string $family, ?bool $inotify, array $expected) {
    expect(WaiterFactory::capabilities($family, $inotify))->toBe($expected);
})->with([
    ['Linux', true, ['platform' => 'linux', 'waiter' => 'inotify', 'available' => true]],
    ['Linux', false, ['platform' => 'linux', 'waiter' => 'inotify', 'available' => false]],
    ['Windows', null, ['platform' => 'other', 'waiter' => 'polling', 'available' => true]],
]);

it('always selects polling explicitly', function () {
    expect(WaiterFactory::make('polling', __DIR__))->toBeInstanceOf(PollingWaiter::class);
});

it('selects the concrete strategy for the current host', function () {
    $autoDirectory = $GLOBALS['sqliteFairTestRunDirectory'].'/factory-host-auto';
    $nativeDirectory = $GLOBALS['sqliteFairTestRunDirectory'].'/factory-host-native';
    if (! is_dir($autoDirectory)) {
        mkdir($autoDirectory, 0775, true);
    }
    if (! is_dir($nativeDirectory)) {
        mkdir($nativeDirectory, 0775, true);
    }
    $expected = PHP_OS_FAMILY === 'Linux' ? InotifyWaiter::class : PollingWaiter::class;

    $auto = WaiterFactory::make('auto', $autoDirectory);
    expect($auto)->toBeInstanceOf($expected);
    if (PHP_OS_FAMILY === 'Linux') {
        $native = WaiterFactory::make('native', $nativeDirectory);
        expect($native)->toBeInstanceOf($expected);
    } else {
        expect(fn () => WaiterFactory::make('native', $nativeDirectory))->toThrow(RuntimeException::class);
    }
});

it('fails linux native startup for a missing directory', function () {
    if (PHP_OS_FAMILY !== 'Linux') {
        $this->markTestSkipped('Native startup failure belongs to Linux hosts.');
    }
    expect(fn () => WaiterFactory::make('native', $GLOBALS['sqliteFairTestRunDirectory'].'/missing-native-directory'))
        ->toThrow(RuntimeException::class);
});

it('backs polling off from 100 microseconds to the 100 millisecond cap', function () {
    $intervals = [];
    $waiter = new PollingWaiter(static function (int $seconds, int $nanoseconds) use (&$intervals): bool {
        $intervals[] = ($seconds * 1_000_000) + intdiv($nanoseconds, 1_000);

        return true;
    });

    $waiter->beginContention();
    for ($attempt = 0; $attempt < 12; $attempt++) {
        $waiter->block(null, static fn (): float => 0.0);
    }

    expect($intervals)->toBe([100, 200, 400, 800, 1_600, 3_200, 6_400, 12_800, 25_600, 51_200, 100_000, 100_000]);
});

it('resets polling backoff for every writer acquisition', function () {
    $intervals = [];
    $waiter = new PollingWaiter(static function (int $seconds, int $nanoseconds) use (&$intervals): bool {
        $intervals[] = intdiv($nanoseconds, 1_000);

        return true;
    });

    $waiter->beginContention();
    $waiter->block(null, static fn (): float => 0.0);
    $waiter->block(null, static fn (): float => 0.0);
    $waiter->beginContention();
    $waiter->block(null, static fn (): float => 0.0);

    expect($intervals)->toBe([100, 200, 100]);
});

it('clamps polling to the deadline without advancing backoff', function () {
    $intervals = [];
    $waiter = new PollingWaiter(static function (int $seconds, int $nanoseconds) use (&$intervals): bool {
        $intervals[] = intdiv($nanoseconds, 1_000);

        return true;
    });

    $waiter->beginContention();
    $waiter->block(null, static fn (): float => 0.0);
    $waiter->block(0.00015, static fn (): float => 0.0);
    $waiter->block(0.0, static fn (): float => 0.0);
    $waiter->block(0.0000005, static fn (): float => 0.0);
    $waiter->block(null, static fn (): float => 0.0);

    expect($intervals)->toBe([100, 150, 200]);
});

it('returns immediately after a signal interruption without advancing backoff', function () {
    $intervals = [];
    $results = [
        ['seconds' => 0, 'nanoseconds' => 50_000],
        true,
    ];
    $waiter = new PollingWaiter(static function (int $seconds, int $nanoseconds) use (&$intervals, &$results): array|bool {
        $intervals[] = intdiv($nanoseconds, 1_000);

        return array_shift($results);
    });

    $waiter->beginContention();
    $waiter->block(null, static fn (): float => 0.0);
    $waiter->block(null, static fn (): float => 0.0);

    expect($intervals)->toBe([100, 100]);
});

it('throws when the polling sleep fails without advancing backoff', function () {
    $intervals = [];
    $results = [false, true];
    $waiter = new PollingWaiter(static function (int $seconds, int $nanoseconds) use (&$intervals, &$results): bool {
        $intervals[] = intdiv($nanoseconds, 1_000);

        return array_shift($results);
    });

    $waiter->beginContention();
    expect(fn () => $waiter->block(null, static fn (): float => 0.0))->toThrow(RuntimeException::class);
    $waiter->block(null, static fn (): float => 0.0);

    expect($intervals)->toBe([100, 100]);
});
