<?php

declare(strict_types=1);

use RunYourApp\LaravelSqliteFair\Wait\InotifyWaiter;
use RunYourApp\LaravelSqliteFair\Wait\KqueueWaiter;
use RunYourApp\LaravelSqliteFair\Wait\PollingWaiter;
use RunYourApp\LaravelSqliteFair\Wait\WaiterFactory;

it('reports the deterministic host capability matrix', function (string $family, bool $native, string $platform, bool $available) {
    expect(WaiterFactory::capabilities($family, $native, $native))->toBe([
        'platform' => $platform,
        'native_available' => $available,
    ]);
})->with([
    ['Linux', true, 'linux', true],
    ['Linux', false, 'linux', false],
    ['Darwin', true, 'darwin', true],
    ['Darwin', false, 'darwin', false],
    ['Windows', false, 'windows', false],
]);

it('always selects polling explicitly', function () {
    expect(WaiterFactory::make('polling', __DIR__))->toBeInstanceOf(PollingWaiter::class);
});

it('uses a working polling protocol for simulated windows auto capability', function () {
    expect(WaiterFactory::capabilities('Windows'))->toBe(['platform' => 'windows', 'native_available' => false]);
    $waiter = new PollingWaiter();
    $waiter->arm();
    $waiter->drain();
    $waiter->block(0.0, static fn (): float => 0.0);

    expect($waiter)->toBeInstanceOf(PollingWaiter::class);
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
    $expected = match (PHP_OS_FAMILY) {
        'Linux' => InotifyWaiter::class,
        'Darwin' => KqueueWaiter::class,
        'Windows' => PollingWaiter::class,
        default => null,
    };
    if ($expected === null) {
        $this->markTestSkipped('The current host has no package wait strategy.');
    }

    $auto = WaiterFactory::make('auto', $autoDirectory);
    expect($auto)->toBeInstanceOf($expected);
    if (PHP_OS_FAMILY === 'Windows') {
        expect(fn () => WaiterFactory::make('native', $nativeDirectory))->toThrow(RuntimeException::class);
    } else {
        $native = WaiterFactory::make('native', $nativeDirectory);
        expect($native)->toBeInstanceOf($expected);
    }
});

it('fails matching-host native startup for a missing directory', function () {
    if (! in_array(PHP_OS_FAMILY, ['Linux', 'Darwin'], true)) {
        $this->markTestSkipped('Native startup failure belongs to Linux and Darwin hosts.');
    }
    expect(fn () => WaiterFactory::make('native', $GLOBALS['sqliteFairTestRunDirectory'].'/missing-native-directory'))
        ->toThrow(RuntimeException::class);
});
