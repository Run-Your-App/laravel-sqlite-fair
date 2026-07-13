<?php

declare(strict_types=1);

use RunYourApp\LaravelSqliteFair\Tests\Support\ProcessHarness;

it('closes every previously started child when a later process cannot start', function (): void {
    $startedProcess = null;
    $startedPipes = [];
    $startCalls = 0;
    $startProcess = static function (
        array $command,
        array $descriptorSpec,
        array &$pipes,
        string $workingDirectory,
        array $environment,
    ) use (&$startedProcess, &$startedPipes, &$startCalls) {
        $startCalls++;
        if ($startCalls === 2) {
            $pipes = [];

            return false;
        }

        $startedProcess = proc_open(
            $command,
            $descriptorSpec,
            $pipes,
            $workingDirectory,
            $environment,
        );
        $startedPipes = $pipes;

        return $startedProcess;
    };
    $harness = new ProcessHarness($startProcess);

    $harness->run(function () use ($harness): void {
        expect(fn (): array => $harness->runChildren([
            ['scenario' => 'wait-for-missing-signal'],
            ['scenario' => 'boot'],
        ]))->toThrow(RuntimeException::class, 'could not be started');
    });

    expect($startCalls)->toBe(2)
        ->and(is_resource($startedProcess))->toBeFalse()
        ->and(array_filter($startedPipes, is_resource(...)))->toBeEmpty();
});

it('retains only the newest completed runs without deleting current or locked runs', function (): void {
    $root = $GLOBALS['sqliteFairTestRunDirectory'].'/retention-proof';
    mkdir($root, 0775, true);
    $completed = [
        '20260101.000001.000.aaaaaaaa',
        '20260101.000002.000.bbbbbbbb',
        '20260101.000003.000.cccccccc',
        '20260101.000004.000.dddddddd',
    ];
    foreach ($completed as $directory) {
        mkdir($root.'/'.$directory, 0775);
        touch($root.'/'.$directory.'/database.sqlite');
    }
    $legacyCompleted = $root.'/20250101.000000.000';
    mkdir($legacyCompleted, 0775);
    touch($legacyCompleted.'/database.sqlite');
    $locked = $root.'/20260101.000000.000.eeeeeeee';
    mkdir($locked, 0775);
    $lockedHandle = fopen($locked.'/.active', 'c+');
    if ($lockedHandle === false || ! flock($lockedHandle, LOCK_EX | LOCK_NB)) {
        throw new RuntimeException('The retention proof could not lock its active fixture.');
    }
    $current = $root.'/20260101.000005.000.ffffffff';
    mkdir($current, 0775);

    try {
        pruneTestRunDirectories($root, $current, 2);

        expect(is_dir($current))->toBeTrue()
            ->and(is_dir($locked))->toBeTrue()
            ->and(is_dir($root.'/'.$completed[3]))->toBeTrue()
            ->and(is_dir($root.'/'.$completed[2]))->toBeTrue()
            ->and(is_dir($root.'/'.$completed[1]))->toBeFalse()
            ->and(is_dir($root.'/'.$completed[0]))->toBeFalse()
            ->and(is_dir($legacyCompleted))->toBeFalse();
    } finally {
        flock($lockedHandle, LOCK_UN);
        fclose($lockedHandle);
        removeTestRunDirectory($root);
    }
});
