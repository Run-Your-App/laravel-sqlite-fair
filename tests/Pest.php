<?php

declare(strict_types=1);
use Illuminate\Foundation\Testing\RefreshDatabase;
use Orchestra\Testbench\TestCase;

const SQLITE_FAIR_RETAINED_COMPLETED_RUNS = 3;

uses(
    TestCase::class,
    RefreshDatabase::class,
)->in('Unit');

function testRunDirectoryName(): string
{
    $now = DateTimeImmutable::createFromFormat('U.u', sprintf('%.6F', microtime(true)));
    if (! $now instanceof DateTimeImmutable) {
        throw new RuntimeException('The package test run timestamp could not be created.');
    }

    return $now->format('Ymd.His.').mb_substr($now->format('u'), 0, 3);
}

function createRunDirectory(string $runsDirectory): string
{
    if (! is_dir($runsDirectory) && ! mkdir($runsDirectory, 0775, true) && ! is_dir($runsDirectory)) {
        throw new RuntimeException('The package test runs directory could not be created.');
    }

    for ($attempt = 0; $attempt < 10; $attempt++) {
        $path = $runsDirectory.DIRECTORY_SEPARATOR.testRunDirectoryName().'.'.bin2hex(random_bytes(4));
        if (@mkdir($path, 0775)) {
            return $path;
        }
    }

    throw new RuntimeException('A unique package test run directory could not be created.');
}

/**
 * Removes completed package-test runs while preserving current and concurrent runs.
 *
 * Every live test process holds an exclusive lock on its `.active` marker. A directory is eligible for retention only
 * when that marker can be locked nonblockingly, so one test process never removes another process's active workspace.
 *
 * @param  string  $runsDirectory  Package-owned parent containing timestamped run directories.
 * @param  string  $activeRunDirectory  Current process run directory that must never be removed.
 * @param  int  $retainedCompletedRuns  Number of newest unlocked completed runs retained for diagnostics.
 * @return void Eligible completed runs beyond the fixed retention count have been removed best-effort.
 */
function pruneTestRunDirectories(string $runsDirectory, string $activeRunDirectory, int $retainedCompletedRuns): void
{
    if ($retainedCompletedRuns < 0) {
        throw new RuntimeException('The package test run retention must not be negative.');
    }

    $completedRuns = [];
    foreach (scandir($runsDirectory) ?: [] as $entry) {
        if (preg_match('/^\d{8}\.\d{6}\.\d{3}(?:\.[0-9a-f]{8})?$/', $entry) !== 1) {
            continue;
        }
        $path = $runsDirectory.DIRECTORY_SEPARATOR.$entry;
        if ($path === $activeRunDirectory || ! is_dir($path)) {
            continue;
        }

        $marker = @fopen($path.DIRECTORY_SEPARATOR.'.active', 'c+');
        if ($marker === false) {
            continue;
        }
        $available = @flock($marker, LOCK_EX | LOCK_NB);
        if ($available) {
            @flock($marker, LOCK_UN);
            $completedRuns[] = $path;
        }
        fclose($marker);
    }

    rsort($completedRuns, SORT_STRING);
    foreach (array_slice($completedRuns, $retainedCompletedRuns) as $expiredRun) {
        removeTestRunDirectory($expiredRun);
    }
}

/** Removes one package-owned test run without following directory symlinks. */
function removeTestRunDirectory(string $path): void
{
    if (! is_dir($path) || is_link($path)) {
        return;
    }
    foreach (scandir($path) ?: [] as $entry) {
        if ($entry === '.' || $entry === '..') {
            continue;
        }
        $child = $path.DIRECTORY_SEPARATOR.$entry;
        if (is_dir($child) && ! is_link($child)) {
            removeTestRunDirectory($child);
        } else {
            @unlink($child);
        }
    }
    @rmdir($path);
}

/** @return resource Exclusive marker lock held for the complete Pest process lifetime. */
function lockActiveTestRun(string $runDirectory)
{
    $marker = fopen($runDirectory.DIRECTORY_SEPARATOR.'.active', 'c+');
    if ($marker === false || ! flock($marker, LOCK_EX | LOCK_NB)) {
        throw new RuntimeException('The active package test run could not be locked.');
    }

    return $marker;
}

$packageRoot = dirname(__DIR__);
$runsDirectory = $packageRoot.'/.phpunit.cache/runs';
$activeRunDirectory = createRunDirectory($runsDirectory);
$GLOBALS['sqliteFairTestRunLock'] = lockActiveTestRun($activeRunDirectory);
$GLOBALS['sqliteFairTestRunDirectory'] = $activeRunDirectory;
pruneTestRunDirectories($runsDirectory, $activeRunDirectory, SQLITE_FAIR_RETAINED_COMPLETED_RUNS);
