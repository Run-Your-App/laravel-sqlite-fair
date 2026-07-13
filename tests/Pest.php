<?php

declare(strict_types=1);

uses(
    Orchestra\Testbench\TestCase::class,
    Illuminate\Foundation\Testing\RefreshDatabase::class,
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
    for ($attempt = 0; $attempt < 1000; $attempt++) {
        $path = $runsDirectory.DIRECTORY_SEPARATOR.testRunDirectoryName();
        if (@mkdir($path, 0775)) {
            return $path;
        }
        usleep(1000);
    }

    throw new RuntimeException('A unique package test run directory could not be created.');
}

$repositoryRoot = dirname(__DIR__, 3);
$GLOBALS['sqliteFairTestRunDirectory'] = createRunDirectory($repositoryRoot.'/storage/framework/testing/runs');
