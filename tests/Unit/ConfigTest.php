<?php

declare(strict_types=1);

it('owns exactly the four package defaults', function () {
    $defaults = require dirname(__DIR__, 2).'/config/sqlite-fair.php';

    expect($defaults)->toHaveCount(4)
        ->and(array_keys($defaults))->toBe(['lock_directory', 'stale_head_seconds', 'wait_strategy', 'debug'])
        ->and($defaults['lock_directory'])->toBe(storage_path('app/private/sqlite-fair'))
        ->and($defaults['stale_head_seconds'])->toBe(10.0)
        ->and($defaults['wait_strategy'])->toBe('auto')
        ->and($defaults['debug'])->toBeFalse();
});
