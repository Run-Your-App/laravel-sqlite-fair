<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Exceptions;

/**
 * Reports that fair writer acquisition reached its deadline before business SQL began.
 *
 * Callers may catch this subclass to handle a bounded wait separately from configuration or connection failures.
 * The enclosing operation has not entered its business callback when this exception is raised.
 */
final class FairWaitTimeoutException extends FairSQLiteException {}
