<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Exceptions;

/**
 * Reports that fair writer acquisition expired before business SQL began.
 *
 * Callers may handle a bounded wait separately from configuration or connection
 * failures. The enclosing operation has not entered its business callback when
 * this exception is raised, so the timeout never reports an uncertain write.
 */
final class FairWaitTimeoutException extends FairSQLiteException {}
