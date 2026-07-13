<?php

declare(strict_types=1);

namespace RunYourApp\LaravelSqliteFair\Exceptions;

use RuntimeException;

/**
 * Represents a failure in the Fair SQLite writer lifecycle.
 *
 * The package throws this base exception for invalid driver configuration, unavailable coordination state, and
 * connection identities that cannot continue safely. Callers may catch it when every Fair SQLite failure has the
 * same application-level outcome, while narrower subclasses preserve failures that need distinct handling.
 */
class FairSQLiteException extends RuntimeException {}
