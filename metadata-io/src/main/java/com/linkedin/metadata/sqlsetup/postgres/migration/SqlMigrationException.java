package com.linkedin.metadata.sqlsetup.postgres.migration;

/** Failure during Postgres SQL migration execution. */
public class SqlMigrationException extends Exception {

  public SqlMigrationException(String message) {
    super(message);
  }

  public SqlMigrationException(String message, Throwable cause) {
    super(message, cause);
  }
}
