package com.linkedin.metadata.sqlsetup.postgres.migration;

/** Flyway-style migration script category. */
public enum SqlMigrationType {
  VERSIONED,
  REPEATABLE
}
