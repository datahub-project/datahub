package com.linkedin.metadata.sqlsetup.postgres.usage_events;

import com.linkedin.metadata.config.postgres.PgUsageEventsSetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import javax.annotation.Nonnull;

public final class PgUsageEventsSqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "usage_events";
  public static final String CLASSPATH_LOCATION = "sqlsetup/usage_events/migrations";

  private PgUsageEventsSqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgUsageEventsSetupOptions options, @Nonnull PgUsageEventsSqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    return SqlMigrationModule.builder()
        .migrationNamespace(MIGRATION_NAMESPACE)
        .targetSchema(options.getSchema())
        .classpathLocation(CLASSPATH_LOCATION)
        .ledgerTableName(ledgerTableName)
        .tokenReplacement(
            PgUsageEventsSqlMigrationTokens.TOKEN_PARENT_TABLE, tokens.getParentTableName())
        .build();
  }
}
