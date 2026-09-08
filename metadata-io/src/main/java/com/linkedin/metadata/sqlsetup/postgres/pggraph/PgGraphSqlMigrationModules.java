package com.linkedin.metadata.sqlsetup.postgres.pggraph;

import com.linkedin.metadata.config.postgres.PgGraphSetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import javax.annotation.Nonnull;

public final class PgGraphSqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "pggraph";
  public static final String CLASSPATH_LOCATION = "sqlsetup/pggraph/migrations";

  private PgGraphSqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgGraphSetupOptions options, @Nonnull PgGraphSqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    return SqlMigrationModule.builder()
        .migrationNamespace(MIGRATION_NAMESPACE)
        .targetSchema(options.getSchema())
        .classpathLocation(CLASSPATH_LOCATION)
        .ledgerTableName(ledgerTableName)
        .tokenReplacement(PgGraphSqlMigrationTokens.TOKEN_PREFIX, tokens.getTablePrefix())
        .build();
  }
}
