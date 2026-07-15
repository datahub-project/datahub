package com.linkedin.metadata.sqlsetup.postgres.pg_system_metadata;

import com.linkedin.metadata.config.postgres.PgSystemMetadataSetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import javax.annotation.Nonnull;

public final class PgSystemMetadataSqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "pg_system_metadata";
  public static final String CLASSPATH_LOCATION = "sqlsetup/pg_system_metadata/migrations";

  private PgSystemMetadataSqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgSystemMetadataSetupOptions options,
      @Nonnull PgSystemMetadataSqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    return SqlMigrationModule.builder()
        .migrationNamespace(MIGRATION_NAMESPACE)
        .targetSchema(options.getSchema())
        .classpathLocation(CLASSPATH_LOCATION)
        .ledgerTableName(ledgerTableName)
        .tokenReplacement(
            PgSystemMetadataSqlMigrationTokens.TOKEN_TABLE_NAME, tokens.getTableName())
        .build();
  }
}
