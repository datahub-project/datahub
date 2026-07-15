package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import com.linkedin.metadata.config.postgres.PgQueueSetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import javax.annotation.Nonnull;

/** Builds {@link SqlMigrationModule} for pgQueue SqlSetup. */
public final class PgQueueSqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "pgqueue";
  public static final String CLASSPATH_LOCATION = "sqlsetup/pgqueue/migrations";

  private PgQueueSqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgQueueSetupOptions options, @Nonnull PgQueueSqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    return SqlMigrationModule.builder()
        .migrationNamespace(MIGRATION_NAMESPACE)
        .targetSchema(options.getSchema())
        .classpathLocation(CLASSPATH_LOCATION)
        .ledgerTableName(ledgerTableName)
        .tokenReplacement(PgQueueSqlMigrationTokens.TOKEN_PREFIX, tokens.getTablePrefix())
        .tokenReplacement(PgQueueSqlMigrationTokens.TOKEN_SCHEMA, tokens.getQuotedSchema())
        .tokenReplacement(
            PgQueueSqlMigrationTokens.TOKEN_BATCH_DELETE_LIMIT, tokens.getBatchDeleteLimit())
        .tokenReplacement(
            PgQueueSqlMigrationTokens.TOKEN_PARTMAN_PARENT, tokens.getPartmanParentQualified())
        .tokenReplacement(
            PgQueueSqlMigrationTokens.TOKEN_PARTMAN_INTERVAL, tokens.getPartmanInterval())
        .tokenReplacement(
            PgQueueSqlMigrationTokens.TOKEN_PARTMAN_PREMAKE, tokens.getPartmanPremake())
        .tokenReplacement(
            PgQueueSqlMigrationTokens.TOKEN_RETENTION_PARTMAN_TAIL,
            tokens.getRetentionPartmanTail())
        .build();
  }
}
