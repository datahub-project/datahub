package com.linkedin.metadata.sqlsetup.postgres.pganalytics;

import com.linkedin.metadata.config.postgres.PgAnalyticsStoreOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import javax.annotation.Nonnull;

public final class PgAnalyticsSqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "pganalytics";
  public static final String CLASSPATH_LOCATION = "sqlsetup/pganalytics/migrations";

  private PgAnalyticsSqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgAnalyticsStoreOptions options, @Nonnull PgAnalyticsSqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    return SqlMigrationModule.builder()
        .migrationNamespace(MIGRATION_NAMESPACE)
        .targetSchema(options.getSchema())
        .classpathLocation(CLASSPATH_LOCATION)
        .ledgerTableName(ledgerTableName)
        .tokenReplacement(PgAnalyticsSqlMigrationTokens.TOKEN_PREFIX, tokens.getTablePrefix())
        .tokenReplacement(
            PgAnalyticsSqlMigrationTokens.TOKEN_PARTMAN_PARENT_EVENT,
            tokens.getPartmanParentEvent())
        .tokenReplacement(
            PgAnalyticsSqlMigrationTokens.TOKEN_PARTMAN_PARENT_ROLLUP,
            tokens.getPartmanParentRollup())
        .tokenReplacement(
            PgAnalyticsSqlMigrationTokens.TOKEN_PARTMAN_PARENT_DISTINCT,
            tokens.getPartmanParentDistinct())
        .tokenReplacement(
            PgAnalyticsSqlMigrationTokens.TOKEN_PARTMAN_INTERVAL, tokens.getPartmanInterval())
        .tokenReplacement(
            PgAnalyticsSqlMigrationTokens.TOKEN_PARTMAN_PREMAKE, tokens.getPartmanPremake())
        .tokenReplacement(
            PgAnalyticsSqlMigrationTokens.TOKEN_PARTMAN_FORCE_OVERWRITE,
            tokens.getPartmanForceOverwrite())
        .build();
  }
}
