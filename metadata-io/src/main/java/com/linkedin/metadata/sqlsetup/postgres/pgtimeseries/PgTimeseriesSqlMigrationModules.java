package com.linkedin.metadata.sqlsetup.postgres.pgtimeseries;

import com.linkedin.metadata.config.postgres.PgTimeseriesSetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import javax.annotation.Nonnull;

public final class PgTimeseriesSqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "pgtimeseries";
  public static final String CLASSPATH_LOCATION = "sqlsetup/pgtimeseries/migrations";

  private PgTimeseriesSqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgTimeseriesSetupOptions options, @Nonnull PgTimeseriesSqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    return SqlMigrationModule.builder()
        .migrationNamespace(MIGRATION_NAMESPACE)
        .targetSchema(options.getSchema())
        .classpathLocation(CLASSPATH_LOCATION)
        .ledgerTableName(ledgerTableName)
        .tokenReplacement(PgTimeseriesSqlMigrationTokens.TOKEN_PREFIX, tokens.getTablePrefix())
        .tokenReplacement(
            PgTimeseriesSqlMigrationTokens.TOKEN_PARTMAN_PARENT, tokens.getPartmanParentQualified())
        .tokenReplacement(
            PgTimeseriesSqlMigrationTokens.TOKEN_PARTMAN_INTERVAL, tokens.getPartmanInterval())
        .tokenReplacement(
            PgTimeseriesSqlMigrationTokens.TOKEN_PARTMAN_PREMAKE, tokens.getPartmanPremake())
        .build();
  }
}
