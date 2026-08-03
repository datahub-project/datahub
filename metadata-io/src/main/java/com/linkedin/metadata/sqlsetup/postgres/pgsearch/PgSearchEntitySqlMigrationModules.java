package com.linkedin.metadata.sqlsetup.postgres.pgsearch;

import com.linkedin.metadata.config.postgres.PgSearchEntitySetupOptions;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationException;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationModule;
import com.linkedin.metadata.sqlsetup.postgres.migration.SqlMigrationVersionSkip;
import java.io.IOException;
import java.sql.SQLException;
import javax.annotation.Nonnull;

public final class PgSearchEntitySqlMigrationModules {

  public static final String MIGRATION_NAMESPACE = "pgsearch_entity";
  public static final String CLASSPATH_LOCATION = "sqlsetup/pgsearch_entity/migrations";

  private PgSearchEntitySqlMigrationModules() {}

  @Nonnull
  public static SqlMigrationModule from(
      @Nonnull PgSearchEntitySetupOptions options,
      @Nonnull PgSearchEntitySqlMigrationTokens tokens) {
    String ledgerTableName = options.getTablePrefix() + "_schema_migration";
    SqlMigrationModule.Builder builder =
        SqlMigrationModule.builder()
            .migrationNamespace(MIGRATION_NAMESPACE)
            .targetSchema(options.getSchema())
            .classpathLocation(CLASSPATH_LOCATION)
            .ledgerTableName(ledgerTableName)
            .tokenReplacement(
                PgSearchEntitySqlMigrationTokens.TOKEN_PREFIX, tokens.getTablePrefix())
            .tokenReplacement(
                PgSearchEntitySqlMigrationTokens.TOKEN_TIER_TEXT_COLUMNS,
                tokens.getTierTextColumns())
            .tokenReplacement(
                PgSearchEntitySqlMigrationTokens.TOKEN_TIER_TSVECTOR_COLUMNS,
                tokens.getTierTsvectorColumns())
            .tokenReplacement(
                PgSearchEntitySqlMigrationTokens.TOKEN_TIER_EMBEDDING_COLUMNS,
                tokens.getTierEmbeddingColumns())
            .tokenReplacement(
                PgSearchEntitySqlMigrationTokens.TOKEN_TIER_TSVECTOR_INDEXES,
                tokens.getTierTsvectorIndexes());

    if (!options.isVectorEnabled()) {
      builder.preMigrate(
          connection -> {
            try {
              SqlMigrationVersionSkip.skipVersionedScriptIfAbsent(
                  connection,
                  options.getSchema(),
                  ledgerTableName,
                  CLASSPATH_LOCATION,
                  "V001__extensions.sql");
            } catch (SQLException | IOException | SqlMigrationException e) {
              throw new SQLException("Failed to skip vector extension migration", e);
            }
          });
    }

    return builder.build();
  }
}
