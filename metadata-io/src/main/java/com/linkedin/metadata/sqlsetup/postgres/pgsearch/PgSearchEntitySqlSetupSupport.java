package com.linkedin.metadata.sqlsetup.postgres.pgsearch;

import com.linkedin.metadata.config.postgres.PostgresSqlSetupProperties;
import javax.annotation.Nonnull;

/** SqlSetup helpers for pgSearch entity tier column DDL fragments. */
public final class PgSearchEntitySqlSetupSupport {

  private PgSearchEntitySqlSetupSupport() {}

  @Nonnull
  public static PgSearchEntitySqlMigrationTokens buildMigrationTokens(
      @Nonnull PostgresSqlSetupProperties properties) {
    var options = properties.buildPgSearchEntityOptions();
    if (options == null) {
      throw new IllegalStateException("pgSearch entity is not enabled");
    }
    int tierCols = options.getTierTsvectorColumnCount();
    boolean vectorEnabled = options.isVectorEnabled();
    int embeddingDims = options.getEmbeddingDimensions();
    String tierTextDdl = buildTierTextColumnDefinitions(tierCols);
    String tierColumnDdl = buildTierTsvectorColumnDefinitions(tierCols);
    String tierEmbeddingCreateDdl =
        vectorEnabled
            ? PostgresSqlSetupProperties.buildTierEmbeddingVectorColumnDefinitionsForCreateTable(
                tierCols, embeddingDims)
            : "";
    String tierIndexDdl = buildTierTsvectorIndexDefinitions(tierCols);
    return PgSearchEntitySqlMigrationTokens.builder()
        .tablePrefix(options.getTablePrefix())
        .tierTextColumns(tierTextDdl)
        .tierTsvectorColumns(tierColumnDdl)
        .tierEmbeddingColumns(tierEmbeddingCreateDdl)
        .tierTsvectorIndexes(tierIndexDdl)
        .build();
  }

  @Nonnull
  public static String buildTierTextColumnDefinitions(int tierColumnCount) {
    if (tierColumnCount < 1) {
      throw new IllegalArgumentException("tierColumnCount must be >= 1");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= tierColumnCount; i++) {
      sb.append("    ")
          .append(PostgresSqlSetupProperties.searchTextTierColumnName(i))
          .append(" TEXT,\n");
    }
    return sb.toString();
  }

  @Nonnull
  public static String buildTierTsvectorColumnDefinitions(int tierColumnCount) {
    if (tierColumnCount < 1) {
      throw new IllegalArgumentException("tierColumnCount must be >= 1");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= tierColumnCount; i++) {
      sb.append("    ")
          .append(PostgresSqlSetupProperties.searchVectorTierColumnName(i))
          .append(" tsvector,\n");
    }
    return sb.toString();
  }

  @Nonnull
  public static String buildTierTsvectorIndexDefinitions(int tierColumnCount) {
    if (tierColumnCount < 1) {
      throw new IllegalArgumentException("tierColumnCount must be >= 1");
    }
    StringBuilder sb = new StringBuilder();
    for (int i = 1; i <= tierColumnCount; i++) {
      String col = PostgresSqlSetupProperties.searchVectorTierColumnName(i);
      sb.append("CREATE INDEX IF NOT EXISTS idx___PGSEARCH_PREFIX___search_row_")
          .append(col)
          .append(" ON __PGSEARCH_PREFIX___search_row USING gin (")
          .append(col)
          .append(");\n\n");
    }
    return sb.toString();
  }
}
