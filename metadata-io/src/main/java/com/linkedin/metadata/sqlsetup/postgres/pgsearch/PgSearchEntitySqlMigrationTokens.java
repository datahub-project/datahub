package com.linkedin.metadata.sqlsetup.postgres.pgsearch;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgSearchEntitySqlMigrationTokens {

  public static final String TOKEN_PREFIX = "__PGSEARCH_PREFIX__";
  public static final String TOKEN_TIER_TEXT_COLUMNS = "__PGSEARCH_TIER_TEXT_COLUMNS__";
  public static final String TOKEN_TIER_TSVECTOR_COLUMNS = "__PGSEARCH_TIER_TSVECTOR_COLUMNS__";
  public static final String TOKEN_TIER_EMBEDDING_COLUMNS =
      "__PGSEARCH_TIER_EMBEDDING_VECTOR_COLUMNS__";
  public static final String TOKEN_TIER_TSVECTOR_INDEXES = "__PGSEARCH_TIER_TSVECTOR_INDEXES__";

  @Nonnull String tablePrefix;
  @Nonnull String tierTextColumns;
  @Nonnull String tierTsvectorColumns;
  @Nonnull String tierEmbeddingColumns;
  @Nonnull String tierTsvectorIndexes;
}
