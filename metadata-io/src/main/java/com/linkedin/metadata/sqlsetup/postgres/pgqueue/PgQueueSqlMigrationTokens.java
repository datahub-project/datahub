package com.linkedin.metadata.sqlsetup.postgres.pgqueue;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Runtime token values for pgQueue classpath SQL migrations. */
@Value
@Builder
public class PgQueueSqlMigrationTokens {

  public static final String TOKEN_PREFIX = "__PGQUEUE_PREFIX__";
  public static final String TOKEN_SCHEMA = "__PGQUEUE_SCHEMA__";
  public static final String TOKEN_BATCH_DELETE_LIMIT = "__BATCH_DELETE_LIMIT__";
  public static final String TOKEN_PARTMAN_PARENT = "__PARTMAN_PARENT_QUALIFIED__";
  public static final String TOKEN_PARTMAN_INTERVAL = "__PARTMAN_INTERVAL__";
  public static final String TOKEN_PARTMAN_PREMAKE = "__PARTMAN_PREMAKE__";
  public static final String TOKEN_RETENTION_PARTMAN_TAIL =
      "__PGQUEUE_APPLY_RETENTION_PARTMAN_TAIL__";

  @Nonnull String quotedSchema;
  @Nonnull String tablePrefix;
  @Nonnull String batchDeleteLimit;
  @Nonnull String partmanParentQualified;
  @Nonnull String partmanInterval;
  @Nonnull String partmanPremake;
  @Nonnull String retentionPartmanTail;
}
