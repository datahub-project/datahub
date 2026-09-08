package com.linkedin.metadata.sqlsetup.postgres.pganalytics;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgAnalyticsSqlMigrationTokens {

  public static final String TOKEN_PREFIX = "__PGANALYTICS_PREFIX__";
  public static final String TOKEN_PARTMAN_PARENT_EVENT = "__PARTMAN_PARENT_EVENT__";
  public static final String TOKEN_PARTMAN_PARENT_ROLLUP = "__PARTMAN_PARENT_ROLLUP__";
  public static final String TOKEN_PARTMAN_PARENT_DISTINCT = "__PARTMAN_PARENT_DISTINCT__";
  public static final String TOKEN_PARTMAN_INTERVAL = "__PARTMAN_INTERVAL__";
  public static final String TOKEN_PARTMAN_PREMAKE = "__PARTMAN_PREMAKE__";
  public static final String TOKEN_PARTMAN_FORCE_OVERWRITE = "__PARTMAN_FORCE_OVERWRITE__";

  @Nonnull String tablePrefix;
  @Nonnull String partmanParentEvent;
  @Nonnull String partmanParentRollup;
  @Nonnull String partmanParentDistinct;
  @Nonnull String partmanInterval;
  @Nonnull String partmanPremake;
  @Nonnull String partmanForceOverwrite;
}
