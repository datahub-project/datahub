package com.linkedin.metadata.sqlsetup.postgres.pgtimeseries;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgTimeseriesSqlMigrationTokens {

  public static final String TOKEN_PREFIX = "__PGTIMESERIES_PREFIX__";
  public static final String TOKEN_PARTMAN_PARENT = "__PARTMAN_PARENT_QUALIFIED__";
  public static final String TOKEN_PARTMAN_INTERVAL = "__PARTMAN_INTERVAL__";
  public static final String TOKEN_PARTMAN_PREMAKE = "__PARTMAN_PREMAKE__";

  @Nonnull String tablePrefix;
  @Nonnull String partmanParentQualified;
  @Nonnull String partmanInterval;
  @Nonnull String partmanPremake;
}
