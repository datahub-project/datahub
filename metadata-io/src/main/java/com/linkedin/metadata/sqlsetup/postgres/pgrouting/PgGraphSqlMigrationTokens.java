package com.linkedin.metadata.sqlsetup.postgres.pgrouting;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgGraphSqlMigrationTokens {

  public static final String TOKEN_PREFIX = "__PGGRAPH_PREFIX__";

  @Nonnull String tablePrefix;
}
