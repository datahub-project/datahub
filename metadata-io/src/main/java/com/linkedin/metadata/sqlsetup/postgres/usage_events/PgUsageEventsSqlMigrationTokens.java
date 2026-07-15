package com.linkedin.metadata.sqlsetup.postgres.usage_events;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgUsageEventsSqlMigrationTokens {

  public static final String TOKEN_PARENT_TABLE = "__PGUSAGEEVENTS_PARENT_TABLE__";

  @Nonnull String parentTableName;
}
