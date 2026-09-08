package com.linkedin.metadata.sqlsetup.postgres.pgsystemmetadata;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class PgSystemMetadataSqlMigrationTokens {

  public static final String TOKEN_TABLE_NAME = "__PGSYSTEMMETADATA_TABLE__";

  @Nonnull String tableName;
}
