package com.linkedin.metadata.config.postgres;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.springframework.lang.Nullable;

/**
 * Resolved pgSystemMetadata options when {@code postgres.pgSystemMetadata.enabled} is true. The
 * data table uses the ES-shaped {@code tableName}; {@code tablePrefix} names only the SqlSetup
 * migration ledger ({@code {prefix}_schema_migration}).
 */
@Value
@Builder
@ToString(exclude = "poolPassword")
public class PgSystemMetadataSetupOptions {
  String schema;
  String tablePrefix;
  String tableName;
  @Nullable String poolUrl;
  @Nullable String poolDriver;
  @Nullable String poolUsername;
  @Nullable String poolPassword;

  public String qualifiedTable() {
    return schema + "." + tableName;
  }
}
