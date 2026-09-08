package com.linkedin.metadata.config.postgres;

import lombok.Builder;
import lombok.ToString;
import lombok.Value;
import org.springframework.lang.Nullable;

/**
 * Resolved pgGraph options when {@code postgres.pgGraph.enabled} is true. Table names use {@code
 * tablePrefix} ({@code {prefix}_vertices}, {@code {prefix}_edges}); the SqlSetup ledger is {@code
 * {prefix}_schema_migration}.
 */
@Value
@Builder
@ToString(exclude = "poolPassword")
public class PgGraphSetupOptions {
  String schema;
  String tablePrefix;
  int partitionCount;
  String idHashAlgo;
  int maxEdgeWriteBatchSize;
  @Nullable String poolUrl;
  @Nullable String poolDriver;
  @Nullable String poolUsername;
  @Nullable String poolPassword;
}
