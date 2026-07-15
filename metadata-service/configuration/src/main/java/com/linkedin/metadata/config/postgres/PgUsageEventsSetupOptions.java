package com.linkedin.metadata.config.postgres;

import lombok.Value;

/** Resolved when {@code postgres.pgUsageEvents.enabled} is true. */
@Value
public class PgUsageEventsSetupOptions {
  String schema;
  String tablePrefix;
  String parentTableName;
}
