package com.linkedin.metadata.config.postgres;

import lombok.Value;

/** Resolved when {@code postgres.pgSystemMetadata.enabled} is true. */
@Value
public class PgSystemMetadataSetupOptions {
  String schema;
  String tablePrefix;
  String tableName;
}
