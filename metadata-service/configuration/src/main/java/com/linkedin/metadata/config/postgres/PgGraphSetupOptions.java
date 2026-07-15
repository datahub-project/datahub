package com.linkedin.metadata.config.postgres;

import lombok.Value;

/** Resolved when {@code postgres.pgGraph.enabled} is true. */
@Value
public class PgGraphSetupOptions {
  String schema;
  String tablePrefix;
  int partitionCount;
  String idHashAlgo;
  int maxEdgeWriteBatchSize;
}
