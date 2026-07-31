package com.linkedin.metadata.config.postgres;

import lombok.Value;

/** Merged catalog row for SqlSetup UPSERT and runtime topic defaults resolution. */
@Value
public class PgQueueResolvedTopicCatalogEntry {

  /** YAML map key (e.g. metadataChangeLogTimeseries). */
  String logicalKey;

  String topicName;
  int partitionCount;
  String priorityBands;
  int retentionMaxAgeSeconds;
  long maxRowsPerTopic;
  long maxTotalPayloadBytesPerTopic;

  /** When true, messages are purged as soon as all registered consumers have read past them. */
  boolean aggressiveRetention;

  /**
   * Same-JVM consumer threads for this topic (partition assignment {@code p % c == threadIndex});
   * never greater than {@link #partitionCount}. Cross-JVM lease deduplication is unchanged.
   */
  int consumerConcurrency;
}
