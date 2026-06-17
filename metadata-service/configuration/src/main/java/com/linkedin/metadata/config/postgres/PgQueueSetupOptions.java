package com.linkedin.metadata.config.postgres;

import java.util.List;
import lombok.Value;

/**
 * Resolved when {@code postgres.pgQueue.enabled} is true; see {@link
 * PostgresSqlSetupProperties#buildPgQueueOptions()}.
 */
@Value
public class PgQueueSetupOptions {
  /**
   * PostgreSQL schema for pgQueue SqlSetup objects ({@code postgres.pgQueue.schema}); default
   * {@code queue}, not {@code postgres.schema}.
   */
  String schema;

  /**
   * Normalized {@code postgres.pgQueue.tablePrefix}; table names are {@code tablePrefix +
   * "_topic"}, {@code tablePrefix + "_message"}, etc.
   */
  String tablePrefix;

  int topicDefaultPartitionCount;
  int topicDefaultVisibilityTimeoutSeconds;
  String topicDefaultPriorityBands;

  /** 0 = retention-by-age disabled at cluster default. */
  int topicDefaultRetentionMaxAgeSeconds;

  /** 0 = disabled. */
  long topicDefaultMaxRowsPerTopic;

  /** 0 = disabled. */
  long topicDefaultMaxTotalPayloadBytesPerTopic;

  /**
   * MIME string resolved to {@code topic.default_content_type_id}; seeded in {@code
   * *_content_type}.
   */
  String topicDefaultContentTypeMime;

  /** Lower-cased allowlisted interval for pg_partman (message table time partitioning). */
  String partmanPartitionInterval;

  int partmanPremake;

  boolean maintenanceCronEnabled;
  int maintenanceIntervalSeconds;
  int maintenanceBatchDeleteLimit;

  /** When true, topics default to aggressive retention (purge once all consumers have read). */
  boolean topicDefaultAggressiveRetention;

  /**
   * Default same-JVM consumer thread count per topic when not overridden in {@link
   * #resolvedTopicCatalog}; never greater than {@link #topicDefaultPartitionCount} after options
   * are built.
   */
  int topicDefaultConsumerConcurrency;

  /**
   * Per-topic merged defaults for catalog seeding and {@link com.linkedin.metadata.queue}
   * resolution.
   */
  List<PgQueueResolvedTopicCatalogEntry> resolvedTopicCatalog;
}
