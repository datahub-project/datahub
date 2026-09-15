package com.linkedin.metadata.config.postgres;

import lombok.Getter;
import lombok.Setter;

/**
 * Optional per-topic settings under {@code postgres.pgQueue.topics.<logicalKey>}; unset fields fall
 * back to {@link PostgresSqlSetupProperties.PgQueue.TopicDefaults}.
 */
@Getter
@Setter
public class PgQueueTopicOverride {

  /** Resolved Kafka/pgQueue topic name (required for catalog upsert). */
  private String topicName;

  private Integer retentionMaxAgeSeconds;
  private Integer partitionCount;
  private String priorityBands;
  private Long maxRowsPerTopic;
  private Long maxTotalPayloadBytesPerTopic;

  /**
   * When true, messages are purged as soon as all registered consumers have read past them. {@code
   * null} inherits {@link
   * PostgresSqlSetupProperties.PgQueue.TopicDefaults#isAggressiveRetention()}.
   */
  private Boolean aggressiveRetention;

  /**
   * Same-JVM pgQueue poller threads for this topic. {@code null} inherits {@link
   * PostgresSqlSetupProperties.PgQueue.TopicDefaults#getConsumerConcurrency()}; non-positive values
   * are normalized to 1 when building the resolved catalog. The stored value is also capped to this
   * topic's {@code partitionCount} (never more threads than partitions).
   */
  private Integer consumerConcurrency;
}
