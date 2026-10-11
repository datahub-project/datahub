package com.linkedin.metadata.queue;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import javax.annotation.Nonnull;

/**
 * Transport-neutral metadata queue API (PostgreSQL pgQueue SqlSetup today; other dialects may
 * implement this contract).
 */
public interface MetadataQueueStore {

  @Nonnull
  Optional<QueueTopicMetadata> fetchTopic(@Nonnull String topicName);

  /**
   * Next exclusive {@code enqueue_seq} per partition after the last row in the log. Partitions with
   * no rows resolve to {@code 1}. Keys are {@code 0 .. partitionCount-1}.
   */
  @Nonnull
  Map<Integer, Long> partitionNextExclusiveSeqs(long topicId, int partitionCount);

  /**
   * Maximum {@code enqueue_seq} per partition for a topic. Partitions with no rows resolve to
   * {@code 0}. Keys are {@code 0 .. partitionCount-1}.
   */
  @Nonnull
  default Map<Integer, Long> partitionMaxEnqueueSeqs(long topicId, int partitionCount) {
    throw new UnsupportedOperationException(
        "partitionMaxEnqueueSeqs is not supported by this store");
  }

  /**
   * Partitions where {@code getCommittedOffset(consumerGroup, topicId, p) > maxSeq(topicId, p)}
   * (STUCK_AHEAD).
   */
  @Nonnull
  default List<PartitionOffsetSkew> detectOffsetAheadOfLog(
      @Nonnull String consumerGroup, long topicId, int partitionCount) {
    throw new UnsupportedOperationException(
        "detectOffsetAheadOfLog is not supported by this store");
  }

  /** Minimum {@code enqueue_seq} for rows with {@code enqueued_at >= minEnqueuedAt}, if any. */
  @Nonnull
  OptionalLong minEnqueueSeqAtOrAfter(
      long topicId, int partitionId, @Nonnull Instant minEnqueuedAt);

  /** Minimum {@code enqueue_seq} in the partition log, if any rows exist. */
  @Nonnull
  OptionalLong minEnqueueSeq(long topicId, int partitionId);

  /**
   * Non-locking read of the topic message log for External Events polling. Rows match {@code
   * enqueue_seq >= partitionToMinExclusiveSeq.get(partition)} per partition key present in the map.
   */
  @Nonnull
  List<QueueLogPeekRow> peekTopicLog(
      long topicId, @Nonnull Map<Integer, Long> partitionToMinExclusiveSeq, int limit);

  /** Ensures a topic catalog row exists (insert-if-missing) and returns its surrogate id. */
  long ensureTopic(@Nonnull String topicName, @Nonnull QueueTopicDefaults defaults);

  /**
   * Enqueues with {@link PgQueuePayloadCompression#NONE}; prefer {@link #enqueue(String, String,
   * QueueTopicDefaults, int, byte[], Optional, List, PgQueuePayloadCompression)} when using
   * application compression.
   */
  @Nonnull
  default QueueMessageHandle enqueue(
      @Nonnull String topicName,
      @Nonnull String routingKey,
      @Nonnull QueueTopicDefaults defaults,
      int priority,
      @Nonnull byte[] payload,
      @Nonnull Optional<String> contentType,
      @Nonnull List<QueueMessageHeader> headers) {
    return enqueue(
        topicName,
        routingKey,
        defaults,
        priority,
        payload,
        contentType,
        headers,
        PgQueuePayloadCompression.NONE);
  }

  @Nonnull
  QueueMessageHandle enqueue(
      @Nonnull String topicName,
      @Nonnull String routingKey,
      @Nonnull QueueTopicDefaults defaults,
      int priority,
      @Nonnull byte[] payload,
      @Nonnull Optional<String> contentType,
      @Nonnull List<QueueMessageHeader> headers,
      @Nonnull PgQueuePayloadCompression payloadCompression);

  @Nonnull
  List<QueueMessageHandle> enqueueBatch(
      @Nonnull List<EnqueueBatchItem> items, @Nonnull QueueTopicDefaults defaults);

  /**
   * Kafka-style receive for one consumer group: each group maintains its own committed offset;
   * acquires a lease row in {@code *_message_group_lease} so parallel groups do not exclude each
   * other on the same payload row.
   */
  @Nonnull
  default List<QueueReceivedMessage> receiveBatchForGroup(
      @Nonnull String consumerGroup,
      long topicId,
      @Nonnull List<Integer> partitionIds,
      @Nonnull String lockOwner,
      @Nonnull Duration visibilityTimeout,
      int maxMessages) {
    throw new UnsupportedOperationException("receiveBatchForGroup is not supported by this store");
  }

  /** Release group leases and optionally advance this consumer group's committed offset. */
  default int commitForGroup(
      @Nonnull String consumerGroup,
      @Nonnull List<QueueMessageHandle> handles,
      boolean updateConsumerOffset) {
    throw new UnsupportedOperationException("commitForGroup is not supported by this store");
  }

  default int extendVisibilityForGroup(
      @Nonnull String consumerGroup,
      @Nonnull List<QueueMessageHandle> handles,
      @Nonnull String lockOwner,
      @Nonnull Duration extendBy) {
    throw new UnsupportedOperationException(
        "extendVisibilityForGroup is not supported by this store");
  }

  /**
   * Committed Kafka-style offset for a consumer group on a partition (exclusive floor used by
   * {@link #receiveBatchForGroup}); {@code 0} when no row exists.
   */
  default long getCommittedOffset(@Nonnull String consumerGroup, long topicId, int partitionId) {
    throw new UnsupportedOperationException("getCommittedOffset is not supported by this store");
  }

  /**
   * Registers (or heartbeats) a consumer group for a topic so aggressive retention can track which
   * consumers must read a message before it can be purged.
   */
  default void registerConsumer(@Nonnull String consumerGroup, long topicId) {
    throw new UnsupportedOperationException("registerConsumer is not supported by this store");
  }

  /** Returns all registered consumer groups for a topic. */
  @Nonnull
  default List<ConsumerRegistrationRow> listRegisteredConsumers(long topicId) {
    throw new UnsupportedOperationException(
        "listRegisteredConsumers is not supported by this store");
  }

  /** Removes a consumer group registration; returns true if a row was deleted. */
  default boolean unregisterConsumer(@Nonnull String consumerGroup, long topicId) {
    throw new UnsupportedOperationException("unregisterConsumer is not supported by this store");
  }

  /**
   * Resets committed consumer offsets to the current log end ({@code MAX(enqueue_seq)} per
   * partition). By default only updates STUCK_AHEAD cells ({@code offset_value &gt; maxSeq}).
   */
  @Nonnull
  default ConsumerOffsetResetReport resetConsumerOffsets(@Nonnull ConsumerOffsetResetSpec spec) {
    throw new UnsupportedOperationException("resetConsumerOffsets is not supported by this store");
  }

  /**
   * Runs topic message retention (age / row and byte caps / aggressive purge). Implementations must
   * preserve the per-partition sequence anchor row ({@code MAX(enqueue_seq)}) so allocation stays
   * monotonic.
   */
  default void applyRetention() {
    throw new UnsupportedOperationException("applyRetention is not supported by this store");
  }
}
