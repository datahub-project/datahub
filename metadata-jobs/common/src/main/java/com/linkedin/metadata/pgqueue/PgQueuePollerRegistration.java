package com.linkedin.metadata.pgqueue;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * One parallel pgQueue consumer (Kafka-analog: one listener container). Multiple registrations run
 * concurrently; each maps to a consumer group + topic list + handler — no per-pipeline poller
 * class.
 *
 * <p>When {@link #batchPolicy} is non-null the poll worker switches to <em>accumulation mode</em>:
 * messages are buffered across poll iterations and flushed via {@link #flushHandler} when any
 * threshold (count, bytes, age) is reached. When null, each poll result is dispatched immediately
 * via {@link #handler}.
 */
public record PgQueuePollerRegistration(
    @Nonnull String consumerGroupId,
    @Nonnull List<String> topicNames,
    int maxBatch,
    @Nonnull String threadName,
    long emptyPollSleepMillis,
    long missingTopicSleepMillis,
    long errorRecoverySleepMillis,
    @Nonnull PgQueuePollHandler handler,
    @Nullable PgQueueBatchPolicy batchPolicy,
    @Nullable PgQueueBatchFlushHandler flushHandler) {

  /** Convenience constructor for non-batch (immediate dispatch) registrations. */
  public PgQueuePollerRegistration(
      @Nonnull String consumerGroupId,
      @Nonnull List<String> topicNames,
      int maxBatch,
      @Nonnull String threadName,
      long emptyPollSleepMillis,
      long missingTopicSleepMillis,
      long errorRecoverySleepMillis,
      @Nonnull PgQueuePollHandler handler) {
    this(
        consumerGroupId,
        topicNames,
        maxBatch,
        threadName,
        emptyPollSleepMillis,
        missingTopicSleepMillis,
        errorRecoverySleepMillis,
        handler,
        null,
        null);
  }

  public PgQueuePollerRegistration {
    if (maxBatch < 1) {
      throw new IllegalArgumentException("maxBatch must be >= 1");
    }
    if (batchPolicy != null && flushHandler == null) {
      throw new IllegalArgumentException("flushHandler is required when batchPolicy is set");
    }
  }
}
