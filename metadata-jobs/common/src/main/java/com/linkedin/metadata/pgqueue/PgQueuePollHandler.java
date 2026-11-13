package com.linkedin.metadata.pgqueue;

import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.util.List;
import javax.annotation.Nonnull;

/** Processing hook for one logical Kafka-style topic batch (same worker loop for all pipelines). */
@FunctionalInterface
public interface PgQueuePollHandler {

  void handleBatch(
      @Nonnull String logicalTopic,
      @Nonnull List<QueueReceivedMessage> messages,
      @Nonnull PgQueuePollContext context);
}
