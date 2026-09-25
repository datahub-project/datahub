package com.linkedin.metadata.pgqueue;

import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Callback invoked when a {@link PgQueueBatchAccumulator} flushes. Receives the full accumulated
 * batch across one or more poll iterations.
 */
@FunctionalInterface
public interface PgQueueBatchFlushHandler {

  void flush(
      @Nonnull String logicalTopic,
      @Nonnull List<QueueReceivedMessage> batch,
      @Nonnull PgQueuePollContext context);
}
