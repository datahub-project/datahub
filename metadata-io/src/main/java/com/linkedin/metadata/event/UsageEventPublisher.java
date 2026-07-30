package com.linkedin.metadata.event;

import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Publishes DataHub usage / analytics JSON events without exposing Kafka types. */
public interface UsageEventPublisher {

  void setWritable(boolean writable);

  /**
   * Publish a usage event to the given topic.
   *
   * @param opContext operation context used for outbound header enrichment (may be a system / async
   *     context without request enrichment)
   * @return a future completed when the broker acknowledges the send (Kafka), or completed
   *     immediately when dropped (e.g. read-only mode).
   */
  @Nonnull
  Future<?> publish(
      @Nonnull OperationContext opContext,
      @Nonnull String topic,
      @Nullable String key,
      @Nonnull String payload);

  void flush();
}
