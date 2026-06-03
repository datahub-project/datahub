package com.linkedin.metadata.event;

import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Publishes DataHub usage / analytics JSON events without exposing Kafka types. */
public interface UsageEventPublisher {

  void setWritable(boolean writable);

  /**
   * Publish a usage event to the given topic.
   *
   * @return a future completed when the broker acknowledges the send (Kafka), or completed
   *     immediately when dropped (e.g. read-only mode).
   */
  @Nonnull
  Future<?> publish(@Nonnull String topic, @Nullable String key, @Nonnull String payload);

  void flush();
}
