package com.linkedin.metadata.event;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;

/** Drops usage/analytics events when messaging transport is neither Kafka nor pgQueue. */
@Slf4j
public final class NoOpUsageEventPublisher implements UsageEventPublisher {

  @Override
  public void setWritable(boolean writable) {
    log.debug("NoOpUsageEventPublisher: setWritable {}", writable);
  }

  @Nonnull
  @Override
  public Future<?> publish(@Nonnull String topic, @Nullable String key, @Nonnull String payload) {
    log.debug("NoOpUsageEventPublisher: skip publish topic={}", topic);
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public void flush() {
    log.debug("NoOpUsageEventPublisher: flush");
  }
}
