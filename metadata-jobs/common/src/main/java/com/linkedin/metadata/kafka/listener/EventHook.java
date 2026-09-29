package com.linkedin.metadata.kafka.listener;

import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

public interface EventHook<E> {
  default EventHook<E> init(@Nonnull OperationContext systemOperationContext) {
    return this;
  }

  /**
   * Suffix for the consumer group
   *
   * @return suffix
   */
  @Nonnull
  default String getConsumerGroupSuffix() {
    return "";
  }

  /**
   * Return whether the hook is enabled or not. If not enabled, the below invoke method is not
   * triggered
   */
  boolean isEnabled();

  /** Invoke the hook when an event is received. */
  void invoke(@Nonnull OperationContext operationContext, @Nonnull E event) throws Exception;

  /**
   * Controls hook execution ordering
   *
   * @return order to execute
   */
  default int executionOrder() {
    return 100;
  }
}
