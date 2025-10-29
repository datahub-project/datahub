package com.linkedin.metadata.kafka.hook;

import com.linkedin.metadata.kafka.listener.EventHook;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;

/**
 * Custom hook which is invoked on receiving a new {@link MetadataChangeLog} event.
 *
 * <p>The semantics of this hook are currently "at most once". That is, the hook will not be called
 * with the same message. In the future, we intend to migrate to "at least once" semantics, meaning
 * that the hook will be responsible for implementing idempotency.
 */
public interface MetadataChangeLogHook extends EventHook<MetadataChangeLog> {

  /** Initialize the hook */
  default MetadataChangeLogHook init(@Nonnull OperationContext systemOperationContext) {
    return this;
  }

  /** Invoke the hook when a MetadataChangeLog is received */
  void invoke(@Nonnull MetadataChangeLog event) throws Exception;

  /**
   * Invoke the hook when a batch of MetadataChangeLog events are received. This method provides
   * better performance by allowing hooks to batch process multiple MetadataChangeLog events
   * together.
   *
   * <p>Default implementation falls back to individual invoke() calls for backward compatibility.
   * Hooks that support batch processing should override this method.
   *
   * @param events Collection of MetadataChangeLog events to process
   * @throws Exception if processing fails
   */
  default void invokeBatch(@Nonnull Collection<MetadataChangeLog> events) throws Exception {
    // Default implementation: process each event individually
    for (MetadataChangeLog event : events) {
      invoke(event);
    }
  }
}
