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

  /**
   * Invoke the hook when a MetadataChangeLog is received.
   *
   * @param operationContext per-event operation context. In OSS this is currently the system
   *     context; downstream callers may swap it for a tenant-scoped per-event context later.
   * @param event the MCL payload
   */
  void invoke(@Nonnull OperationContext operationContext, @Nonnull MetadataChangeLog event)
      throws Exception;

  /**
   * Invoke the hook on a batch of MCL events under the batch-level / system context. Default
   * implementation falls back to individual {@link #invoke} calls for backward compatibility. Hooks
   * that benefit from true batch processing should override.
   *
   * @param systemOperationContext batch-level operation context
   * @param events the MCL events to process
   * @throws Exception if processing fails
   */
  default void invokeBatch(
      @Nonnull OperationContext systemOperationContext,
      @Nonnull Collection<MetadataChangeLog> events)
      throws Exception {
    for (MetadataChangeLog event : events) {
      invoke(systemOperationContext, event);
    }
  }
}
