package com.linkedin.metadata.kafka.hook;

import com.linkedin.metadata.kafka.listener.EventHook;
import com.linkedin.mxe.MetadataChangeLog;
import io.datahubproject.metadata.context.OperationContext;
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
}
