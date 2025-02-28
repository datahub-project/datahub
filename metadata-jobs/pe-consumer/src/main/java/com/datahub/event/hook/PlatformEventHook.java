package com.datahub.event.hook;

import com.linkedin.mxe.PlatformEvent;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/**
 * Custom hook which is invoked on receiving a new {@link PlatformEvent} event.
 *
 * <p>The semantics of this hook are currently "at most once". That is, the hook will not be called
 * with the same message. In the future, we intend to migrate to "at least once" semantics, meaning
 * that the hook will be responsible for implementing idempotency.
 */
public interface PlatformEventHook {

  /** Initialize the hook */
  default void init() {}

  /**
   * Return whether the hook is enabled or not. If not enabled, the below invoke method is not
   * triggered
   */
  boolean isEnabled();

  /** Invoke the hook when a PlatformEvent is received */
  void invoke(@Nonnull OperationContext opContext, @Nonnull PlatformEvent event);
}
