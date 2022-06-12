package com.datahub.event.hook;

import com.linkedin.mxe.PlatformEvent;
import javax.annotation.Nonnull;

/**
 * Custom hook which is invoked on receiving a new {@link PlatformEvent} event.
 *
 * The semantics of this hook are currently "at most once". That is, the hook will not be called
 * with the same message. In the future, we intend to migrate to "at least once" semantics, meaning
 * that the hook will be responsible for implementing idempotency.
 */
public interface PlatformEventHook {

  /**
   * Initialize the hook
   */
  default void init() { }

  /**
   * Invoke the hook when a PlatformEvent is received
   */
  void invoke(@Nonnull PlatformEvent event);

}
