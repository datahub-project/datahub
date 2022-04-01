package com.datahub.event.hook;

import com.linkedin.mxe.PlatformEvent;
import javax.annotation.Nonnull;

/**
 * Custom hook which is invoked on receiving a new {@link PlatformEvent} event.
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
