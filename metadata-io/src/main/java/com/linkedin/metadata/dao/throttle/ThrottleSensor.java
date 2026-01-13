package com.linkedin.metadata.dao.throttle;

import java.util.function.Consumer;
import java.util.function.Function;
import javax.annotation.Nullable;

public interface ThrottleSensor {
  ThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback);

  @Nullable
  default Consumer<Integer> eventRecorder() {
    return null;
  }

  /**
   * Returns whether the system is currently being throttled. Default implementation returns false.
   *
   * @return true if currently throttled, false otherwise
   */
  default boolean getIsThrottled() {
    return false;
  }
}
