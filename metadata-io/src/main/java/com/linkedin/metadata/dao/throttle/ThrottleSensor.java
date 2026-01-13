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
}
