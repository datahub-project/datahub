package com.linkedin.metadata.dao.throttle;

import java.util.function.Function;

public interface ThrottleSensor {
  ThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback);
}
