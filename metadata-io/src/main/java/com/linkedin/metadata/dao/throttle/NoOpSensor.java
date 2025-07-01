package com.linkedin.metadata.dao.throttle;

import java.util.function.Function;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class NoOpSensor implements ThrottleSensor {
  @Override
  public ThrottleSensor addCallback(Function<ThrottleEvent, ThrottleControl> callback) {
    return this;
  }
}
