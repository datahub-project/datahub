package com.linkedin.metadata.utils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ParseUtils {
  private ParseUtils() {}

  public static Duration parseDuration(int value, String unit) {
    TimeUnit timeUnit = unit != null ? TimeUnit.valueOf(unit.toUpperCase()) : TimeUnit.SECONDS;
    return Duration.of(value, timeUnit.toChronoUnit());
  }
}
