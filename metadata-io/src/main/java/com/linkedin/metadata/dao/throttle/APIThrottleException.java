package com.linkedin.metadata.dao.throttle;

import java.util.concurrent.TimeUnit;

public class APIThrottleException extends RuntimeException {
  private final long durationMs;

  public APIThrottleException(long durationMs, String message) {
    super(message);
    this.durationMs = durationMs;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public long getDurationSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(durationMs);
  }
}
