package com.linkedin.metadata.dao.throttle;

import com.linkedin.metadata.throttle.ThrottleMechanismType;
import com.linkedin.metadata.throttle.ThrottleResponseSource;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class APIThrottleException extends RuntimeException {
  private final long durationMs;
  @Nullable private final String ruleId;
  @Nonnull private final ThrottleMechanismType mechanismType;
  @Nonnull private final ThrottleResponseSource source;

  public APIThrottleException(long durationMs, @Nonnull String message) {
    this(
        durationMs,
        message,
        null,
        ThrottleMechanismType.INGEST,
        ThrottleResponseSource.METADATA_WRITE);
  }

  public APIThrottleException(
      long durationMs,
      @Nonnull String message,
      @Nullable String ruleId,
      @Nonnull ThrottleMechanismType mechanismType,
      @Nonnull ThrottleResponseSource source) {
    super(message);
    this.durationMs = durationMs;
    this.ruleId = ruleId;
    this.mechanismType = mechanismType;
    this.source = source;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public long getDurationSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(durationMs);
  }

  @Nullable
  public String getRuleId() {
    return ruleId;
  }

  @Nonnull
  public ThrottleMechanismType getMechanismType() {
    return mechanismType;
  }

  @Nonnull
  public ThrottleResponseSource getSource() {
    return source;
  }
}
