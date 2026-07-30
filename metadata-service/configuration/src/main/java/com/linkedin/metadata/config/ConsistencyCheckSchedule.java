package com.linkedin.metadata.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Cadence for when a consistency check should re-run on datahub-upgrade system-update.
 *
 * <p>Scheduling applies only to non-blocking upgrade runs, not OpenAPI.
 */
public enum ConsistencyCheckSchedule {
  EVERY_RUN,
  DAILY,
  WEEKLY,
  MONTHLY;

  @JsonCreator
  @Nonnull
  public static ConsistencyCheckSchedule fromString(@Nullable String value) {
    if (value == null || value.isBlank()) {
      return EVERY_RUN;
    }
    String normalized = value.trim().toLowerCase().replace('_', '-');
    return switch (normalized) {
      case "every-run", "everyrun" -> EVERY_RUN;
      case "daily" -> DAILY;
      case "weekly" -> WEEKLY;
      case "monthly" -> MONTHLY;
      default -> throw new IllegalArgumentException(
          "Unknown consistency check schedule '"
              + value
              + "'. Expected: every-run, daily, weekly, or monthly");
    };
  }
}
