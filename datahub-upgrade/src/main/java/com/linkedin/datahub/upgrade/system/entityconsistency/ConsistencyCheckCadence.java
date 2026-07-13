package com.linkedin.datahub.upgrade.system.entityconsistency;

import com.linkedin.metadata.config.ConsistencyCheckSchedule;
import java.time.Clock;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Cadence window helpers for consistency checks on datahub-upgrade system-update runs.
 *
 * <p>A check is due when there is no recorded successful completion at or after the start of the
 * current cadence window (UTC). Cadence advances on any completed full pass (active or dry-run) so
 * restarts within the same window do not re-scan.
 */
public final class ConsistencyCheckCadence {

  public static final String KEY_LAST_COMPLETED_TIME = "lastCompletedTime";
  public static final String CHECK_UPGRADE_ID_PREFIX = "entity-consistency-check-";

  private ConsistencyCheckCadence() {}

  /** Upgrade id used to persist per-check cadence state (stable across config fingerprints). */
  @Nonnull
  public static String checkUpgradeId(@Nonnull String checkId) {
    return CHECK_UPGRADE_ID_PREFIX + checkId;
  }

  /**
   * Start of the current cadence window in epoch millis (UTC).
   *
   * <p>For {@link ConsistencyCheckSchedule#EVERY_RUN}, returns {@link Long#MIN_VALUE} so any prior
   * completion does not satisfy the window (the check is always due unless callers short-circuit).
   */
  public static long windowStartEpochMs(
      @Nonnull ConsistencyCheckSchedule schedule, @Nonnull Clock clock) {
    LocalDate today = Instant.ofEpochMilli(clock.millis()).atZone(ZoneOffset.UTC).toLocalDate();
    return switch (schedule) {
      case EVERY_RUN -> Long.MIN_VALUE;
      case DAILY -> today.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli();
      case WEEKLY -> today
          .with(TemporalAdjusters.previousOrSame(DayOfWeek.MONDAY))
          .atStartOfDay(ZoneOffset.UTC)
          .toInstant()
          .toEpochMilli();
      case MONTHLY -> today
          .with(TemporalAdjusters.firstDayOfMonth())
          .atStartOfDay(ZoneOffset.UTC)
          .toInstant()
          .toEpochMilli();
    };
  }

  /**
   * Whether the check should run given its schedule and last successful completion time.
   *
   * @param schedule check cadence
   * @param lastCompletedEpochMs last recorded completion, or null if never completed
   * @param forceDue when true (e.g. reprocess), always due
   * @param clock clock for determining the current window
   */
  public static boolean isDue(
      @Nonnull ConsistencyCheckSchedule schedule,
      @Nullable Long lastCompletedEpochMs,
      boolean forceDue,
      @Nonnull Clock clock) {
    if (forceDue) {
      return true;
    }
    if (schedule == ConsistencyCheckSchedule.EVERY_RUN) {
      return true;
    }
    if (lastCompletedEpochMs == null) {
      return true;
    }
    long windowStart = windowStartEpochMs(schedule, clock);
    return lastCompletedEpochMs < windowStart;
  }

  /** Parse lastCompletedTime from an upgrade result map. */
  @Nullable
  public static Long parseLastCompleted(@Nullable Map<String, String> resultMap) {
    if (resultMap == null) {
      return null;
    }
    String value = resultMap.get(KEY_LAST_COMPLETED_TIME);
    if (value == null || value.isBlank()) {
      return null;
    }
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
