package com.linkedin.metadata.usage.flush;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import javax.annotation.Nonnull;

/** Calendar-grid helpers for optional usage flush window alignment. */
public final class UsageFlushBoundaryUtils {

  private UsageFlushBoundaryUtils() {}

  /**
   * Floor {@code instant} to the containing alignment period in {@code zone}.
   *
   * <p>Invariant for a well-formed batch: {@code alignDown(windowStart) == alignDown(windowEnd -
   * 1ms)}.
   */
  @Nonnull
  public static Instant alignDown(
      @Nonnull Instant instant, @Nonnull Duration period, @Nonnull ZoneId zone) {
    requirePositivePeriod(period);
    long periodSeconds = period.getSeconds();
    ZonedDateTime zdt = instant.atZone(zone);
    if (periodSeconds == ChronoUnit.HOURS.getDuration().getSeconds()) {
      return zdt.truncatedTo(ChronoUnit.HOURS).toInstant();
    }
    if (periodSeconds == ChronoUnit.DAYS.getDuration().getSeconds()) {
      return zdt.truncatedTo(ChronoUnit.DAYS).toInstant();
    }
    if (periodSeconds < ChronoUnit.HOURS.getDuration().getSeconds()
        && ChronoUnit.HOURS.getDuration().getSeconds() % periodSeconds == 0) {
      int periodMinutes = (int) (periodSeconds / 60);
      int alignedMinute = (zdt.getMinute() / periodMinutes) * periodMinutes;
      return zdt.withMinute(alignedMinute).withSecond(0).withNano(0).toInstant();
    }

    ZonedDateTime dayStart = zdt.truncatedTo(ChronoUnit.DAYS);
    long periodMillis = period.toMillis();
    long millisSinceDayStart = Duration.between(dayStart.toInstant(), instant).toMillis();
    long alignedMillisSinceDayStart = (millisSinceDayStart / periodMillis) * periodMillis;
    return dayStart.plus(alignedMillisSinceDayStart, ChronoUnit.MILLIS).toInstant();
  }

  /** Start of the next alignment period strictly after {@code instant}. */
  @Nonnull
  public static Instant nextBoundary(
      @Nonnull Instant instant, @Nonnull Duration period, @Nonnull ZoneId zone) {
    return alignDown(instant, period, zone).plus(period);
  }

  /** Whether {@code [start, end)} spans more than one alignment bucket. */
  public static boolean crossesBoundary(
      @Nonnull Instant start,
      @Nonnull Instant end,
      @Nonnull Duration period,
      @Nonnull ZoneId zone) {
    if (!end.isAfter(start)) {
      return false;
    }
    Instant endInclusive = end.minusMillis(1);
    return !alignDown(start, period, zone).equals(alignDown(endInclusive, period, zone));
  }

  private static void requirePositivePeriod(@Nonnull Duration period) {
    if (period.isZero() || period.isNegative()) {
      throw new IllegalArgumentException("alignment period must be positive");
    }
  }
}
