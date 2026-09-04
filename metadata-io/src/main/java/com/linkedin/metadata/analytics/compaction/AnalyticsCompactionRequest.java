package com.linkedin.metadata.analytics.compaction;

import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Value;

/** Store-agnostic compact budgets and optional scan horizons for a single invocation. */
@Value
@Builder
public class AnalyticsCompactionRequest {
  public static final int DEFAULT_MAX_HOURS_TO_SEAL = 6;
  public static final int DEFAULT_MAX_DAYS_TO_COMPACT = 2;
  public static final int DEFAULT_MAX_MONTHS_TO_COMPACT = 1;
  public static final long DEFAULT_MAX_WALL_CLOCK_MILLIS = 30_000L;
  public static final int DEFAULT_HOUR_LOOKBACK_HOURS = 72;
  public static final int DEFAULT_DAY_LOOKBACK_DAYS = 14;
  public static final int DEFAULT_MONTH_LOOKBACK_MONTHS = 3;

  @Builder.Default int maxHoursToSeal = DEFAULT_MAX_HOURS_TO_SEAL;
  @Builder.Default int maxDaysToCompact = DEFAULT_MAX_DAYS_TO_COMPACT;
  @Builder.Default int maxMonthsToCompact = DEFAULT_MAX_MONTHS_TO_COMPACT;
  @Builder.Default long maxWallClockMillis = DEFAULT_MAX_WALL_CLOCK_MILLIS;

  /** How far back to scan for sealable hours (steady-state default 72). */
  @Builder.Default int hourLookbackHours = DEFAULT_HOUR_LOOKBACK_HOURS;

  /** How far back to scan for hour→day work (steady-state default 14). */
  @Builder.Default int dayLookbackDays = DEFAULT_DAY_LOOKBACK_DAYS;

  /** How far back to scan for day→month work (steady-state default 3). */
  @Builder.Default int monthLookbackMonths = DEFAULT_MONTH_LOOKBACK_MONTHS;

  @Nonnull
  public static AnalyticsCompactionRequest defaults() {
    return AnalyticsCompactionRequest.builder().build();
  }
}
