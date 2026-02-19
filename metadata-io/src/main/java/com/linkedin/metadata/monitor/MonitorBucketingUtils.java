package com.linkedin.metadata.monitor;

import com.linkedin.common.CronSchedule;
import com.linkedin.monitor.AssertionEvaluationParameters;
import com.linkedin.monitor.AssertionEvaluationSpec;
import com.linkedin.monitor.AssertionMonitor;
import com.linkedin.monitor.AssertionTimeBucketingStrategy;
import com.linkedin.monitor.DatasetFieldAssertionParameters;
import com.linkedin.monitor.DatasetVolumeAssertionParameters;
import com.linkedin.monitor.MonitorInfo;
import com.linkedin.timeseries.CalendarInterval;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Utilities for deriving monitor evaluation schedules from bucketing strategy configuration.
 *
 * <p>When a time bucketing strategy is configured, the evaluation schedule is deterministic: it
 * must fire once per bucket interval, at bucket maturity time, in the bucket's own timezone. This
 * eliminates cross-timezone conversion and DST concerns entirely.
 */
public final class MonitorBucketingUtils {

  private static final String DAILY_CRON_STR_TEMPLATE = "0 0 * * *";
  private static final String WEEKLY_CRON_STR_TEMPLATE = "0 0 * * %d";

  public static final String DEFAULT_CRON = DAILY_CRON_STR_TEMPLATE;
  public static final String DEFAULT_CRON_TIMEZONE = "UTC";

  private MonitorBucketingUtils() {}

  /**
   * Extracts the time bucketing strategy from a MonitorInfo's first assertion evaluation
   * parameters.
   *
   * @return the strategy, or null if no bucketing is configured
   */
  @Nullable
  public static AssertionTimeBucketingStrategy extractTimeBucketingStrategy(
      @Nonnull MonitorInfo monitorInfo) {
    final AssertionMonitor monitor = monitorInfo.getAssertionMonitor();
    if (monitor == null || monitor.getAssertions() == null || monitor.getAssertions().isEmpty()) {
      return null;
    }
    final AssertionEvaluationParameters params = monitor.getAssertions().get(0).getParameters();
    if (params == null) {
      return null;
    }
    return extractTimeBucketingStrategyFromParams(params);
  }

  /**
   * Extracts the time bucketing strategy from evaluation parameters.
   *
   * @return the strategy, or null if no bucketing is configured
   */
  @Nullable
  public static AssertionTimeBucketingStrategy extractTimeBucketingStrategyFromParams(
      @Nonnull AssertionEvaluationParameters parameters) {
    if (parameters.hasDatasetVolumeParameters()) {
      final DatasetVolumeAssertionParameters volumeParams = parameters.getDatasetVolumeParameters();
      if (volumeParams.hasTimeBucketingStrategy()) {
        return volumeParams.getTimeBucketingStrategy();
      }
    }
    if (parameters.hasDatasetFieldParameters()) {
      final DatasetFieldAssertionParameters fieldParams = parameters.getDatasetFieldParameters();
      if (fieldParams.hasTimeBucketingStrategy()) {
        return fieldParams.getTimeBucketingStrategy();
      }
    }
    return null;
  }

  /**
   * Derives a {@link CronSchedule} aligned to bucket maturity for the given bucketing strategy. The
   * cron timezone is set equal to the bucket timezone so there are no cross-timezone or DST
   * concerns.
   *
   * <ul>
   *   <li>DAY bucket: fires daily at midnight in bucket tz ({@code 0 0 * * *})
   *   <li>WEEK bucket: fires once per week on the day the bucket matures (Monday + grace days)
   *   <li>No bucketing (null): returns {@code 0 0 * * *} in UTC
   * </ul>
   *
   * <p>Grace period (always expressed in days) shifts the maturity day for WEEK buckets. For DAY
   * buckets, grace shifts maturity by full days so the cron hour stays at midnight.
   */
  public static CronSchedule deriveScheduleForBucketing(
      @Nullable AssertionTimeBucketingStrategy strategy) {
    if (strategy == null) {
      return new CronSchedule().setCron(DEFAULT_CRON).setTimezone(DEFAULT_CRON_TIMEZONE);
    }

    final String bucketTz =
        (strategy.hasTimezone() && !strategy.getTimezone().isBlank())
            ? strategy.getTimezone()
            : DEFAULT_CRON_TIMEZONE;

    int graceDays = 0;
    if (strategy.hasLateArrivalGracePeriod()) {
      graceDays = strategy.getLateArrivalGracePeriod().getMultiple();
    }

    final CalendarInterval interval = strategy.getBucketInterval().getUnit();
    final String cron;
    if (interval == CalendarInterval.WEEK) {
      // Weeks start on Monday (cron day 1). Grace shifts the maturity day forward.
      final int maturityDay = (1 + graceDays) % 7;
      cron = String.format(WEEKLY_CRON_STR_TEMPLATE, maturityDay);
    } else {
      // DAY (and any future sub-day intervals): fire at midnight in bucket tz
      cron = DAILY_CRON_STR_TEMPLATE;
    }

    return new CronSchedule().setCron(cron).setTimezone(bucketTz);
  }

  /**
   * Resolves the evaluation schedule for a monitor given its evaluation parameters and an optional
   * user-provided schedule.
   *
   * <ul>
   *   <li>Bucketing present: derives the schedule from the strategy (ignores user schedule)
   *   <li>No bucketing, user schedule provided: uses the user schedule as-is
   *   <li>No bucketing, no user schedule: defaults to {@code 0 0 * * *} in UTC
   * </ul>
   */
  @Nonnull
  public static CronSchedule resolveSchedule(
      @Nonnull AssertionEvaluationParameters evalParams,
      @Nullable CronSchedule userProvidedSchedule) {
    final AssertionTimeBucketingStrategy strategy =
        extractTimeBucketingStrategyFromParams(evalParams);
    if (strategy != null) {
      return deriveScheduleForBucketing(strategy);
    }
    if (userProvidedSchedule != null) {
      return userProvidedSchedule;
    }
    return new CronSchedule().setCron(DEFAULT_CRON).setTimezone(DEFAULT_CRON_TIMEZONE);
  }

  /**
   * Overwrites the schedule on a MonitorInfo's first assertion evaluation spec with the
   * bucket-aligned schedule, if bucketing is configured. No-op if no bucketing is present.
   *
   * @return true if the schedule was overwritten
   */
  public static boolean maybeOverrideSchedule(@Nonnull MonitorInfo monitorInfo) {
    final AssertionTimeBucketingStrategy strategy = extractTimeBucketingStrategy(monitorInfo);
    if (strategy == null) {
      return false;
    }
    final CronSchedule derived = deriveScheduleForBucketing(strategy);
    final AssertionEvaluationSpec spec = monitorInfo.getAssertionMonitor().getAssertions().get(0);
    spec.setSchedule(derived);
    return true;
  }
}
