package com.linkedin.datahub.graphql.util;

import com.linkedin.datahub.graphql.generated.DateRange;
import org.joda.time.DateTime;
import org.joda.time.DateTimeConstants;

public class DateUtil {
  public DateTime getNow() {
    return DateTime.now();
  }

  public DateTime getStartOfNextWeek() {
    return setTimeToZero(getNow().withDayOfWeek(DateTimeConstants.SUNDAY).plusDays(1));
  }

  public DateTime getStartOfThisMonth() {
    return setTimeToZero(getNow().withDayOfMonth(1));
  }

  public DateTime getStartOfNextMonth() {
    return setTimeToZero(getNow().withDayOfMonth(1).plusMonths(1));
  }

  public DateTime setTimeToZero(DateTime input) {
    return input.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfDay(0);
  }

  public DateTime getTomorrowStart() {
    return setTimeToZero(getNow().plusDays(1));
  }

  /**
   * Trailing week as a half-open range {@code [start, end)} ending at tomorrow UTC midnight so
   * analytics backends (ES {@code .lt(end)}, Postgres {@code event_time < end}) include all of
   * today and stay grain-aligned for rollups.
   */
  public DateRange getTrailingWeekDateRange() {
    final DateTime endExclusive = getTomorrowStart();
    final DateTime start = endExclusive.minusWeeks(1);
    return new DateRange(
        String.valueOf(start.getMillis()), String.valueOf(endExclusive.getMillis()));
  }

  /**
   * Trailing month as a half-open range {@code [start, end)} ending at tomorrow UTC midnight (same
   * exclusive-end contract as {@link #getTrailingWeekDateRange()}).
   */
  public DateRange getTrailingMonthDateRange() {
    final DateTime endExclusive = getTomorrowStart();
    final DateTime start = endExclusive.minusMonths(1);
    return new DateRange(
        String.valueOf(start.getMillis()), String.valueOf(endExclusive.getMillis()));
  }
}
