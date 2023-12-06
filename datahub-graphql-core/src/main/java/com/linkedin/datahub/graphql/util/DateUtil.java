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

  public DateTime getStartOfNextMonth() {
    return setTimeToZero(getNow().withDayOfMonth(1).plusMonths(1));
  }

  public DateTime setTimeToZero(DateTime input) {
    return input.withHourOfDay(0).withMinuteOfHour(0).withSecondOfMinute(0).withMillisOfDay(0);
  }

  public DateTime getTomorrowStart() {
    return setTimeToZero(getNow().plusDays(1));
  }

  public DateRange getTrailingWeekDateRange() {
    final DateTime todayEnd = getTomorrowStart().minusMillis(1);
    final DateTime aWeekAgoStart = todayEnd.minusWeeks(1).plusMillis(1);
    return new DateRange(
        String.valueOf(aWeekAgoStart.getMillis()), String.valueOf(todayEnd.getMillis()));
  }
}
