/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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

  public DateRange getTrailingWeekDateRange() {
    final DateTime todayEnd = getTomorrowStart().minusMillis(1);
    final DateTime aWeekAgoStart = todayEnd.minusWeeks(1).plusMillis(1);
    return new DateRange(
        String.valueOf(aWeekAgoStart.getMillis()), String.valueOf(todayEnd.getMillis()));
  }

  public DateRange getTrailingMonthDateRange() {
    final DateTime todayEnd = getTomorrowStart().minusMillis(1);
    final DateTime aMonthAgoStart = todayEnd.minusMonths(1).plusMillis(1);
    return new DateRange(
        String.valueOf(aMonthAgoStart.getMillis()), String.valueOf(todayEnd.getMillis()));
  }
}
