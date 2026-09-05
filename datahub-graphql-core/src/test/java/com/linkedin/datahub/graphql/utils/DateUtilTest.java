package com.linkedin.datahub.graphql.utils;

import static org.testng.AssertJUnit.assertEquals;

import com.linkedin.datahub.graphql.generated.DateRange;
import com.linkedin.datahub.graphql.util.DateUtil;
import org.joda.time.DateTime;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DateUtilTest {

  private DateTime setTimeParts(int dayOfMonth, boolean zeroTime) {
    DateTime result = new DateTime().withDate(2023, 1, dayOfMonth);
    if (zeroTime) {
      return new DateUtil().setTimeToZero(result);
    }
    return result.withHourOfDay(1).withMinuteOfHour(2).withSecondOfMinute(3).withMillisOfSecond(4);
  }

  private void assertEqualStartOfNextWeek(DateUtil dateUtil, int dayOfMonth) {
    assertEquals(
        setTimeParts(dayOfMonth, true).getMillis(), dateUtil.getStartOfNextWeek().getMillis());
  }

  @Test
  public void testStartOfNextWeek() {
    DateUtil dateUtil = Mockito.spy(DateUtil.class);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(2, false));
    assertEqualStartOfNextWeek(dateUtil, 9);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(3, false));
    assertEqualStartOfNextWeek(dateUtil, 9);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(4, false));
    assertEqualStartOfNextWeek(dateUtil, 9);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(5, false));
    assertEqualStartOfNextWeek(dateUtil, 9);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(6, false));
    assertEqualStartOfNextWeek(dateUtil, 9);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(7, false));
    assertEqualStartOfNextWeek(dateUtil, 9);

    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(8, false));
    assertEqualStartOfNextWeek(dateUtil, 9);
  }

  @Test
  public void testTrailingRangesUseExclusiveMidnightEnds() {
    DateUtil dateUtil = Mockito.spy(DateUtil.class);
    // Wednesday 2023-01-04 01:02:03.004 → tomorrow start 2023-01-05 00:00:00.000
    Mockito.when(dateUtil.getNow()).thenReturn(setTimeParts(4, false));
    DateTime tomorrow = dateUtil.getTomorrowStart();

    DateRange week = dateUtil.getTrailingWeekDateRange();
    assertEquals(tomorrow.minusWeeks(1).getMillis(), Long.parseLong(week.getStart()));
    assertEquals(tomorrow.getMillis(), Long.parseLong(week.getEnd()));

    DateRange month = dateUtil.getTrailingMonthDateRange();
    assertEquals(tomorrow.minusMonths(1).getMillis(), Long.parseLong(month.getStart()));
    assertEquals(tomorrow.getMillis(), Long.parseLong(month.getEnd()));
  }
}
