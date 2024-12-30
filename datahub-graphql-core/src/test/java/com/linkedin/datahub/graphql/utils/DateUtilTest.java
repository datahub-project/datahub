package com.linkedin.datahub.graphql.utils;

import static org.testng.AssertJUnit.assertEquals;

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

  // validates logic to display correct dates in MAU chart
  @Test
  public void testDateAdjustmentsForMonth() {
    DateUtil dateUtil = Mockito.spy(DateUtil.class);

    Mockito.when(dateUtil.getNow()).thenReturn(new DateTime(2024, 11, 15, 0, 0, 0));

    // start date should be next month minus a day
    // but we want to display Dec 1 instead of Nov 30, so add a day and verify it's Dec
    DateTime startOfNextMonthMinus12 = dateUtil.getStartOfNextMonth().minusMonths(12);
    DateTime adjustedStart = startOfNextMonthMinus12.minusMillis(1).plusDays(1);
    assertEquals(12, adjustedStart.getMonthOfYear()); // Verify it is December
    assertEquals(2023, adjustedStart.getYear()); // Verify it is 2023

    // verify that the end date displays correctly
    // the chart will display Oct 1 as the last month because we don't show current month
    DateTime startOfThisMonth = dateUtil.getStartOfThisMonth();
    DateTime adjustedEnd = startOfThisMonth.minusMillis(1).plusDays(1);
    assertEquals(11, adjustedEnd.getMonthOfYear()); // Verify it is November
    assertEquals(2024, adjustedEnd.getYear()); // Verify it is 2024
  }
}
