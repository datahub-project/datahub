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
}
