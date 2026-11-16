package com.linkedin.datahub.graphql.types.common.mappers.util;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.TimeWindow;
import com.linkedin.timeseries.TimeWindowSize;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.testng.annotations.Test;

public class TimeWindowUtilsTest {

  // 2024-01-31T00:00:00Z: adding a month exercises end-of-month clamping, adding a year crosses
  // the leap day.
  private static final long START = 1706659200000L;

  private static TimeWindow window(CalendarInterval unit, int multiple) {
    return new TimeWindow()
        .setStartTimeMillis(START)
        .setLength(new TimeWindowSize().setUnit(unit).setMultiple(multiple));
  }

  /** Expected end computed independently with java.time in the JVM default timezone. */
  private static long expected(java.util.function.UnaryOperator<ZonedDateTime> plus) {
    return plus.apply(ZonedDateTime.ofInstant(Instant.ofEpochMilli(START), ZoneId.systemDefault()))
        .toInstant()
        .toEpochMilli();
  }

  @Test
  public void testFixedUnits() {
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.SECOND, 30)),
        (Long) (START + 30_000L));
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.MINUTE, 5)),
        (Long) (START + 5 * 60_000L));
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.HOUR, 3)),
        (Long) expected(t -> t.plusHours(3)));
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.DAY, 2)),
        (Long) expected(t -> t.plusDays(2)));
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.WEEK, 1)),
        (Long) expected(t -> t.plusWeeks(1)));
  }

  @Test
  public void testCalendarUnits() {
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.MONTH, 1)),
        (Long) expected(t -> t.plusMonths(1)));
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.QUARTER, 2)),
        (Long) expected(t -> t.plusMonths(6)));
    assertEquals(
        TimeWindowUtils.getEndTimeMillis(window(CalendarInterval.YEAR, 1)),
        (Long) expected(t -> t.plusYears(1)));
  }

  @Test
  public void testDefaultMultipleIsOne() {
    final TimeWindow window =
        new TimeWindow()
            .setStartTimeMillis(START)
            .setLength(new TimeWindowSize().setUnit(CalendarInterval.DAY));
    assertEquals(TimeWindowUtils.getEndTimeMillis(window), (Long) expected(t -> t.plusDays(1)));
  }

  @Test
  public void testMissingLengthReturnsNull() {
    assertNull(TimeWindowUtils.getEndTimeMillis(new TimeWindow().setStartTimeMillis(START)));
  }

  @Test
  public void testMissingUnitReturnsNull() {
    assertNull(
        TimeWindowUtils.getEndTimeMillis(
            new TimeWindow()
                .setStartTimeMillis(START)
                .setLength(new TimeWindowSize().setMultiple(2))));
  }

  @Test
  public void testMissingStartReturnsNull() {
    assertNull(
        TimeWindowUtils.getEndTimeMillis(
            new TimeWindow().setLength(new TimeWindowSize().setUnit(CalendarInterval.DAY))));
  }

  @Test
  public void testUnknownUnitReturnsNull() {
    final TimeWindowSize length = new TimeWindowSize().setMultiple(1);
    // A unit this server's schema does not know deserializes as $UNKNOWN.
    length.data().put("unit", "FORTNIGHT");
    assertEquals(length.getUnit(), CalendarInterval.$UNKNOWN);
    assertNull(
        TimeWindowUtils.getEndTimeMillis(
            new TimeWindow().setStartTimeMillis(START).setLength(length)));
  }
}
