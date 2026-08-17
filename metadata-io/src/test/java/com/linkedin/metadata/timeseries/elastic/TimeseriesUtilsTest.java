package com.linkedin.metadata.timeseries.elastic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.usage.UsageTimeRange;
import java.util.List;
import org.testng.annotations.Test;

public class TimeseriesUtilsTest {

  private static final long NOW = 1_700_000_000_000L;
  private static final long HOUR_MILLIS = 60L * 60L * 1000L;
  private static final long DAY_MILLIS = 24L * HOUR_MILLIS;

  @Test
  public void testWindowToInterval() {
    assertEquals(TimeseriesUtils.windowToInterval(WindowDuration.HOUR), CalendarInterval.HOUR);
    assertEquals(TimeseriesUtils.windowToInterval(WindowDuration.DAY), CalendarInterval.DAY);
    assertEquals(TimeseriesUtils.windowToInterval(WindowDuration.WEEK), CalendarInterval.WEEK);
    assertEquals(TimeseriesUtils.windowToInterval(WindowDuration.MONTH), CalendarInterval.MONTH);
    assertEquals(TimeseriesUtils.windowToInterval(WindowDuration.YEAR), CalendarInterval.YEAR);
  }

  @Test
  public void testConvertRangeToStartTime() {
    // Each range subtracts a fixed window (+1ms) from now; ALL clamps to epoch 0.
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.HOUR, NOW).longValue(),
        NOW - (2 * HOUR_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.DAY, NOW).longValue(),
        NOW - (2 * DAY_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.WEEK, NOW).longValue(),
        NOW - (8 * DAY_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, NOW).longValue(),
        NOW - (31 * DAY_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.QUARTER, NOW).longValue(),
        NOW - (92 * DAY_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.HALF_YEAR, NOW).longValue(),
        NOW - (183 * DAY_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.YEAR, NOW).longValue(),
        NOW - (366 * DAY_MILLIS + 1));
    assertEquals(TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.ALL, NOW).longValue(), 0L);
  }

  @Test
  public void testCreateTimeRangeCriteriaBothBounds() {
    List<Criterion> criteria = TimeseriesUtils.createTimeRangeCriteria(100L, 200L);
    assertEquals(criteria.size(), 2);
    assertEquals(criteria.get(0).getField(), Constants.ES_FIELD_TIMESTAMP);
    assertEquals(criteria.get(0).getCondition(), Condition.GREATER_THAN_OR_EQUAL_TO);
    assertEquals(criteria.get(1).getField(), Constants.ES_FIELD_TIMESTAMP);
    assertEquals(criteria.get(1).getCondition(), Condition.LESS_THAN_OR_EQUAL_TO);
  }

  @Test
  public void testCreateTimeRangeCriteriaPartialAndEmpty() {
    List<Criterion> startOnly = TimeseriesUtils.createTimeRangeCriteria(100L, null);
    assertEquals(startOnly.size(), 1);
    assertEquals(startOnly.get(0).getCondition(), Condition.GREATER_THAN_OR_EQUAL_TO);

    List<Criterion> endOnly = TimeseriesUtils.createTimeRangeCriteria(null, 200L);
    assertEquals(endOnly.size(), 1);
    assertEquals(endOnly.get(0).getCondition(), Condition.LESS_THAN_OR_EQUAL_TO);

    assertTrue(TimeseriesUtils.createTimeRangeCriteria(null, null).isEmpty());
  }

  @Test
  public void testCreateCommonFilterCriteria() {
    List<Criterion> criteria =
        TimeseriesUtils.createCommonFilterCriteria("urn:li:dataset:(x)", 100L, 200L);
    // urn EQUAL criterion followed by the two time-range criteria.
    assertEquals(criteria.size(), 3);
    assertEquals(criteria.get(0).getField(), "urn");
    assertEquals(criteria.get(0).getCondition(), Condition.EQUAL);
  }
}
