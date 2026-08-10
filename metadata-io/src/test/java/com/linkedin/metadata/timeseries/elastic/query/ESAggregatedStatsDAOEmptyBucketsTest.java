package com.linkedin.metadata.timeseries.elastic.query;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.TimeWindowSize;
import org.testng.annotations.Test;

public class ESAggregatedStatsDAOEmptyBucketsTest {

  @Test
  public void testShouldIncludeEmptyDateBuckets_DayOrCoarser() {
    assertTrue(ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.DAY)));
    assertTrue(ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.WEEK)));
    assertTrue(ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.MONTH)));
    assertTrue(
        ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.QUARTER)));
    assertTrue(ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.YEAR)));
  }

  @Test
  public void testShouldIncludeEmptyDateBuckets_FineGrainedSkipped() {
    assertFalse(
        ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.MINUTE)));
    assertFalse(ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.HOUR)));
    assertFalse(
        ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(window(CalendarInterval.SECOND)));
    assertFalse(ESAggregatedStatsDAO.shouldIncludeEmptyDateBuckets(null));
  }

  private static TimeWindowSize window(CalendarInterval unit) {
    return new TimeWindowSize().setUnit(unit).setMultiple(1);
  }
}
