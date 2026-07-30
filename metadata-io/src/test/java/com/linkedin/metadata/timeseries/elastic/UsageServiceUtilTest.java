package com.linkedin.metadata.timeseries.elastic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import com.linkedin.common.WindowDuration;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import org.testng.annotations.Test;

public class UsageServiceUtilTest {

  @Test
  public void testLatestAggSpec() {
    AggregationSpec spec = UsageServiceUtil.latestAggSpec(UsageServiceUtil.FIELD_TOTAL_SQL_QUERIES);
    assertEquals(spec.getAggregationType(), AggregationType.LATEST);
    assertEquals(spec.getFieldPath(), UsageServiceUtil.FIELD_TOTAL_SQL_QUERIES);
  }

  @Test
  public void testSumAggSpec() {
    AggregationSpec spec = UsageServiceUtil.sumAggSpec(UsageServiceUtil.FIELD_USER_COUNTS_COUNT);
    assertEquals(spec.getAggregationType(), AggregationType.SUM);
    assertEquals(spec.getFieldPath(), UsageServiceUtil.FIELD_USER_COUNTS_COUNT);
  }

  @Test
  public void testUsageTimestampBucket() {
    GroupingBucket bucket =
        UsageServiceUtil.usageTimestampBucket(WindowDuration.DAY, "America/Los_Angeles");
    assertEquals(bucket.getType(), GroupingBucketType.DATE_GROUPING_BUCKET);
    assertEquals(bucket.getKey(), Constants.ES_FIELD_TIMESTAMP);
    assertEquals(bucket.getTimeWindowSize().getUnit(), CalendarInterval.DAY);
    assertEquals(bucket.getTimeWindowSize().getMultiple().intValue(), 1);
    assertEquals(bucket.getTimeZone(), "America/Los_Angeles");
  }

  @Test
  public void testUsageTimestampBucketNullTimeZoneOmitted() {
    // Null time zone is set with IGNORE_NULL, so the field is left unset rather than null-valued.
    GroupingBucket bucket = UsageServiceUtil.usageTimestampBucket(WindowDuration.HOUR, null);
    assertEquals(bucket.getTimeWindowSize().getUnit(), CalendarInterval.HOUR);
    assertNull(bucket.getTimeZone());
  }

  @Test
  public void testUserCountsGroupingBucket() {
    GroupingBucket bucket = UsageServiceUtil.userCountsGroupingBucket();
    assertEquals(bucket.getType(), GroupingBucketType.STRING_GROUPING_BUCKET);
    assertEquals(bucket.getKey(), UsageServiceUtil.FIELD_USER_COUNTS_USER);
  }
}
