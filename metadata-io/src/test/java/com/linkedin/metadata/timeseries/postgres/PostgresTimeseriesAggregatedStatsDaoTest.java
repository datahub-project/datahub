package com.linkedin.metadata.timeseries.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.annotations.Test;

public class PostgresTimeseriesAggregatedStatsDaoTest {

  @Test
  public void formatMetricCell_sumIntegral_emitsLongStringWithoutDecimal() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            6.0d, AggregationType.SUM, DataSchema.Type.LONG),
        "6");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            new BigDecimal("6.0"), AggregationType.SUM, DataSchema.Type.INT),
        "6");
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            650.0d, AggregationType.SUM, DataSchema.Type.LONG),
        "650");
  }

  @Test
  public void formatMetricCell_sumFloating_keepsDecimalString() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            6.5d, AggregationType.SUM, DataSchema.Type.DOUBLE),
        "6.5");
  }

  @Test
  public void formatMetricCell_cardinality_emitsLongString() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            2L, AggregationType.CARDINALITY, DataSchema.Type.STRING),
        "2");
  }

  @Test
  public void formatMetricCell_null_isEsNullSentinel() {
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.formatMetricCell(
            null, AggregationType.SUM, DataSchema.Type.LONG),
        PostgresTimeseriesAggregatedStatsDao.ES_NULL_VALUE);
  }

  @Test
  public void documentTextPathSql_nestedCollectionPath() {
    String sql = PostgresTimeseriesAggregatedStatsDao.documentTextPathSql("userCounts.user");
    assertEquals(sql, "document #>> ARRAY['userCounts','user']::text[]");
  }

  @Test
  public void shouldIncludeEmptyDateBuckets_dayAndCoarserOnly() {
    assertTrue(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.DAY)));
    assertTrue(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.WEEK)));
    assertTrue(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.MONTH)));
    assertTrue(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.QUARTER)));
    assertTrue(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.YEAR)));
    assertFalse(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.HOUR)));
    assertFalse(
        PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(
            window(CalendarInterval.MINUTE)));
    assertFalse(PostgresTimeseriesAggregatedStatsDao.shouldIncludeEmptyDateBuckets(null));
  }

  @Test
  public void fillEmptyDateBuckets_day_insertsInterstitialNullMetrics() {
    GroupingBucket[] buckets = new GroupingBucket[] {dateBucket(CalendarInterval.DAY)};
    // May 27, May 28, Jun 1 2021 UTC — same sparse golden as smoke test_gms_usage_fetch
    List<StringArray> rows =
        List.of(
            row("1622073600000", "4", "1", "q1"),
            row("1622160000000", "2", "1", "q2"),
            row("1622505600000", "1", "1", "q3"));

    List<StringArray> filled =
        PostgresTimeseriesAggregatedStatsDao.fillEmptyDateBuckets(rows, buckets, 3);

    assertEquals(
        filled.stream().map(r -> r.get(0)).collect(Collectors.toList()),
        List.of(
            "1622073600000",
            "1622160000000",
            "1622246400000",
            "1622332800000",
            "1622419200000",
            "1622505600000"));
    assertEquals(filled.get(2).get(1), PostgresTimeseriesAggregatedStatsDao.ES_NULL_VALUE);
    assertEquals(filled.get(2).get(2), PostgresTimeseriesAggregatedStatsDao.ES_NULL_VALUE);
    assertEquals(filled.get(0).get(1), "4");
    assertEquals(filled.get(5).get(1), "1");
  }

  @Test
  public void fillEmptyDateBuckets_hour_doesNotGapFill() {
    GroupingBucket[] buckets = new GroupingBucket[] {dateBucket(CalendarInterval.HOUR)};
    List<StringArray> rows =
        List.of(row("1622073600000", "1"), row("1622080800000", "2")); // +2h gap
    List<StringArray> filled =
        PostgresTimeseriesAggregatedStatsDao.fillEmptyDateBuckets(rows, buckets, 1);
    assertEquals(filled.size(), 2);
  }

  private static TimeWindowSize window(CalendarInterval unit) {
    return new TimeWindowSize().setMultiple(1).setUnit(unit);
  }

  private static GroupingBucket dateBucket(CalendarInterval unit) {
    return new GroupingBucket()
        .setKey("@timestamp")
        .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
        .setTimeWindowSize(window(unit));
  }

  private static StringArray row(String... cells) {
    return new StringArray(List.of(cells));
  }
}
