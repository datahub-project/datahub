package com.linkedin.metadata.timeseries.postgres;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.data.schema.DataSchema;
import com.linkedin.data.template.StringArray;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import com.linkedin.timeseries.TimeWindowSize;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
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
  public void formatGroupCell_offsetDateTime_emitsEpochMillis() {
    OffsetDateTime bucket = OffsetDateTime.parse("2021-05-27T00:00:00Z");
    assertEquals(PostgresTimeseriesAggregatedStatsDao.formatGroupCell(bucket), "1622073600000");
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

  @Test
  public void buildAggregatedStatsSql_dateThenString_usesPerParentRowNumber() {
    GroupingBucket[] buckets =
        new GroupingBucket[] {dateBucket(CalendarInterval.DAY), stringBucket("user", 5)};
    AggregationSpec[] specs =
        new AggregationSpec[] {
          new AggregationSpec().setFieldPath("count").setAggregationType(AggregationType.SUM)
        };
    String sql =
        PostgresTimeseriesAggregatedStatsDao.buildAggregatedStatsSql(
            "public.ts_aspect",
            List.of("date_trunc('day', ...) AS g0", "document #>> ARRAY['user'] AS g1"),
            List.of("date_trunc('day', ...)", "document #>> ARRAY['user']"),
            List.of("g0", "g1"),
            List.of("SUM(...) AS \"sum_count\""),
            List.of("sum_count"),
            List.of("document #>> ARRAY['user']"),
            buckets,
            specs,
            null,
            "true");
    assertTrue(sql.contains("ROW_NUMBER() OVER (PARTITION BY g0 "));
    assertTrue(sql.contains("WHERE _rn <= 5 ORDER BY"));
    assertFalse(sql.trim().endsWith("LIMIT 5"));
    assertTrue(sql.contains("AT TIME ZONE") || sql.contains("date_trunc('day'"));
    assertTrue(sql.contains("ORDER BY"));
  }

  @Test
  public void buildAggregatedStatsSql_stringOnly_ranksGlobally() {
    GroupingBucket[] buckets = new GroupingBucket[] {stringBucket("user", 3)};
    AggregationSpec[] specs =
        new AggregationSpec[] {
          new AggregationSpec().setFieldPath("count").setAggregationType(AggregationType.SUM)
        };
    String sql =
        PostgresTimeseriesAggregatedStatsDao.buildAggregatedStatsSql(
            "public.ts_aspect",
            List.of("document #>> ARRAY['user'] AS g0"),
            List.of("document #>> ARRAY['user']"),
            List.of("g0"),
            List.of("SUM(...) AS \"sum_count\""),
            List.of("sum_count"),
            List.of("document #>> ARRAY['user']"),
            buckets,
            specs,
            null,
            "true");
    assertTrue(sql.contains("ROW_NUMBER() OVER (ORDER BY"));
    assertFalse(sql.contains("PARTITION BY"));
    assertTrue(sql.contains("DISTINCT ON (g0)"));
    assertTrue(sql.contains("WHERE _rn <= 3"));
    assertTrue(sql.contains(" ORDER BY document #>> ARRAY['user'] ASC"));
  }

  @Test
  public void buildAggregatedStatsSql_twoStringGroups_ranksEachLevel() {
    GroupingBucket[] buckets =
        new GroupingBucket[] {stringBucket("user", 5), stringBucket("status", 3)};
    AggregationSpec[] specs =
        new AggregationSpec[] {
          new AggregationSpec().setFieldPath("count").setAggregationType(AggregationType.SUM)
        };
    String sql =
        PostgresTimeseriesAggregatedStatsDao.buildAggregatedStatsSql(
            "public.ts_aspect",
            List.of("u AS g0", "s AS g1"),
            List.of("u", "s"),
            List.of("g0", "g1"),
            List.of("SUM(...) AS \"sum_count\""),
            List.of("sum_count"),
            List.of("u", "s"),
            buckets,
            specs,
            null,
            "true");
    assertTrue(sql.contains("PARTITION BY g0"));
    assertTrue(sql.contains("WHERE _rn <= 3"));
    assertTrue(sql.contains("DISTINCT ON (g0)"));
    assertTrue(sql.contains("WHERE _rn <= 5"));
  }

  @Test
  public void buildGroupOrderBy_stringDefaultAscending() {
    GroupingBucket[] buckets = new GroupingBucket[] {stringBucket("user", 10)};
    String order =
        PostgresTimeseriesAggregatedStatsDao.buildGroupOrderBy(
            buckets, List.of("g0"), List.of("g0"), List.of("sum_count"), new AggregationSpec[0], null);
    assertEquals(order, "g0 ASC");
  }

  @Test
  public void stringGroupingLimit_defaultsToMaxTermBuckets() {
    GroupingBucket[] buckets =
        new GroupingBucket[] {
          new GroupingBucket().setKey("user").setType(GroupingBucketType.STRING_GROUPING_BUCKET)
        };
    assertEquals(
        PostgresTimeseriesAggregatedStatsDao.stringGroupingLimit(buckets),
        Integer.valueOf(PostgresTimeseriesAggregatedStatsDao.MAX_TERM_BUCKETS));
  }

  @Test
  public void fillEmptyDateBuckets_dayMultiple2_preservesAlignedBuckets() {
    GroupingBucket[] buckets =
        new GroupingBucket[] {
          new GroupingBucket()
              .setKey("@timestamp")
              .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
              .setTimeWindowSize(new TimeWindowSize().setMultiple(2).setUnit(CalendarInterval.DAY))
        };
    // Aligned 2-day bucket keys (as SQL with multiple=2 would emit)
    List<StringArray> rows =
        List.of(row("1622073600000", "4"), row("1622246400000", "1")); // May 27, May 29

    List<StringArray> filled =
        PostgresTimeseriesAggregatedStatsDao.fillEmptyDateBuckets(rows, buckets, 1);

    assertEquals(
        filled.stream().map(r -> r.get(0)).collect(Collectors.toList()),
        List.of("1622073600000", "1622246400000"));
    assertEquals(filled.get(0).get(1), "4");
    assertEquals(filled.get(1).get(1), "1");
  }

  @Test
  public void fillEmptyDateBuckets_dayMultiple2_matchesSqlBucketKeysAndFillsGap() {
    GroupingBucket[] buckets =
        new GroupingBucket[] {
          new GroupingBucket()
              .setKey("@timestamp")
              .setType(GroupingBucketType.DATE_GROUPING_BUCKET)
              .setTimeWindowSize(new TimeWindowSize().setMultiple(2).setUnit(CalendarInterval.DAY))
        };
    // Unaligned wall-clock times; SQL date_trunc+multiple=2 floors to even unix-day starts.
    long firstSqlKey = sqlDayMultiple2BucketMillis(1622116800000L); // May 27 12:00 UTC
    long lastSqlKey = sqlDayMultiple2BucketMillis(1622462400000L); // May 31 12:00 UTC
    assertEquals(firstSqlKey, 1622073600000L); // May 27 00:00 UTC
    assertEquals(lastSqlKey, 1622419200000L); // May 31 00:00 UTC

    List<StringArray> rows =
        List.of(row(String.valueOf(firstSqlKey), "4"), row(String.valueOf(lastSqlKey), "1"));
    List<StringArray> filled =
        PostgresTimeseriesAggregatedStatsDao.fillEmptyDateBuckets(rows, buckets, 1);

    assertEquals(
        filled.stream().map(r -> r.get(0)).collect(Collectors.toList()),
        List.of("1622073600000", "1622246400000", "1622419200000"));
    assertEquals(filled.get(1).get(1), PostgresTimeseriesAggregatedStatsDao.ES_NULL_VALUE);
    assertEquals(filled.get(1).get(0), String.valueOf(sqlDayMultiple2BucketMillis(1622246400000L)));
    assertEquals(filled.get(0).get(1), "4");
    assertEquals(filled.get(2).get(1), "1");
  }

  @Test
  public void postgresDateBucketSql_multiple1_usesDateTruncOnly() {
    String sql =
        PostgresTimeseriesAggregatedStatsDao.postgresDateBucketSql(
            window(CalendarInterval.DAY), "document->>'@timestamp'", java.time.ZoneId.of("GMT"));
    assertTrue(sql.contains("date_trunc('day'"));
    assertFalse(sql.contains("% 2"));
    assertFalse(sql.contains("INTERVAL '1 day'"));
  }

  @Test
  public void postgresDateBucketSql_multiple2_alignsDayBuckets() {
    String sql =
        PostgresTimeseriesAggregatedStatsDao.postgresDateBucketSql(
            new TimeWindowSize().setMultiple(2).setUnit(CalendarInterval.DAY),
            "document->>'@timestamp'",
            java.time.ZoneId.of("GMT"));
    assertTrue(sql.contains("date_trunc('day'"));
    assertTrue(sql.contains("/ 86400) % 2"));
    assertTrue(sql.contains("INTERVAL '1 day'"));
  }

  @Test
  public void alignDateTruncToMultiple_month() {
    String aligned =
        PostgresTimeseriesAggregatedStatsDao.alignDateTruncToMultiple(
            "date_trunc('month', ts)", CalendarInterval.MONTH, 3);
    assertTrue(aligned.contains("make_interval(months =>"));
    assertTrue(aligned.contains("% 3"));
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

  private static GroupingBucket stringBucket(String key, int size) {
    return new GroupingBucket()
        .setKey(key)
        .setType(GroupingBucketType.STRING_GROUPING_BUCKET)
        .setSize(size);
  }

  private static StringArray row(String... cells) {
    return new StringArray(List.of(cells));
  }

  /** Java equivalent of {@code postgresDateBucketSql} for DAY + multiple=2 in GMT. */
  private static long sqlDayMultiple2BucketMillis(long epochMillis) {
    ZonedDateTime truncated =
        Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC).truncatedTo(ChronoUnit.DAYS);
    long remainder = (truncated.toEpochSecond() / 86400) % 2;
    return truncated.minusDays(remainder).toInstant().toEpochMilli();
  }
}
