package com.linkedin.metadata.timeseries.elastic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.WindowDuration;
import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer.CanonicalNow;
import com.linkedin.timeseries.CalendarInterval;
import com.linkedin.usage.UsageTimeRange;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import org.testng.annotations.Test;

public class TimeseriesUtilsTest {

  private static final long NOW = 1_700_000_000_000L;
  private static final long HOUR_MILLIS = 60L * 60L * 1000L;
  private static final long DAY_MILLIS = 24L * HOUR_MILLIS;
  private static final long FIVE_MINUTES_MILLIS = 5L * 60L * 1000L;

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

  // ---------------------------------------------------------------------------------------------
  // Canonical clock overload
  // ---------------------------------------------------------------------------------------------

  /** A canonical clock read pinned to a fixed instant with a 5m EXPAND bucket. */
  private static CanonicalNow canonicalNow(String isoInstant) {
    return QueryTimeCanonicalizer.fromConfig(
            QueryCanonicalizationConfiguration.builder()
                .enabled(true)
                .time(
                    TimeCanonicalizationConfiguration.builder()
                        .enabled(true)
                        .bucketSize(5)
                        .bucketSizeUnit("MINUTES")
                        .timezone("UTC")
                        .rounding("EXPAND")
                        .build())
                .build(),
            null,
            Clock.fixed(Instant.parse(isoInstant), ZoneOffset.UTC))
        .now();
  }

  private static long utc(String isoInstant) {
    return Instant.parse(isoInstant).toEpochMilli();
  }

  /**
   * The overload must measure from {@link CanonicalNow#reference()} - the floored bucket boundary -
   * not from the raw clock reading. Measuring from the raw clock would leave the start unique per
   * request even though the end is canonical, so the query as a whole would still never repeat.
   */
  @Test
  public void testStartTimeIsMeasuredFromTheCanonicalReference() {
    CanonicalNow now = canonicalNow("2026-08-16T19:03:42.123Z");
    long flooredReference = utc("2026-08-16T19:00:00Z");

    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, now).longValue(),
        flooredReference - (31 * DAY_MILLIS + 1));
    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.DAY, now).longValue(),
        flooredReference - (2 * DAY_MILLIS + 1));
  }

  /**
   * Every request inside one bucket must produce the same start. That is the point of the overload:
   * a stable start paired with a stable {@link CanonicalNow#upperBound()} is what makes two
   * requests eligible to share a cache entry.
   */
  @Test
  public void testSameBucketProducesSameStartTime() {
    Long early =
        TimeseriesUtils.convertRangeToStartTime(
            UsageTimeRange.MONTH, canonicalNow("2026-08-16T19:00:00Z"));
    Long late =
        TimeseriesUtils.convertRangeToStartTime(
            UsageTimeRange.MONTH, canonicalNow("2026-08-16T19:04:59.999Z"));
    Long nextBucket =
        TimeseriesUtils.convertRangeToStartTime(
            UsageTimeRange.MONTH, canonicalNow("2026-08-16T19:05:00Z"));

    assertEquals(late, early);
    assertEquals(nextBucket.longValue(), early + FIVE_MINUTES_MILLIS);
  }

  /**
   * Under EXPAND the canonical window must be a superset of the exact one, so no usage bucket is
   * dropped. That holds only if the start floors and the end ceils - swapping them would narrow the
   * window on both sides.
   */
  @Test
  public void testCanonicalWindowIsSupersetOfExactWindow() {
    String instant = "2026-08-16T19:03:42.123Z";
    CanonicalNow now = canonicalNow(instant);

    long canonicalStart = TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, now);
    long exactStart = TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, utc(instant));

    assertTrue(
        canonicalStart <= exactStart,
        "canonical start " + canonicalStart + " must not be after exact start " + exactStart);
    assertTrue(
        now.upperBound() >= utc(instant),
        "canonical end " + now.upperBound() + " must not be before the clock reading");
  }

  /** With canonicalization off the overload must reproduce the exact wall-clock behavior. */
  @Test
  public void testDisabledCanonicalizerLeavesStartTimeExact() {
    CanonicalNow now = QueryTimeCanonicalizer.DISABLED.now();

    assertEquals(
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, now),
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, now.raw()));
  }
}
