package com.linkedin.metadata.timeseries.elastic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.common.WindowDuration;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.metadata.config.search.QueryCanonicalizationConfiguration;
import com.linkedin.metadata.config.search.TimeCanonicalizationConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.canonicalization.QueryTimeCanonicalizer;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.usage.UsageTimeRange;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.function.BiConsumer;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

/**
 * The usage and operations {@code queryRange} paths derive both ends of their window from the
 * operation's canonical clock. These tests check the time filter that actually reaches the
 * timeseries service, for an enabled canonicalizer and for a disabled one.
 */
public class QueryRangeCanonicalBoundsTest {

  private static final String RESOURCE = "urn:li:dataset:(urn:li:dataPlatform:hive,db.events,PROD)";

  /** Mid-bucket, so an enabled 5-minute canonicalizer moves both ends of the window. */
  private static final Instant NOW = Instant.parse("2026-08-16T19:03:42Z");

  private static final long FLOORED = Instant.parse("2026-08-16T19:00:00Z").toEpochMilli();
  private static final long CEILED = Instant.parse("2026-08-16T19:05:00Z").toEpochMilli();

  @Test
  public void testUsageQueryRangeSendsCanonicalBounds() {
    long[] bounds =
        timestampBoundsSentBy(
            contextWith(true, 5, "MINUTES"), QueryRangeCanonicalBoundsTest::usageQueryRange);

    assertEquals(bounds[0], startBefore(FLOORED), "start is measured back from the floor");
    assertEquals(bounds[1], CEILED);
  }

  @Test
  public void testUsageQueryRangeDisabledSendsExactBounds() {
    long[] bounds =
        timestampBoundsSentBy(
            contextWith(false, 5, "MINUTES"), QueryRangeCanonicalBoundsTest::usageQueryRange);

    assertEquals(bounds[0], startBefore(NOW.toEpochMilli()));
    assertEquals(bounds[1], NOW.toEpochMilli());
  }

  @Test
  public void testOperationsQueryRangeSendsCanonicalBounds() {
    long[] bounds =
        timestampBoundsSentBy(
            contextWith(true, 5, "MINUTES"), QueryRangeCanonicalBoundsTest::operationsQueryRange);

    assertEquals(bounds[0], startBefore(FLOORED), "start is measured back from the floor");
    assertEquals(bounds[1], CEILED);
  }

  @Test
  public void testOperationsQueryRangeDisabledSendsExactBounds() {
    long[] bounds =
        timestampBoundsSentBy(
            contextWith(false, 5, "MINUTES"), QueryRangeCanonicalBoundsTest::operationsQueryRange);

    assertEquals(bounds[0], startBefore(NOW.toEpochMilli()));
    assertEquals(bounds[1], NOW.toEpochMilli());
  }

  /**
   * A bucket as long as a day would round an hour usage chart out to a day or more, so it is
   * rejected and the hour range keeps its exact bounds.
   */
  @Test
  public void testDayBucketDoesNotStretchHourRange() {
    long[] bounds =
        timestampBoundsSentBy(
            contextWith(true, 1, "DAYS"),
            (opContext, service) ->
                UsageServiceUtil.queryRange(
                    opContext, service, RESOURCE, WindowDuration.HOUR, UsageTimeRange.HOUR, null));

    assertEquals(
        bounds[0],
        TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.HOUR, NOW.toEpochMilli()));
    assertEquals(bounds[1], NOW.toEpochMilli());
  }

  private static void usageQueryRange(OperationContext opContext, TimeseriesAspectService service) {
    UsageServiceUtil.queryRange(
        opContext, service, RESOURCE, WindowDuration.DAY, UsageTimeRange.MONTH, null);
  }

  private static void operationsQueryRange(
      OperationContext opContext, TimeseriesAspectService service) {
    OperationsServiceUtil.queryRange(
        opContext, service, RESOURCE, WindowDuration.DAY, UsageTimeRange.MONTH, null);
  }

  private static long startBefore(long reference) {
    return TimeseriesUtils.convertRangeToStartTime(UsageTimeRange.MONTH, reference);
  }

  /** Runs the query and returns the [start, end] timestamp bounds of the filter it sent. */
  private static long[] timestampBoundsSentBy(
      OperationContext opContext, BiConsumer<OperationContext, TimeseriesAspectService> query) {
    TimeseriesAspectService service = mock(TimeseriesAspectService.class);
    when(service.getAggregatedStats(
            any(OperationContext.class), anyString(), anyString(), any(), any(), any()))
        .thenAnswer(
            invocation ->
                emptyTable(
                    invocation.<AggregationSpec[]>getArgument(3).length
                        + invocation.<GroupingBucket[]>getArgument(5).length));

    query.accept(opContext, service);

    ArgumentCaptor<Filter> filters = ArgumentCaptor.forClass(Filter.class);
    verify(service, atLeastOnce())
        .getAggregatedStats(
            any(OperationContext.class), anyString(), anyString(), any(), filters.capture(), any());
    return timestampBounds(filters.getValue());
  }

  private static OperationContext contextWith(
      boolean canonicalize, int bucketSize, String bucketSizeUnit) {
    QueryTimeCanonicalizer canonicalizer =
        QueryTimeCanonicalizer.fromConfig(
            QueryCanonicalizationConfiguration.builder()
                .enabled(canonicalize)
                .time(
                    TimeCanonicalizationConfiguration.builder()
                        .enabled(true)
                        .bucketSize(bucketSize)
                        .bucketSizeUnit(bucketSizeUnit)
                        .timezone("UTC")
                        .rounding("EXPAND")
                        .build())
                .build(),
            null,
            Clock.fixed(NOW, ZoneOffset.UTC));
    return TestOperationContexts.systemContextNoSearchAuthorization()
        .withQueryTimeCanonicalizer(canonicalizer);
  }

  /** No rows, but as many columns as the caller asked for, which the operations path checks. */
  private static GenericTable emptyTable(int columns) {
    StringArray names = new StringArray();
    for (int i = 0; i < columns; i++) {
      names.add("col" + i);
    }
    return new GenericTable()
        .setColumnNames(names)
        .setColumnTypes(new StringArray(names))
        .setRows(new StringArrayArray());
  }

  private static long[] timestampBounds(Filter filter) {
    Long start = null;
    Long end = null;
    for (Criterion criterion : filter.getOr().get(0).getAnd()) {
      if (!Constants.ES_FIELD_TIMESTAMP.equals(criterion.getField())) {
        continue;
      }
      long value = Long.parseLong(criterion.getValues().get(0));
      if (criterion.getCondition() == Condition.GREATER_THAN_OR_EQUAL_TO) {
        start = value;
      } else if (criterion.getCondition() == Condition.LESS_THAN_OR_EQUAL_TO) {
        end = value;
      }
    }
    assertNotNull(start, "no lower timestamp bound in: " + filter);
    assertNotNull(end, "no upper timestamp bound in: " + filter);
    return new long[] {start, end};
  }
}
