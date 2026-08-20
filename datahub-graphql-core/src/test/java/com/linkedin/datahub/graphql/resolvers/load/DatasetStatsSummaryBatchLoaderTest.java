package com.linkedin.datahub.graphql.resolvers.load;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class DatasetStatsSummaryBatchLoaderTest {

  private static final String DATASET_A = "urn:li:dataset:(urn:li:dataPlatform:test,a,PROD)";
  private static final String DATASET_B = "urn:li:dataset:(urn:li:dataPlatform:test,b,PROD)";
  private static final String USER_X = "urn:li:corpuser:x";
  private static final String USER_Y = "urn:li:corpuser:y";
  private static final String USER_Z = "urn:li:corpuser:z";

  /**
   * N+1 guarantee + per-URN assembly: two datasets produce exactly TWO batched aggregation calls
   * (one per shape) and ZERO single-URN calls, and each dataset's summary is assembled from its own
   * slice — queryCount summed across day buckets, uniqueUserCount = #user buckets, topUsers ordered
   * by count desc.
   */
  @Test
  public void testBatchesAndAssemblesPerUrn() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);

    // queryCount shape: DATE bucket + LATEST totalSqlQueries. Rows [dayTs, totalSqlQueries].
    final Map<Urn, GenericTable> queryCounts = new HashMap<>();
    queryCounts.put(Urn.createFromString(DATASET_A), table("3", "5")); // sum = 8
    queryCounts.put(Urn.createFromString(DATASET_B), table("2")); // sum = 2
    stubBatch(ts, /* dateShape= */ true, queryCounts);

    // userCounts shape: STRING bucket on userCounts.user + SUM count. Rows [userUrn, count].
    final Map<Urn, GenericTable> userCounts = new HashMap<>();
    userCounts.put(Urn.createFromString(DATASET_A), userTable(row(USER_X, "4"), row(USER_Y, "9")));
    userCounts.put(Urn.createFromString(DATASET_B), userTable(row(USER_X, "1")));
    stubBatch(ts, /* dateShape= */ false, userCounts);

    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts, DatasetStatsSummaryBatchLoader.newCache(), (ctx, urn) -> true);

    final List<DatasetStatsSummary> result =
        loader.batchLoad(
            ImmutableList.of(Urn.createFromString(DATASET_A), Urn.createFromString(DATASET_B)),
            mockContext());

    // Two batched calls (one per shape), never the single-URN fallback.
    Mockito.verify(ts, Mockito.times(2))
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any());
    Mockito.verify(ts, Mockito.times(0))
        .getAggregatedStats(any(), any(), any(), any(), any(), any());

    assertEquals(result.size(), 2);
    // Dataset A: queryCount 3+5=8, 2 unique users, top user is Y (9) then X (4).
    assertEquals(result.get(0).getQueryCountLast30Days().intValue(), 8);
    assertEquals(result.get(0).getUniqueUserCountLast30Days().intValue(), 2);
    assertEquals(
        result.get(0).getTopUsersLast30Days().stream()
            .map(u -> u.getUrn())
            .collect(Collectors.toList()),
        ImmutableList.of(USER_Y, USER_X));
    // Dataset B: queryCount 2, 1 unique user.
    assertEquals(result.get(1).getQueryCountLast30Days().intValue(), 2);
    assertEquals(result.get(1).getUniqueUserCountLast30Days().intValue(), 1);
  }

  /** Top users are capped at 5 and ordered by summed count descending. */
  @Test
  public void testTopUsersCappedAndOrdered() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    stubBatch(ts, true, new HashMap<>()); // no query-count rows

    final Map<Urn, GenericTable> userCounts = new HashMap<>();
    userCounts.put(
        Urn.createFromString(DATASET_A),
        userTable(
            row(USER_X, "1"),
            row(USER_Y, "50"),
            row(USER_Z, "10"),
            row("urn:li:corpuser:d", "7"),
            row("urn:li:corpuser:e", "3"),
            row("urn:li:corpuser:f", "99")));
    stubBatch(ts, false, userCounts);

    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts, DatasetStatsSummaryBatchLoader.newCache(), (ctx, urn) -> true);
    final List<DatasetStatsSummary> result =
        loader.batchLoad(ImmutableList.of(Urn.createFromString(DATASET_A)), mockContext());

    assertEquals(result.get(0).getUniqueUserCountLast30Days().intValue(), 6);
    final List<String> top =
        result.get(0).getTopUsersLast30Days().stream()
            .map(u -> u.getUrn())
            .collect(Collectors.toList());
    assertEquals(top.size(), 5);
    // Ordered by count desc: f(99), y(50), z(10), d(7), e(3) — x(1) dropped.
    assertEquals(
        top,
        ImmutableList.of(
            "urn:li:corpuser:f", USER_Y, USER_Z, "urn:li:corpuser:d", "urn:li:corpuser:e"));
  }

  /** Absent results (no usage) yield a summary with the stats fields left unset. */
  @Test
  public void testEmptyResultsYieldEmptySummary() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    stubBatch(ts, true, new HashMap<>());
    stubBatch(ts, false, new HashMap<>());

    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts, DatasetStatsSummaryBatchLoader.newCache(), (ctx, urn) -> true);
    final List<DatasetStatsSummary> result =
        loader.batchLoad(ImmutableList.of(Urn.createFromString(DATASET_A)), mockContext());

    assertEquals(result.size(), 1);
    assertNull(result.get(0).getQueryCountLast30Days());
    assertNull(result.get(0).getUniqueUserCountLast30Days());
    assertNull(result.get(0).getTopUsersLast30Days());
  }

  /**
   * URNs resolved once are served from cache on the next load. A repeat load of fully-cached URNs
   * issues NO further aggregation calls — the summaries come straight from the cache.
   */
  @Test
  public void testCacheServesRepeatUrnsWithoutRequerying() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);

    final Map<Urn, GenericTable> queryCounts = new HashMap<>();
    queryCounts.put(Urn.createFromString(DATASET_A), table("5"));
    queryCounts.put(Urn.createFromString(DATASET_B), table("2"));
    stubBatch(ts, true, queryCounts);
    final Map<Urn, GenericTable> userCounts = new HashMap<>();
    userCounts.put(Urn.createFromString(DATASET_A), userTable(row(USER_X, "4")));
    userCounts.put(Urn.createFromString(DATASET_B), userTable(row(USER_Y, "1")));
    stubBatch(ts, false, userCounts);

    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts, DatasetStatsSummaryBatchLoader.newCache(), (ctx, urn) -> true);
    final ImmutableList<Urn> urns =
        ImmutableList.of(Urn.createFromString(DATASET_A), Urn.createFromString(DATASET_B));

    // First load: both are misses → 2 batch calls (one per shape) populate the cache.
    loader.batchLoad(urns, mockContext());
    // Second load: both cached → still only the original 2 calls, no re-query.
    final List<DatasetStatsSummary> second = loader.batchLoad(urns, mockContext());

    Mockito.verify(ts, Mockito.times(2))
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any());
    assertEquals(second.get(0).getQueryCountLast30Days().intValue(), 5); // A from cache
    assertEquals(second.get(1).getQueryCountLast30Days().intValue(), 2); // B from cache
  }

  /**
   * Unauthorized URNs resolve to null (never fetched). With a predicate that allows only A, B's
   * summary is null while A is assembled normally.
   */
  @Test
  public void testUnauthorizedUrnsResolveToNull() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    final Urn a = Urn.createFromString(DATASET_A);
    final Urn b = Urn.createFromString(DATASET_B);

    final Map<Urn, GenericTable> queryCounts = new HashMap<>();
    queryCounts.put(a, table("7"));
    queryCounts.put(b, table("9"));
    stubBatch(ts, true, queryCounts);
    stubBatch(ts, false, new HashMap<>());

    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts, DatasetStatsSummaryBatchLoader.newCache(), (ctx, urn) -> urn.equals(a));

    final List<DatasetStatsSummary> result =
        loader.batchLoad(ImmutableList.of(a, b), mockContext());

    assertEquals(result.size(), 2);
    assertEquals(result.get(0).getQueryCountLast30Days().intValue(), 7); // A authorized
    assertNull(result.get(1)); // B unauthorized -> null
  }

  /**
   * A failing authorization check is treated as unauthorized: the loader does not throw, the URN
   * resolves to null, and no aggregation is fetched for it.
   */
  @Test
  public void testAuthorizationExceptionTreatedAsUnauthorized() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts,
            DatasetStatsSummaryBatchLoader.newCache(),
            (ctx, urn) -> {
              throw new RuntimeException("boom");
            });

    final List<DatasetStatsSummary> result =
        loader.batchLoad(ImmutableList.of(Urn.createFromString(DATASET_A)), mockContext());

    assertEquals(result.size(), 1);
    assertNull(result.get(0)); // auth failure -> unauthorized -> null
    // Unauthorized URN is never fetched.
    Mockito.verify(ts, Mockito.times(0))
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any());
  }

  /**
   * Tied counts are broken deterministically by user urn ascending (not ES row order), so the top-N
   * is stable and matches the per-URN resolver.
   */
  @Test
  public void testTopUsersTieBrokenByUrnAscending() throws Exception {
    final TimeseriesAspectService ts = Mockito.mock(TimeseriesAspectService.class);
    stubBatch(ts, true, new HashMap<>());
    // z has the highest count; x and y are tied and supplied out of urn order.
    final Map<Urn, GenericTable> userCounts = new HashMap<>();
    userCounts.put(
        Urn.createFromString(DATASET_A),
        userTable(row(USER_Y, "5"), row(USER_Z, "10"), row(USER_X, "5")));
    stubBatch(ts, false, userCounts);

    final DatasetStatsSummaryBatchLoader loader =
        new DatasetStatsSummaryBatchLoader(
            ts, DatasetStatsSummaryBatchLoader.newCache(), (ctx, urn) -> true);
    final List<DatasetStatsSummary> result =
        loader.batchLoad(ImmutableList.of(Urn.createFromString(DATASET_A)), mockContext());

    // z (10) first; x and y tied at 5 → urn asc (x before y), independent of input order.
    assertEquals(
        result.get(0).getTopUsersLast30Days().stream()
            .map(u -> u.getUrn())
            .collect(Collectors.toList()),
        ImmutableList.of(USER_Z, USER_X, USER_Y));
  }

  // Distinguish the two batch calls by grouping-bucket shape (DATE = queryCount, STRING = users).
  private static void stubBatch(
      final TimeseriesAspectService ts, final boolean dateShape, final Map<Urn, GenericTable> ret) {
    Mockito.when(
            ts.batchGetAggregatedStats(
                any(),
                any(),
                any(),
                any(),
                any(),
                any(),
                Mockito.argThat(
                    (GroupingBucket[] gb) ->
                        gb != null
                            && gb.length > 0
                            && (gb[0].getType() == GroupingBucketType.DATE_GROUPING_BUCKET)
                                == dateShape)))
        .thenReturn(ret);
  }

  private static GenericTable table(final String... totalSqlQueriesPerDay) {
    final StringArrayArray rows = new StringArrayArray();
    long ts = 1000;
    for (String q : totalSqlQueriesPerDay) {
      rows.add(new StringArray(ImmutableList.of(Long.toString(ts), q)));
      ts += 1;
    }
    return new GenericTable()
        .setColumnNames(new StringArray(ImmutableList.of("timestamp", "totalSqlQueries")))
        .setColumnTypes(new StringArray("long", "int"))
        .setRows(rows);
  }

  private static GenericTable userTable(final StringArray... userRows) {
    return new GenericTable()
        .setColumnNames(new StringArray(ImmutableList.of("userCounts.user", "userCounts.count")))
        .setColumnTypes(new StringArray("string", "int"))
        .setRows(new StringArrayArray(ImmutableList.copyOf(userRows)));
  }

  private static StringArray row(final String user, final String count) {
    return new StringArray(ImmutableList.of(user, count));
  }

  private static QueryContext mockContext() {
    final QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getOperationContext()).thenReturn(Mockito.mock(OperationContext.class));
    return context;
  }
}
