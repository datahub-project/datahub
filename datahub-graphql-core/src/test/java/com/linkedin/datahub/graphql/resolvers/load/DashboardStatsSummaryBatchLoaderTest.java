package com.linkedin.datahub.graphql.resolvers.load;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardUsageStatistics;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.timeseries.GenericTable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardStatsSummaryBatchLoaderTest {

  private static final Urn URN_1 = UrnUtils.getUrn("urn:li:dashboard:(airflow,dash1)");
  private static final Urn URN_2 = UrnUtils.getUrn("urn:li:dashboard:(airflow,dash2)");
  private static final String USER_URN = "urn:li:corpuser:alice";

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private static EnvelopedAspect makeAbsoluteAspect(int viewsCount) {
    DashboardUsageStatistics stats =
        new DashboardUsageStatistics().setTimestampMillis(0L).setViewsCount(viewsCount);
    return new EnvelopedAspect().setAspect(GenericRecordUtils.serializeAspect(stats));
  }

  private static GenericTable cardinalityTable(String value) {
    return new GenericTable()
        .setRows(new StringArrayArray(new StringArray(ImmutableList.of(value))))
        .setColumnNames(new StringArray())
        .setColumnTypes(new StringArray());
  }

  private static GenericTable topUsersTable(String... userUrns) {
    StringArrayArray rows = new StringArrayArray();
    for (String u : userUrns) {
      rows.add(new StringArray(ImmutableList.of(u, "10")));
    }
    return new GenericTable()
        .setRows(rows)
        .setColumnNames(new StringArray())
        .setColumnTypes(new StringArray());
  }

  private static TimeseriesAspectService mockService(
      Map<Urn, List<EnvelopedAspect>> aspectMap,
      Map<Urn, GenericTable> cardinalityMap,
      Map<Urn, GenericTable> topUsersMap) {
    TimeseriesAspectService svc = Mockito.mock(TimeseriesAspectService.class);
    Mockito.when(
            svc.batchGetAspectValues(
                any(),
                any(),
                eq(Constants.DASHBOARD_ENTITY_NAME),
                eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
                isNull(),
                isNull(),
                eq(1),
                any(),
                isNull()))
        .thenReturn(aspectMap);
    Mockito.when(
            svc.batchGetAggregatedStats(
                any(),
                eq(Constants.DASHBOARD_ENTITY_NAME),
                eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
                eq(getUniqueUserCountAggSpecs()),
                any(),
                any(),
                isNull()))
        .thenReturn(cardinalityMap);
    Mockito.when(
            svc.batchGetAggregatedStats(
                any(),
                eq(Constants.DASHBOARD_ENTITY_NAME),
                eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
                eq(getTopUsersSumAggSpecs()),
                any(),
                any(),
                any()))
        .thenReturn(topUsersMap);
    return svc;
  }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  @Test
  public void testBatchLoadPopulatesAllFields() {
    // Two dashboards, both with data
    Map<Urn, List<EnvelopedAspect>> aspects =
        Map.of(
            URN_1, ImmutableList.of(makeAbsoluteAspect(100)),
            URN_2, ImmutableList.of(makeAbsoluteAspect(200)));
    Map<Urn, GenericTable> cardinality =
        Map.of(URN_1, cardinalityTable("3"), URN_2, cardinalityTable("5"));
    Map<Urn, GenericTable> topUsers =
        Map.of(
            URN_1, topUsersTable(USER_URN), URN_2, topUsersTable(USER_URN, "urn:li:corpuser:bob"));

    TimeseriesAspectService svc = mockService(aspects, cardinality, topUsers);
    DashboardStatsSummaryBatchLoader loader = new DashboardStatsSummaryBatchLoader(svc);
    QueryContext ctx = getMockAllowContext();

    List<DashboardStatsSummary> results = loader.batchLoad(ImmutableList.of(URN_1, URN_2), ctx);

    Assert.assertEquals(results.size(), 2);

    DashboardStatsSummary s1 = results.get(0);
    Assert.assertEquals((int) s1.getViewCount(), 100);
    Assert.assertEquals((int) s1.getUniqueUserCountLast30Days(), 3);
    Assert.assertEquals(s1.getTopUsersLast30Days().size(), 1);
    Assert.assertEquals(s1.getTopUsersLast30Days().get(0).getUrn(), USER_URN);

    DashboardStatsSummary s2 = results.get(1);
    Assert.assertEquals((int) s2.getViewCount(), 200);
    Assert.assertEquals((int) s2.getUniqueUserCountLast30Days(), 5);
    Assert.assertEquals(s2.getTopUsersLast30Days().size(), 2);
  }

  @Test
  public void testBatchLoadUrnAbsentFromAllMapsReturnsEmptySummary() {
    // URN_2 has no data in any of the three ES responses
    Map<Urn, List<EnvelopedAspect>> aspects =
        Map.of(URN_1, ImmutableList.of(makeAbsoluteAspect(50)));
    Map<Urn, GenericTable> cardinality = Map.of(URN_1, cardinalityTable("2"));
    Map<Urn, GenericTable> topUsers = Map.of(URN_1, topUsersTable(USER_URN));

    TimeseriesAspectService svc = mockService(aspects, cardinality, topUsers);
    DashboardStatsSummaryBatchLoader loader = new DashboardStatsSummaryBatchLoader(svc);
    QueryContext ctx = getMockAllowContext();

    List<DashboardStatsSummary> results = loader.batchLoad(ImmutableList.of(URN_1, URN_2), ctx);

    Assert.assertEquals(results.size(), 2);

    // URN_1 has data
    Assert.assertEquals((int) results.get(0).getViewCount(), 50);

    // URN_2: all fields null / absent — no exception thrown
    DashboardStatsSummary empty = results.get(1);
    Assert.assertNull(empty.getViewCount());
    Assert.assertNull(empty.getUniqueUserCountLast30Days());
    Assert.assertNull(empty.getTopUsersLast30Days());
  }

  @Test
  public void testBatchLoadNullCardinalityValueIsIgnored() {
    // ES returns "NULL" for cardinality — must not set uniqueUserCountLast30Days
    Map<Urn, List<EnvelopedAspect>> aspects =
        Map.of(URN_1, ImmutableList.of(makeAbsoluteAspect(10)));
    Map<Urn, GenericTable> cardinality = Map.of(URN_1, cardinalityTable(ES_NULL_VALUE));
    Map<Urn, GenericTable> topUsers = Map.of(URN_1, topUsersTable());

    TimeseriesAspectService svc = mockService(aspects, cardinality, topUsers);
    DashboardStatsSummaryBatchLoader loader = new DashboardStatsSummaryBatchLoader(svc);

    List<DashboardStatsSummary> results =
        loader.batchLoad(ImmutableList.of(URN_1), getMockAllowContext());

    Assert.assertNull(results.get(0).getUniqueUserCountLast30Days());
  }

  @Test
  public void testBatchLoadOutputOrderMatchesInputOrder() {
    // Results must be in the same order as the input URN list (DataLoader contract)
    Map<Urn, List<EnvelopedAspect>> aspects =
        Map.of(
            URN_1, ImmutableList.of(makeAbsoluteAspect(111)),
            URN_2, ImmutableList.of(makeAbsoluteAspect(222)));
    Map<Urn, GenericTable> cardinality =
        Map.of(URN_1, cardinalityTable("1"), URN_2, cardinalityTable("2"));
    Map<Urn, GenericTable> topUsers = Collections.emptyMap();

    TimeseriesAspectService svc = mockService(aspects, cardinality, topUsers);
    DashboardStatsSummaryBatchLoader loader = new DashboardStatsSummaryBatchLoader(svc);

    // Pass URN_2 first
    List<DashboardStatsSummary> results =
        loader.batchLoad(ImmutableList.of(URN_2, URN_1), getMockAllowContext());

    Assert.assertEquals((int) results.get(0).getViewCount(), 222);
    Assert.assertEquals((int) results.get(1).getViewCount(), 111);
  }

  @Test
  public void testBatchLoadMakesExactlyThreeServiceCalls() {
    TimeseriesAspectService svc =
        mockService(Collections.emptyMap(), Collections.emptyMap(), Collections.emptyMap());
    DashboardStatsSummaryBatchLoader loader = new DashboardStatsSummaryBatchLoader(svc);

    loader.batchLoad(ImmutableList.of(URN_1, URN_2), getMockAllowContext());

    // Exactly 1 call to batchGetAspectValues
    Mockito.verify(svc, Mockito.times(1))
        .batchGetAspectValues(
            any(), any(), any(), any(), isNull(), isNull(), anyInt(), any(), isNull());
    // Exactly 2 calls to batchGetAggregatedStats (cardinality + topUsers)
    Mockito.verify(svc, Mockito.times(2))
        .batchGetAggregatedStats(any(), any(), any(), any(), any(), any(), any());
    Mockito.verifyNoMoreInteractions(svc);
  }
}
