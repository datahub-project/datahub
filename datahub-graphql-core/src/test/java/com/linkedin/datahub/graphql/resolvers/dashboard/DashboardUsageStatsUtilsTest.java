package com.linkedin.datahub.graphql.resolvers.dashboard;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.generated.CorpUser;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.AggregationSpec;
import com.linkedin.timeseries.AggregationType;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.timeseries.GroupingBucket;
import com.linkedin.timeseries.GroupingBucketType;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardUsageStatsUtilsTest {

  private static final String TEST_USER_URN_1 = "urn:li:corpuser:test1";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:test2";
  private static final String TEST_URN = "urn:li:dashboard:(airflow,test)";
  private static final String USER_URN_1 = "urn:li:corpuser:alice";
  private static final String USER_URN_2 = "urn:li:corpuser:bob";

  // --- getUserUsageCounts ---

  @Test
  public void testGetUserUsageCountsWithViewsCountOnly() {
    TimeseriesAspectService mockService = mock(TimeseriesAspectService.class);
    OperationContext mockOpContext = mock(OperationContext.class);
    Filter filter = mock(Filter.class);

    // Row format: [user, sumUsageCount, sumViewsCount, sumExecCount,
    //              cardUsageCount, cardViewsCount, cardExecCount]
    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
            Mockito.eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
            any(),
            any(Filter.class),
            any()))
        .thenReturn(
            new GenericTable()
                .setRows(
                    new StringArrayArray(
                        // User 1: only viewsCount set (usageCount and execCount are NULL)
                        new StringArray(
                            ImmutableList.of(TEST_USER_URN_1, "NULL", "30", "NULL", "0", "1", "0")),
                        // User 2: only viewsCount set
                        new StringArray(
                            ImmutableList.of(
                                TEST_USER_URN_2, "NULL", "12", "NULL", "0", "1", "0"))))
                .setColumnNames(new StringArray())
                .setColumnTypes(new StringArray()));

    List<DashboardUserUsageCounts> result =
        DashboardUsageStatsUtils.getUserUsageCounts(mockOpContext, filter, mockService);

    Assert.assertEquals(result.size(), 2);
    // usageCount should be null for both (not set in source data)
    Assert.assertNull(result.get(0).getUsageCount());
    Assert.assertNull(result.get(1).getUsageCount());
    // viewsCount MUST be populated
    Assert.assertEquals((int) result.get(0).getViewsCount(), 30);
    Assert.assertEquals((int) result.get(1).getViewsCount(), 12);
  }

  @Test
  public void testGetUserUsageCountsWithAllFieldsSet() {
    TimeseriesAspectService mockService = mock(TimeseriesAspectService.class);
    OperationContext mockOpContext = mock(OperationContext.class);
    Filter filter = mock(Filter.class);

    when(mockService.getAggregatedStats(
            any(),
            Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
            Mockito.eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
            any(),
            any(Filter.class),
            any()))
        .thenReturn(
            new GenericTable()
                .setRows(
                    new StringArrayArray(
                        new StringArray(
                            ImmutableList.of(TEST_USER_URN_1, "10", "20", "30", "1", "1", "1")),
                        new StringArray(
                            ImmutableList.of(TEST_USER_URN_2, "50", "40", "60", "1", "1", "1"))))
                .setColumnNames(new StringArray())
                .setColumnTypes(new StringArray()));

    List<DashboardUserUsageCounts> result =
        DashboardUsageStatsUtils.getUserUsageCounts(mockOpContext, filter, mockService);

    Assert.assertEquals(result.size(), 2);
    // Sorted descending by usageCount: user2 (50) before user1 (10)
    Assert.assertEquals(result.get(0).getUser().getUrn(), TEST_USER_URN_2);
    Assert.assertEquals((int) result.get(0).getUsageCount(), 50);
    Assert.assertEquals((int) result.get(0).getViewsCount(), 40);
    Assert.assertEquals((int) result.get(0).getExecutionsCount(), 60);

    Assert.assertEquals(result.get(1).getUser().getUrn(), TEST_USER_URN_1);
    Assert.assertEquals((int) result.get(1).getUsageCount(), 10);
    Assert.assertEquals((int) result.get(1).getViewsCount(), 20);
    Assert.assertEquals((int) result.get(1).getExecutionsCount(), 30);
  }

  // --- buildSharedUsageFilter ---

  @Test
  public void testBuildSharedUsageFilterNoUrn() {
    Filter filter = DashboardUsageStatsUtils.buildSharedUsageFilter(null, null, false);
    CriterionArray criteria = filter.getOr().get(0).getAnd();
    boolean hasUrn =
        criteria.stream().anyMatch(c -> DashboardUsageStatsUtils.ES_FIELD_URN.equals(c.getField()));
    Assert.assertFalse(hasUrn, "buildSharedUsageFilter must not include a URN criterion");
  }

  @Test
  public void testBuildSharedUsageFilterCriteriaCount() {
    // no time range, byBucket=false → 1 criterion (eventGranularity is null)
    CriterionArray noRange =
        DashboardUsageStatsUtils.buildSharedUsageFilter(null, null, false).getOr().get(0).getAnd();
    Assert.assertEquals(noRange.size(), 1);

    // with time range, byBucket=true → 3 criteria (start, end, eventGranularity contains "unit")
    CriterionArray withRange =
        DashboardUsageStatsUtils.buildSharedUsageFilter(1000L, 2000L, true).getOr().get(0).getAnd();
    Assert.assertEquals(withRange.size(), 3);
  }

  @Test
  public void testBuildSharedUsageFilterVsCreateUsageFilter() {
    // createUsageFilter adds exactly one extra URN criterion at the front
    CriterionArray shared =
        DashboardUsageStatsUtils.buildSharedUsageFilter(1000L, 2000L, true).getOr().get(0).getAnd();
    CriterionArray perUrn =
        DashboardUsageStatsUtils.createUsageFilter(TEST_URN, 1000L, 2000L, true)
            .getOr()
            .get(0)
            .getAnd();
    Assert.assertEquals(perUrn.size(), shared.size() + 1);
    Assert.assertEquals(perUrn.get(0).getField(), DashboardUsageStatsUtils.ES_FIELD_URN);
  }

  // --- getUniqueUserCountAggSpecs ---

  @Test
  public void testGetUniqueUserCountAggSpecs() {
    AggregationSpec[] specs = DashboardUsageStatsUtils.getUniqueUserCountAggSpecs();
    Assert.assertEquals(specs.length, 1);
    Assert.assertEquals(specs[0].getAggregationType(), AggregationType.CARDINALITY);
    Assert.assertEquals(specs[0].getFieldPath(), "userCounts.user");
  }

  // --- getTopUsersSumAggSpecs ---

  @Test
  public void testGetTopUsersSumAggSpecs() {
    AggregationSpec[] specs = DashboardUsageStatsUtils.getTopUsersSumAggSpecs();
    Assert.assertEquals(specs.length, 1);
    Assert.assertEquals(specs[0].getAggregationType(), AggregationType.SUM);
    Assert.assertEquals(specs[0].getFieldPath(), "userCounts.usageCount");
  }

  // --- getTopUsersGroupingBuckets ---

  @Test
  public void testGetTopUsersGroupingBuckets() {
    GroupingBucket[] buckets = DashboardUsageStatsUtils.getTopUsersGroupingBuckets(5);
    Assert.assertEquals(buckets.length, 1);
    GroupingBucket b = buckets[0];
    Assert.assertEquals(b.getType(), GroupingBucketType.STRING_GROUPING_BUCKET);
    Assert.assertEquals(b.getKey(), "userCounts.user");
    Assert.assertEquals((int) b.getSize(), 5);
    Assert.assertTrue(b.isOrderByMetric(), "orderByMetric must be true");
    Assert.assertFalse(b.isAscending(), "ascending must be false for top-N desc ordering");
  }

  @Test
  public void testGetTopUsersGroupingBucketsCustomSize() {
    GroupingBucket[] buckets = DashboardUsageStatsUtils.getTopUsersGroupingBuckets(10);
    Assert.assertEquals((int) buckets[0].getSize(), 10);
  }

  // --- buildTopUsersFromBatchAggResult ---

  @Test
  public void testBuildTopUsersFromBatchAggResultFiltersNullRows() {
    GenericTable table =
        new GenericTable()
            .setRows(
                new StringArrayArray(
                    new StringArray(ImmutableList.of(USER_URN_1, "10")),
                    new StringArray(ImmutableList.of(DashboardUsageStatsUtils.ES_NULL_VALUE, "5")),
                    new StringArray(ImmutableList.of(USER_URN_2, "3"))))
            .setColumnNames(new StringArray())
            .setColumnTypes(new StringArray());

    List<CorpUser> users = DashboardUsageStatsUtils.buildTopUsersFromBatchAggResult(table);

    Assert.assertEquals(users.size(), 2);
    Assert.assertEquals(users.get(0).getUrn(), USER_URN_1);
    Assert.assertEquals(users.get(1).getUrn(), USER_URN_2);
  }

  @Test
  public void testBuildTopUsersFromBatchAggResultEmptyTable() {
    GenericTable table =
        new GenericTable()
            .setRows(new StringArrayArray())
            .setColumnNames(new StringArray())
            .setColumnTypes(new StringArray());

    List<CorpUser> users = DashboardUsageStatsUtils.buildTopUsersFromBatchAggResult(table);
    Assert.assertTrue(users.isEmpty());
  }

  @Test
  public void testBuildTopUsersFromBatchAggResultAllNulls() {
    GenericTable table =
        new GenericTable()
            .setRows(
                new StringArrayArray(
                    new StringArray(ImmutableList.of(DashboardUsageStatsUtils.ES_NULL_VALUE, "10")),
                    new StringArray(ImmutableList.of(DashboardUsageStatsUtils.ES_NULL_VALUE, "5"))))
            .setColumnNames(new StringArray())
            .setColumnTypes(new StringArray());

    List<CorpUser> users = DashboardUsageStatsUtils.buildTopUsersFromBatchAggResult(table);
    Assert.assertTrue(users.isEmpty());
  }
}
