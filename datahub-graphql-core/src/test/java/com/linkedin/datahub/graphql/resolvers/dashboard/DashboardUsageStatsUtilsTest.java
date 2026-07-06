package com.linkedin.datahub.graphql.resolvers.dashboard;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.generated.DashboardUserUsageCounts;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.timeseries.GenericTable;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

public class DashboardUsageStatsUtilsTest {

  private static final String TEST_USER_URN_1 = "urn:li:corpuser:test1";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:test2";

  /**
   * Reproduces the scenario from CAT-1520: customer emits dashboardUsageStatistics with per-user
   * viewsCount but no usageCount. Verifies that viewsCount is correctly populated on the returned
   * DashboardUserUsageCounts objects.
   */
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
    // viewsCount MUST be populated — this is the bug fix for CAT-1520
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
}
