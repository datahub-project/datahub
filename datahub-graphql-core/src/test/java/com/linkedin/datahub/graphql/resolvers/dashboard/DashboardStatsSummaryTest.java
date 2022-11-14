package com.linkedin.datahub.graphql.resolvers.dashboard;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dashboard.DashboardUsageStatistics;
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringArrayArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dashboard;
import com.linkedin.datahub.graphql.generated.DashboardStatsSummary;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.resolvers.dataset.DatasetStatsSummaryResolver;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.EnvelopedAspect;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.timeseries.GenericTable;
import com.linkedin.usage.UsageClient;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import graphql.schema.DataFetchingEnvironment;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.resolvers.dashboard.DashboardUsageStatsUtils.*;


public class DashboardStatsSummaryTest {

  private static final Dashboard TEST_SOURCE = new Dashboard();
  private static final String TEST_DASHBOARD_URN = "urn:li:dashboard:(airflow,id)";
  private static final String TEST_USER_URN_1 = "urn:li:corpuser:test1";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:test2";

  static {
    TEST_SOURCE.setUrn(TEST_DASHBOARD_URN);
  }

  @Test
  public void testGetSuccess() throws Exception {

    TimeseriesAspectService mockClient = initTestAspectService();

    // Execute resolver
    DashboardStatsSummaryResolver resolver = new DashboardStatsSummaryResolver(mockClient);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DashboardStatsSummary result = resolver.get(mockEnv).get();

    // Validate Result
    Assert.assertEquals((int) result.getViewCount(), 20);
    Assert.assertEquals((int) result.getTopUsersLast30Days().size(), 2);
    Assert.assertEquals((String) result.getTopUsersLast30Days().get(0).getUrn(), TEST_USER_URN_2);
    Assert.assertEquals((String) result.getTopUsersLast30Days().get(1).getUrn(), TEST_USER_URN_1);
    Assert.assertEquals((int) result.getUniqueUserCountLast30Days(), 2);

    // Validate the cache. -- First return a new result.
    DashboardUsageStatistics newUsageStats = new DashboardUsageStatistics()
        .setTimestampMillis(0L)
        .setLastViewedAt(0L)
        .setExecutionsCount(10)
        .setFavoritesCount(5)
        .setViewsCount(40);
    EnvelopedAspect newResult = new EnvelopedAspect()
        .setAspect(GenericRecordUtils.serializeAspect(newUsageStats));
    Filter filterForLatestStats = createUsageFilter(TEST_DASHBOARD_URN, null, null, false);
    Mockito.when(mockClient.getAspectValues(
        Mockito.eq(UrnUtils.getUrn(TEST_DASHBOARD_URN)),
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.eq(1),
        Mockito.eq(null),
        Mockito.eq(filterForLatestStats)
    )).thenReturn(ImmutableList.of(newResult));

    // Then verify that the new result is _not_ returned (cache hit)
    DashboardStatsSummary cachedResult = resolver.get(mockEnv).get();
    Assert.assertEquals((int) cachedResult.getViewCount(), 20);
    Assert.assertEquals((int) cachedResult.getTopUsersLast30Days().size(), 2);
    Assert.assertEquals((String) cachedResult.getTopUsersLast30Days().get(0).getUrn(), TEST_USER_URN_2);
    Assert.assertEquals((String) cachedResult.getTopUsersLast30Days().get(1).getUrn(), TEST_USER_URN_1);
    Assert.assertEquals((int) cachedResult.getUniqueUserCountLast30Days(), 2);
  }

  @Test
  public void testGetException() throws Exception {
    // Init test UsageQueryResult
    UsageQueryResult testResult = new UsageQueryResult();
    testResult.setAggregations(new UsageQueryResultAggregations()
        .setUniqueUserCount(5)
        .setTotalSqlQueries(10)
        .setUsers(new UserUsageCountsArray(
            ImmutableList.of(
                new UserUsageCounts()
                    .setUser(UrnUtils.getUrn(TEST_USER_URN_1))
                    .setUserEmail("test1@gmail.com")
                    .setCount(20),
                new UserUsageCounts()
                    .setUser(UrnUtils.getUrn(TEST_USER_URN_2))
                    .setUserEmail("test2@gmail.com")
                    .setCount(30)
            )
        ))
    );

    UsageClient mockClient = Mockito.mock(UsageClient.class);
    Mockito.when(mockClient.getUsageStats(
        Mockito.eq(TEST_DASHBOARD_URN),
        Mockito.eq(UsageTimeRange.MONTH),
        Mockito.any(Authentication.class)
    )).thenThrow(RuntimeException.class);

    // Execute resolver
    DatasetStatsSummaryResolver resolver = new DatasetStatsSummaryResolver(mockClient);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    // The resolver should NOT throw.
    DatasetStatsSummary result = resolver.get(mockEnv).get();

    // Summary should be null
    Assert.assertNull(result);
  }

  private TimeseriesAspectService initTestAspectService() {

    TimeseriesAspectService mockClient = Mockito.mock(TimeseriesAspectService.class);

    // Mock fetching the latest absolute (snapshot) statistics
    DashboardUsageStatistics latestUsageStats = new DashboardUsageStatistics()
        .setTimestampMillis(0L)
        .setLastViewedAt(0L)
        .setExecutionsCount(10)
        .setFavoritesCount(5)
        .setViewsCount(20);
    EnvelopedAspect envelopedLatestStats = new EnvelopedAspect()
        .setAspect(GenericRecordUtils.serializeAspect(latestUsageStats));

    Filter filterForLatestStats = createUsageFilter(TEST_DASHBOARD_URN, null, null, false);
    Mockito.when(mockClient.getAspectValues(
        Mockito.eq(UrnUtils.getUrn(TEST_DASHBOARD_URN)),
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.eq(1),
        Mockito.eq(null),
        Mockito.eq(filterForLatestStats)
    )).thenReturn(
        ImmutableList.of(envelopedLatestStats)
    );

    Mockito.when(mockClient.getAggregatedStats(
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(Constants.DASHBOARD_USAGE_STATISTICS_ASPECT_NAME),
        Mockito.any(),
        Mockito.any(Filter.class),
        Mockito.any()
    )).thenReturn(
        new GenericTable().setRows(new StringArrayArray(
            new StringArray(ImmutableList.of(
                TEST_USER_URN_1, "10", "20", "30", "1", "1", "1"
            )),
            new StringArray(ImmutableList.of(
                TEST_USER_URN_2, "20", "30", "40", "1", "1", "1"
            ))
        ))
        .setColumnNames(new StringArray())
        .setColumnTypes(new StringArray())
    );

    return mockClient;
  }

}
