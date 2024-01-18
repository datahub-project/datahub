package com.linkedin.datahub.graphql.resolvers.dataset;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
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

public class DatasetStatsSummaryResolverTest {

  private static final Dataset TEST_SOURCE = new Dataset();
  private static final String TEST_DATASET_URN =
      "urn:li:dataset:(urn:li:dataPlatform:hive,test,PROD)";
  private static final String TEST_USER_URN_1 = "urn:li:corpuser:test1";
  private static final String TEST_USER_URN_2 = "urn:li:corpuser:test2";

  static {
    TEST_SOURCE.setUrn(TEST_DATASET_URN);
  }

  @Test
  public void testGetSuccess() throws Exception {
    // Init test UsageQueryResult
    UsageQueryResult testResult = new UsageQueryResult();
    testResult.setAggregations(
        new UsageQueryResultAggregations()
            .setUniqueUserCount(5)
            .setTotalSqlQueries(10)
            .setUsers(
                new UserUsageCountsArray(
                    ImmutableList.of(
                        new UserUsageCounts()
                            .setUser(UrnUtils.getUrn(TEST_USER_URN_1))
                            .setUserEmail("test1@gmail.com")
                            .setCount(20),
                        new UserUsageCounts()
                            .setUser(UrnUtils.getUrn(TEST_USER_URN_2))
                            .setUserEmail("test2@gmail.com")
                            .setCount(30)))));

    UsageClient mockClient = Mockito.mock(UsageClient.class);
    Mockito.when(
            mockClient.getUsageStats(
                Mockito.eq(TEST_DATASET_URN), Mockito.eq(UsageTimeRange.MONTH)))
        .thenReturn(testResult);

    // Execute resolver
    DatasetStatsSummaryResolver resolver = new DatasetStatsSummaryResolver(mockClient);
    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");
    Authorizer mockAuthorizer = Mockito.mock(Authorizer.class);
    AuthorizationResult mockAuthorizerResult = Mockito.mock(AuthorizationResult.class);
    Mockito.when(mockAuthorizerResult.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    Mockito.when(mockAuthorizer.authorize(Mockito.any())).thenReturn(mockAuthorizerResult);
    Mockito.when(mockContext.getAuthorizer()).thenReturn(mockAuthorizer);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DatasetStatsSummary result = resolver.get(mockEnv).get();

    // Validate Result
    Assert.assertEquals((int) result.getQueryCountLast30Days(), 10);
    Assert.assertEquals((int) result.getTopUsersLast30Days().size(), 2);
    Assert.assertEquals((String) result.getTopUsersLast30Days().get(0).getUrn(), TEST_USER_URN_2);
    Assert.assertEquals((String) result.getTopUsersLast30Days().get(1).getUrn(), TEST_USER_URN_1);
    Assert.assertEquals((int) result.getUniqueUserCountLast30Days(), 5);

    // Validate the cache. -- First return a new result.
    UsageQueryResult newResult = new UsageQueryResult();
    newResult.setAggregations(new UsageQueryResultAggregations());
    Mockito.when(
            mockClient.getUsageStats(
                Mockito.eq(TEST_DATASET_URN), Mockito.eq(UsageTimeRange.MONTH)))
        .thenReturn(newResult);

    // Then verify that the new result is _not_ returned (cache hit)
    DatasetStatsSummary cachedResult = resolver.get(mockEnv).get();
    Assert.assertEquals((int) cachedResult.getQueryCountLast30Days(), 10);
    Assert.assertEquals((int) cachedResult.getTopUsersLast30Days().size(), 2);
    Assert.assertEquals(
        (String) cachedResult.getTopUsersLast30Days().get(0).getUrn(), TEST_USER_URN_2);
    Assert.assertEquals(
        (String) cachedResult.getTopUsersLast30Days().get(1).getUrn(), TEST_USER_URN_1);
    Assert.assertEquals((int) cachedResult.getUniqueUserCountLast30Days(), 5);
  }

  @Test
  public void testGetException() throws Exception {
    // Init test UsageQueryResult
    UsageQueryResult testResult = new UsageQueryResult();
    testResult.setAggregations(
        new UsageQueryResultAggregations()
            .setUniqueUserCount(5)
            .setTotalSqlQueries(10)
            .setUsers(
                new UserUsageCountsArray(
                    ImmutableList.of(
                        new UserUsageCounts()
                            .setUser(UrnUtils.getUrn(TEST_USER_URN_1))
                            .setUserEmail("test1@gmail.com")
                            .setCount(20),
                        new UserUsageCounts()
                            .setUser(UrnUtils.getUrn(TEST_USER_URN_2))
                            .setUserEmail("test2@gmail.com")
                            .setCount(30)))));

    UsageClient mockClient = Mockito.mock(UsageClient.class);
    Mockito.when(
            mockClient.getUsageStats(
                Mockito.eq(TEST_DATASET_URN), Mockito.eq(UsageTimeRange.MONTH)))
        .thenThrow(RuntimeException.class);

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
}
