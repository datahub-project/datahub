package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.datahub.graphql.resolvers.load.DatasetStatsSummaryBatchLoader;
import com.linkedin.metadata.client.UsageStatsJavaClient;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.CompletableFuture;
import org.dataloader.DataLoader;
import org.dataloader.DataLoaderRegistry;
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

    UsageStatsJavaClient mockClient = Mockito.mock(UsageStatsJavaClient.class);
    Mockito.when(
            mockClient.getUsageStats(
                any(OperationContext.class),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(UsageTimeRange.MONTH),
                Mockito.eq(null),
                Mockito.eq(null)))
        .thenReturn(testResult);

    // Execute resolver
    DatasetStatsSummaryResolver resolver = new DatasetStatsSummaryResolver(mockClient);
    QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getActorUrn()).thenReturn("urn:li:corpuser:test");

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
                any(OperationContext.class),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(UsageTimeRange.MONTH),
                Mockito.eq(null),
                Mockito.eq(null)))
        .thenReturn(newResult);
  }

  @Test
  public void testBatchLoadEnabledDelegatesToDataLoader() throws Exception {
    // With the flag on, the resolver must enqueue into the batch DataLoader and NOT call the
    // usage client directly (auth + fetching move into the loader).
    final UsageStatsJavaClient mockClient = Mockito.mock(UsageStatsJavaClient.class);

    final DatasetStatsSummary expected = new DatasetStatsSummary();
    expected.setQueryCountLast30Days(42);

    @SuppressWarnings("unchecked")
    final DataLoader<Urn, DatasetStatsSummary> mockLoader = Mockito.mock(DataLoader.class);
    Mockito.when(mockLoader.load(UrnUtils.getUrn(TEST_DATASET_URN)))
        .thenReturn(CompletableFuture.completedFuture(expected));
    final DataLoaderRegistry registry = Mockito.mock(DataLoaderRegistry.class);
    // doReturn avoids generic-inference issues on the generic getDataLoader(String) signature.
    Mockito.doReturn(mockLoader)
        .when(registry)
        .getDataLoader(DatasetStatsSummaryBatchLoader.LOADER_NAME);

    final FeatureFlags flags = new FeatureFlags();
    flags.setDatasetStatsSummaryBatchLoadEnabled(true);
    final DatasetStatsSummaryResolver resolver = new DatasetStatsSummaryResolver(mockClient, flags);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getContext()).thenReturn(Mockito.mock(QueryContext.class));
    Mockito.when(mockEnv.getDataLoaderRegistry()).thenReturn(registry);

    final DatasetStatsSummary result = resolver.get(mockEnv).get();

    Assert.assertEquals((int) result.getQueryCountLast30Days(), 42);
    Mockito.verify(mockLoader).load(UrnUtils.getUrn(TEST_DATASET_URN));
    Mockito.verifyNoInteractions(mockClient); // did not fall back to the per-URN path
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

    UsageStatsJavaClient mockClient = Mockito.mock(UsageStatsJavaClient.class);
    Mockito.when(
            mockClient.getUsageStats(
                any(OperationContext.class),
                Mockito.eq(TEST_DATASET_URN),
                Mockito.eq(UsageTimeRange.MONTH),
                Mockito.eq(null),
                Mockito.eq(null)))
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
