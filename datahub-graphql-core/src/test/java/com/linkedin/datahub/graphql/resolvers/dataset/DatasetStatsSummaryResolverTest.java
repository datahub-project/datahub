package com.linkedin.datahub.graphql.resolvers.dataset;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.nullable;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Dataset;
import com.linkedin.datahub.graphql.generated.DatasetStatsSummary;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.metadata.client.UsageStatsJavaClient;
import com.linkedin.metadata.search.features.StorageFeatures;
import com.linkedin.metadata.search.features.UsageFeatures;
import com.linkedin.usage.UsageQueryResult;
import com.linkedin.usage.UsageQueryResultAggregations;
import com.linkedin.usage.UsageTimeRange;
import com.linkedin.usage.UserUsageCounts;
import com.linkedin.usage.UserUsageCountsArray;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
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
  public void testGetSuccessNoOfflineFeatures() throws Exception {
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
    SystemEntityClient mockEntityClient = initMockEntityClient();
    DatasetStatsSummaryResolver resolver =
        new DatasetStatsSummaryResolver(mockEntityClient, mockClient);
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
  public void testGetSuccessOfflineFeatures() throws Exception {
    // Init test UsageQueryResult
    UsageStatsJavaClient mockClient = Mockito.mock(UsageStatsJavaClient.class);

    // Execute resolver
    final UsageFeatures mockUsageFeatures = new UsageFeatures();
    mockUsageFeatures.setUsageCountLast30Days(24L);
    mockUsageFeatures.setQueryCountPercentileLast30Days(40);
    mockUsageFeatures.setTopUsersLast30Days(
        new UrnArray(ImmutableList.of(UrnUtils.getUrn(TEST_USER_URN_1))));
    mockUsageFeatures.setUniqueUserCountLast30Days(20);
    mockUsageFeatures.setUniqueUserPercentileLast30Days(75);
    mockUsageFeatures.setWriteCountLast30Days(34);
    mockUsageFeatures.setWriteCountPercentileLast30Days(32);

    final StorageFeatures mockStorageFeatures = new StorageFeatures();
    mockStorageFeatures.setRowCount(1L);
    mockStorageFeatures.setRowCountPercentile(50);
    mockStorageFeatures.setSizeInBytes(5);
    mockStorageFeatures.setSizeInBytesPercentile(10L);

    SystemEntityClient mockEntityClient =
        initMockEntityClient(mockUsageFeatures, mockStorageFeatures);
    DatasetStatsSummaryResolver resolver =
        new DatasetStatsSummaryResolver(mockEntityClient, mockClient);
    QueryContext mockContext = getMockAllowContext();

    DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getSource()).thenReturn(TEST_SOURCE);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);

    DatasetStatsSummary result = resolver.get(mockEnv).join();

    // Validate Result
    Assert.assertEquals(
        (int) result.getQueryCountLast30Days(),
        mockUsageFeatures.getUsageCountLast30Days().intValue());
    Assert.assertEquals(
        result.getQueryCountPercentileLast30Days(),
        mockUsageFeatures.getQueryCountPercentileLast30Days());
    Assert.assertEquals(
        (int) result.getUniqueUserCountLast30Days(),
        mockUsageFeatures.getUniqueUserCountLast30Days().intValue());
    Assert.assertEquals(
        result.getUniqueUserPercentileLast30Days(),
        mockUsageFeatures.getUniqueUserPercentileLast30Days());
    Assert.assertEquals(result.getTopUsersLast30Days().size(), 1);
    Assert.assertEquals(result.getTopUsersLast30Days().get(0).getUrn(), TEST_USER_URN_1);
    Assert.assertEquals(
        (int) result.getUpdateCountLast30Days(),
        mockUsageFeatures.getWriteCountLast30Days().intValue());
    Assert.assertEquals(
        result.getUpdateCountPercentileLast30Days(),
        mockUsageFeatures.getWriteCountPercentileLast30Days());

    // Storage Features
    Assert.assertEquals(result.getRowCount(), mockStorageFeatures.getRowCount());
    Assert.assertEquals(
        result.getRowCountPercentile(), mockStorageFeatures.getRowCountPercentile());
    Assert.assertEquals(result.getSizeInBytes(), mockStorageFeatures.getSizeInBytes());
    Assert.assertEquals(
        (int) result.getSizeInBytesPercentile(),
        mockStorageFeatures.getSizeInBytesPercentile().intValue());
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
    SystemEntityClient mockEntityClient = initMockEntityClient();
    DatasetStatsSummaryResolver resolver =
        new DatasetStatsSummaryResolver(mockEntityClient, mockClient);
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

  private SystemEntityClient initMockEntityClient(
      @Nonnull final UsageFeatures usageFeatures, @Nonnull final StorageFeatures storageFeatures)
      throws Exception {
    SystemEntityClient client = Mockito.mock(SystemEntityClient.class);
    Urn testUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    Mockito.when(
            client.getV2(
                nullable(OperationContext.class),
                Mockito.eq(testUrn),
                Mockito.eq(
                    ImmutableSet.of(USAGE_FEATURES_ASPECT_NAME, STORAGE_FEATURES_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(testUrn)
                .setEntityName(DATASET_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            USAGE_FEATURES_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(usageFeatures.data())),
                            STORAGE_FEATURES_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(storageFeatures.data()))))));
    return client;
  }

  private SystemEntityClient initMockEntityClient() throws Exception {
    SystemEntityClient client = Mockito.mock(SystemEntityClient.class);
    Urn testUrn = UrnUtils.getUrn(TEST_DATASET_URN);
    Mockito.when(
            client.getV2(
                any(OperationContext.class),
                Mockito.eq(testUrn),
                Mockito.eq(
                    ImmutableSet.of(USAGE_FEATURES_ASPECT_NAME, STORAGE_FEATURES_ASPECT_NAME))))
        .thenReturn(null);
    return client;
  }
}
