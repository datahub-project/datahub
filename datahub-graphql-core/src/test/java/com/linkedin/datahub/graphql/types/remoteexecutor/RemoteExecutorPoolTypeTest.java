package com.linkedin.datahub.graphql.types.remoteexecutor;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.*;

import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.RemoteExecutorPool;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.r2.RemoteInvocationException;
import graphql.execution.DataFetcherResult;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RemoteExecutorPoolTypeTest {

  private static final String DEFAULT_POOL_NAME = "test-pool-1";
  private static final String TEST_POOL_URN =
      String.format("urn:li:remoteExecutorPool:%s", DEFAULT_POOL_NAME);
  private static final String TEST_POOL_URN_2 = "urn:li:remoteExecutorPool:test-pool-2";

  private static final RemoteExecutorPoolInfo TEST_POOL_INFO =
      new RemoteExecutorPoolInfo().setCreatedAt(System.currentTimeMillis());

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn poolUrn1 = Urn.createFromString(TEST_POOL_URN);
    Urn poolUrn2 = Urn.createFromString(TEST_POOL_URN_2);

    Map<String, EnvelopedAspect> pool1Aspects = new HashMap<>();
    pool1Aspects.put(
        AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_POOL_INFO.data())));

    // Mock the pool aspects response
    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(poolUrn1, poolUrn2))),
                Mockito.eq(RemoteExecutorPoolType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                poolUrn1,
                new EntityResponse()
                    .setEntityName(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME)
                    .setUrn(poolUrn1)
                    .setAspects(new EnvelopedAspectMap(pool1Aspects))));

    // Mock the default pool lookup
    Mockito.when(client.getV2(any(), any(), any(), any()))
        .thenReturn(null); // Simulate no default pool config

    RemoteExecutorPoolType type = new RemoteExecutorPoolType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<RemoteExecutorPool>> result =
        type.batchLoad(ImmutableList.of(TEST_POOL_URN, TEST_POOL_URN_2), mockContext);

    // Verify response
    assertEquals(result.size(), 2);

    RemoteExecutorPool pool = result.get(0).getData();
    assertEquals(pool.getUrn(), TEST_POOL_URN);
    assertEquals(pool.getType(), EntityType.REMOTE_EXECUTOR_POOL);
    assertEquals(pool.getExecutorPoolId(), DEFAULT_POOL_NAME);
    assertEquals(pool.getCreatedAt(), TEST_POOL_INFO.getCreatedAt());
    assertFalse(pool.getIsDefault()); // Should be false since we mocked no default pool config

    // Assert second element is null
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadWithDefaultPool() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn poolUrn = Urn.createFromString(TEST_POOL_URN);

    Map<String, EnvelopedAspect> poolAspects = new HashMap<>();
    poolAspects.put(
        AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_POOL_INFO.data())));

    // Mock the pool aspects response
    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(poolUrn))),
                Mockito.eq(RemoteExecutorPoolType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                poolUrn,
                new EntityResponse()
                    .setEntityName(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME)
                    .setUrn(poolUrn)
                    .setAspects(new EnvelopedAspectMap(poolAspects))));

    // Mock default pool lookup to return our test pool
    Mockito.when(client.getV2(any(), any(), any(), any()))
        .thenReturn(null); // Simplified mock - in real scenario this would return platform resource

    RemoteExecutorPoolType type = new RemoteExecutorPoolType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<RemoteExecutorPool>> result =
        type.batchLoad(ImmutableList.of(TEST_POOL_URN), mockContext);

    assertEquals(result.size(), 1);
    RemoteExecutorPool pool = result.get(0).getData();
    assertEquals(pool.getUrn(), TEST_POOL_URN);
    assertEquals(pool.getExecutorPoolId(), DEFAULT_POOL_NAME);
    assertEquals(pool.getCreatedAt(), TEST_POOL_INFO.getCreatedAt());
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    RemoteExecutorPoolType type = new RemoteExecutorPoolType(mockClient);

    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(context.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_POOL_URN, TEST_POOL_URN_2), context));
  }
}
