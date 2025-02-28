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
import com.linkedin.datahub.graphql.generated.RemoteExecutor;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executor.RemoteExecutorStatus;
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

public class RemoteExecutorTypeTest {

  private static final String TEST_EXECUTOR_URN = "urn:li:remoteExecutor:guid-1";
  private static final String TEST_EXECUTOR_URN_2 = "urn:li:remoteExecutor:guid-2";

  private static final RemoteExecutorStatus TEST_EXECUTOR_STATUS =
      new RemoteExecutorStatus()
          .setExecutorPoolId("executor-1")
          .setExecutorReleaseVersion("1.0.0")
          .setExecutorAddress("localhost:8080")
          .setExecutorHostname("test-host")
          .setExecutorUptime(1000L)
          .setExecutorExpired(false)
          .setExecutorStopped(false)
          .setExecutorEmbedded(true)
          .setExecutorInternal(false)
          .setLogDeliveryEnabled(true)
          .setReportedAt(System.currentTimeMillis());

  @Test
  public void testBatchLoad() throws Exception {
    EntityClient client = Mockito.mock(EntityClient.class);

    Urn executorUrn1 = Urn.createFromString(TEST_EXECUTOR_URN);
    Urn executorUrn2 = Urn.createFromString(TEST_EXECUTOR_URN_2);

    Map<String, EnvelopedAspect> executor1Aspects = new HashMap<>();
    executor1Aspects.put(
        AcrylConstants.REMOTE_EXECUTOR_STATUS_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(TEST_EXECUTOR_STATUS.data())));

    Mockito.when(
            client.batchGetV2(
                any(),
                Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME),
                Mockito.eq(new HashSet<>(ImmutableSet.of(executorUrn1, executorUrn2))),
                Mockito.eq(RemoteExecutorType.ASPECTS_TO_FETCH)))
        .thenReturn(
            ImmutableMap.of(
                executorUrn1,
                new EntityResponse()
                    .setEntityName(AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME)
                    .setUrn(executorUrn1)
                    .setAspects(new EnvelopedAspectMap(executor1Aspects))));

    RemoteExecutorType type = new RemoteExecutorType(client);

    QueryContext mockContext = Mockito.mock(QueryContext.class);
    Mockito.when(mockContext.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    Mockito.when(mockContext.getOperationContext())
        .thenReturn(TestOperationContexts.systemContextNoSearchAuthorization());

    List<DataFetcherResult<RemoteExecutor>> result =
        type.batchLoad(ImmutableList.of(TEST_EXECUTOR_URN, TEST_EXECUTOR_URN_2), mockContext);

    // Verify response
    Mockito.verify(client, Mockito.times(1))
        .batchGetV2(
            any(),
            Mockito.eq(AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME),
            Mockito.eq(ImmutableSet.of(executorUrn1, executorUrn2)),
            Mockito.eq(RemoteExecutorType.ASPECTS_TO_FETCH));

    assertEquals(result.size(), 2);

    RemoteExecutor executor = result.get(0).getData();
    assertEquals(executor.getUrn(), TEST_EXECUTOR_URN);
    assertEquals(executor.getType(), EntityType.REMOTE_EXECUTOR);
    assertEquals(executor.getExecutorPoolId(), TEST_EXECUTOR_STATUS.getExecutorPoolId());
    assertEquals(
        executor.getExecutorReleaseVersion(), TEST_EXECUTOR_STATUS.getExecutorReleaseVersion());
    assertEquals(executor.getExecutorAddress(), TEST_EXECUTOR_STATUS.getExecutorAddress());
    assertEquals(executor.getExecutorHostname(), TEST_EXECUTOR_STATUS.getExecutorHostname());
    assertEquals(executor.getExecutorUptime(), (double) TEST_EXECUTOR_STATUS.getExecutorUptime());
    assertEquals(executor.getExecutorExpired(), TEST_EXECUTOR_STATUS.isExecutorExpired());
    assertEquals(executor.getExecutorStopped(), TEST_EXECUTOR_STATUS.isExecutorStopped());
    assertEquals(executor.getExecutorEmbedded(), TEST_EXECUTOR_STATUS.isExecutorEmbedded());
    assertEquals(executor.getExecutorInternal(), TEST_EXECUTOR_STATUS.isExecutorInternal());
    assertEquals(executor.getLogDeliveryEnabled(), TEST_EXECUTOR_STATUS.isLogDeliveryEnabled());
    assertEquals(executor.getReportedAt(), TEST_EXECUTOR_STATUS.getReportedAt());

    // Assert second element is null
    assertNull(result.get(1));
  }

  @Test
  public void testBatchLoadClientException() throws Exception {
    EntityClient mockClient = Mockito.mock(EntityClient.class);
    Mockito.doThrow(RemoteInvocationException.class)
        .when(mockClient)
        .batchGetV2(any(), Mockito.anyString(), Mockito.anySet(), Mockito.anySet());
    RemoteExecutorType type = new RemoteExecutorType(mockClient);

    QueryContext context = Mockito.mock(QueryContext.class);
    Mockito.when(context.getAuthentication()).thenReturn(Mockito.mock(Authentication.class));
    assertThrows(
        RuntimeException.class,
        () -> type.batchLoad(ImmutableList.of(TEST_EXECUTOR_URN, TEST_EXECUTOR_URN_2), context));
  }
}
