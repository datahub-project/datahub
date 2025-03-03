package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_STATUS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.when;
import static org.testng.Assert.expectThrows;
import static org.testng.AssertJUnit.*;

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
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetRemoteExecutorResolverTest {

  @Mock private EntityClient entityClient;
  @Mock private DataFetchingEnvironment environment;
  @Mock private QueryContext queryContext;
  @Mock private OperationContext operationContext;

  private GetRemoteExecutorResolver resolver;

  private static EnvelopedAspect mockRemoteExecutorStatusAspect() {
    RemoteExecutorStatus status = new RemoteExecutorStatus();
    status.setExecutorPoolId("test-pool");
    status.setExecutorReleaseVersion("1.0.0");
    status.setExecutorAddress("localhost:8080");
    status.setExecutorHostname("test-host");
    status.setExecutorUptime(3600L);
    status.setExecutorExpired(false);
    status.setExecutorStopped(false);
    status.setExecutorEmbedded(true);
    status.setExecutorInternal(false);
    status.setLogDeliveryEnabled(true);
    status.setReportedAt(1234567890L);

    // Create EnvelopedAspect and EnvelopedAspectMap
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(status.data()));
    return envelopedAspect;
  }

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    resolver = new GetRemoteExecutorResolver(entityClient);
    when(environment.getContext()).thenReturn(queryContext);
    when(queryContext.getOperationContext()).thenReturn(operationContext);
  }

  @Test
  public void testGetRemoteExecutor() throws Exception {
    // Setup input parameters
    String urnStr = "urn:li:remoteExecutor:test-executor";
    Urn urn = Urn.createFromString(urnStr);
    when(environment.getArgument("urn")).thenReturn(urnStr);

    // Create and setup RemoteExecutorStatus mock
    EnvelopedAspect envelopedAspect = mockRemoteExecutorStatusAspect();

    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(
            new HashMap<String, EnvelopedAspect>() {
              {
                put(REMOTE_EXECUTOR_STATUS_ASPECT_NAME, envelopedAspect);
              }
            });

    // Create EntityResponse
    EntityResponse entityResponse = new EntityResponse();
    entityResponse.setUrn(urn);
    entityResponse.setAspects(aspectMap);

    // Setup mock response
    when(entityClient.getV2(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_ENTITY_NAME),
            eq(urn),
            eq(Set.of(REMOTE_EXECUTOR_STATUS_ASPECT_NAME)),
            eq(false)))
        .thenReturn(entityResponse);

    // Execute resolver
    CompletableFuture<RemoteExecutor> futureResult = resolver.get(environment);
    RemoteExecutor result = futureResult.get();

    // Verify results
    assertNotNull(result);
    assertEquals(urnStr, result.getUrn());
    assertEquals(EntityType.REMOTE_EXECUTOR, result.getType());
    assertEquals("test-pool", result.getExecutorPoolId());
    assertEquals("1.0.0", result.getExecutorReleaseVersion());
    assertEquals("localhost:8080", result.getExecutorAddress());
    assertEquals("test-host", result.getExecutorHostname());
    assertEquals(3600L, (long) result.getExecutorUptime());
    assertFalse(result.getExecutorExpired());
    assertFalse(result.getExecutorStopped());
    assertTrue(result.getExecutorEmbedded());
    assertFalse(result.getExecutorInternal());
    assertTrue(result.getLogDeliveryEnabled());
    assertEquals(1234567890L, result.getReportedAt().longValue());
  }

  @Test
  public void testGetRemoteExecutorNotFound() throws Exception {
    // Setup input parameters
    String urnStr = "urn:li:remoteExecutor:non-existent";
    Urn urn = Urn.createFromString(urnStr);
    when(environment.getArgument("urn")).thenReturn(urnStr);

    // Setup null response to simulate not found
    when(entityClient.getV2(any(), any(), any(), any(), anyBoolean())).thenReturn(null);

    // Execute and verify exception
    CompletableFuture<RemoteExecutor> futureResult = resolver.get(environment);

    ExecutionException exception = expectThrows(ExecutionException.class, futureResult::get);
    assertTrue(exception.getCause() instanceof RuntimeException);
    assertEquals(
        String.format("Could not find remote executor with urn %s", urn),
        exception.getCause().getCause().getMessage());
  }

  @Test
  public void testGetRemoteExecutorWithError() throws Exception {
    // Setup input parameters
    String urnStr = "urn:li:remoteExecutor:test-executor";
    when(environment.getArgument("urn")).thenReturn(urnStr);

    // Simulate error in entityClient
    when(entityClient.getV2(any(), any(), any(), any(), anyBoolean()))
        .thenThrow(new RemoteInvocationException("Test error"));

    // Execute and verify exception
    CompletableFuture<RemoteExecutor> futureResult = resolver.get(environment);

    ExecutionException exception = expectThrows(ExecutionException.class, () -> futureResult.get());
    assertTrue(exception.getCause() instanceof RuntimeException);
    assertEquals("Failed to get remote executor", exception.getCause().getMessage());
  }
}
