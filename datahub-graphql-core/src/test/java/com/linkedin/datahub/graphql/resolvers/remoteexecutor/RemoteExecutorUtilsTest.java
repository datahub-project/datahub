package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME;
import static com.linkedin.metadata.AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME;
import static org.mockito.Mockito.*;
import static org.testng.AssertJUnit.assertEquals;
import static org.testng.AssertJUnit.assertNull;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorglobalconfig.RemoteExecutorPoolGlobalConfig;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoteExecutorUtilsTest {

  @Mock private EntityClient entityClient;
  @Mock private OperationContext operationContext;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testTryGetDefaultExecutorPoolId_Success() throws Exception {
    // Arrange
    String expectedPoolName = "default-pool";
    RemoteExecutorPoolGlobalConfig config = new RemoteExecutorPoolGlobalConfig();
    config.setDefaultPoolName(expectedPoolName);

    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect()
            .setValue(new Aspect(config.data()))
            .setName(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME);

    EntityResponse response = mock(EntityResponse.class);
    when(response.getAspects())
        .thenReturn(
            new EnvelopedAspectMap(
                ImmutableMap.of(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME, envelopedAspect)));

    when(entityClient.getV2(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME))))
        .thenReturn(response);

    // Act
    String result = RemoteExecutorUtils.tryGetDefaultExecutorPoolId(entityClient, operationContext);

    // Assert
    assertEquals(expectedPoolName, result);
  }

  @Test
  public void testTryGetDefaultExecutorPoolId_NullResponse() throws Exception {
    // Arrange
    when(entityClient.getV2(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME))))
        .thenReturn(null);

    // Act
    String result = RemoteExecutorUtils.tryGetDefaultExecutorPoolId(entityClient, operationContext);

    // Assert
    assertNull(result);
  }

  @Test
  public void testTryGetDefaultExecutorPoolId_NullAspect() throws Exception {
    // Arrange
    EntityResponse response = mock(EntityResponse.class);
    when(response.getAspects()).thenReturn(new EnvelopedAspectMap(ImmutableMap.of()));

    when(entityClient.getV2(
            eq(operationContext),
            eq(REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME))))
        .thenReturn(response);

    // Act
    String result = RemoteExecutorUtils.tryGetDefaultExecutorPoolId(entityClient, operationContext);

    // Assert
    assertNull(result);
  }
}
