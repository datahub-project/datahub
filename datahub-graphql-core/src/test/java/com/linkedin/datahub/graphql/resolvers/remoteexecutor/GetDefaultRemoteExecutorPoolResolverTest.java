package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DefaultRemoteExecutorPoolResult;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorglobalconfig.RemoteExecutorPoolGlobalConfig;
import com.linkedin.metadata.AcrylConstants;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.Set;
import org.mockito.ArgumentMatchers;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GetDefaultRemoteExecutorPoolResolverTest {

  private static final String DEFAULT_POOL_NAME = "defaultPool";
  private GetDefaultRemoteExecutorPoolResolver _resolver;
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);

    final QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    _resolver = new GetDefaultRemoteExecutorPoolResolver(_entityClient);
  }

  @Test
  public void testGetDefaultPoolSuccess() throws Exception {
    // Mock the global config response
    EntityResponse response = new EntityResponse();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    RemoteExecutorPoolGlobalConfig config = new RemoteExecutorPoolGlobalConfig();
    config.setDefaultExecutorPoolId(DEFAULT_POOL_NAME);

    // Create a map to represent the aspect
    Aspect aspect = new Aspect(config.data());
    envelopedAspect.setValue(aspect);

    response.setAspects(
        new EnvelopedAspectMap(
            Collections.singletonMap(
                AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME, envelopedAspect)));

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(Urn.class),
            ArgumentMatchers.<Set<String>>any()))
        .thenReturn(response);

    // Execute resolver and verify result
    final DefaultRemoteExecutorPoolResult result = _resolver.get(_dataFetchingEnvironment).join();

    assertNotNull(result);
    assertNotNull(result.getPool());
    assertEquals(result.getPool().getType(), EntityType.REMOTE_EXECUTOR_POOL);
    assertEquals(
        result.getPool().getUrn(),
        new Urn(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, DEFAULT_POOL_NAME).toString());
  }

  @Test
  public void testGetDefaultPoolNoConfig() throws Exception {
    // Mock empty response from entity client
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(Urn.class),
            ArgumentMatchers.<Set<String>>any()))
        .thenReturn(null);

    // Execute resolver and verify empty result
    final DefaultRemoteExecutorPoolResult result = _resolver.get(_dataFetchingEnvironment).join();

    assertNotNull(result);
    assertNull(result.getPool());
  }

  @Test
  public void testGetDefaultPoolNoDefaultName() throws Exception {
    // Mock response with no default pool name
    EntityResponse response = new EntityResponse();
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    response.setAspects(map);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(Urn.class),
            ArgumentMatchers.<Set<String>>any()))
        .thenReturn(response);

    // Execute resolver and verify empty result
    final DefaultRemoteExecutorPoolResult result = _resolver.get(_dataFetchingEnvironment).join();

    assertNotNull(result);
    assertNull(result.getPool());
  }

  @Test
  public void testGetDefaultPoolException() throws Exception {
    // Mock exception from entity client
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            any(Urn.class),
            ArgumentMatchers.<Set<String>>any()))
        .thenThrow(new RuntimeException("Test exception"));

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}
