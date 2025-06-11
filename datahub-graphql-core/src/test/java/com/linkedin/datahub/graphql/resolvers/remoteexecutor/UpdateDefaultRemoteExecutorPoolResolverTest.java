package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorglobalconfig.RemoteExecutorPoolGlobalConfig;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateDefaultRemoteExecutorPoolResolverTest {

  private static final String TEST_POOL_URN = "urn:li:remoteExecutorPool:testPool";
  private UpdateDefaultRemoteExecutorPoolResolver _resolver;
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private QueryContext _mockContext;
  private OperationContext _operationContext;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _mockContext = getMockAllowContext();
    _operationContext = mock(OperationContext.class);

    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(TEST_POOL_URN);
    when(_mockContext.getOperationContext()).thenReturn(_operationContext);

    // Mock the operation context for ingestion management permissions
    when(_operationContext.authorize(anyString(), any(EntitySpec.class), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    _resolver = new UpdateDefaultRemoteExecutorPoolResolver(_entityClient);
  }

  @Test
  public void testUpdateDefaultPoolSuccess() throws Exception {
    // Mock permissions check for managing ingestion
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    // Mock pool existence check
    EntityResponse poolResponse = new EntityResponse();
    final RemoteExecutorPoolInfo poolInfo = new RemoteExecutorPoolInfo();
    final EnvelopedAspectMap poolMap = new EnvelopedAspectMap();
    poolMap.put(
        AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(poolInfo.data())));
    poolResponse.setAspects(poolMap);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(UrnUtils.getUrn(TEST_POOL_URN)),
            eq(Set.of(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME))))
        .thenReturn(poolResponse);

    // Mock global config response with existing pool name
    EntityResponse configResponse = new EntityResponse();
    final RemoteExecutorPoolGlobalConfig existingConfig = new RemoteExecutorPoolGlobalConfig();
    existingConfig.setDefaultExecutorPoolId("oldDefaultPool");
    final EnvelopedAspectMap configMap = new EnvelopedAspectMap();
    configMap.put(
        AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(existingConfig.data())));
    configResponse.setAspects(configMap);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_GLOBAL_CONFIG_ENTITY_NAME),
            eq(UrnUtils.getUrn(AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_SINGLETON_URN)),
            eq(ImmutableSet.of(AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME))))
        .thenReturn(configResponse);

    // Execute resolver
    final Boolean result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result
    assertTrue(result);

    // Verify ingest proposal was called with correct parameters
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(false));

    // Verify proposal contents
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(
        proposal.getEntityUrn(),
        UrnUtils.getUrn(AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_SINGLETON_URN));
    assertEquals(
        proposal.getAspectName(), AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME);

    // Verify the new pool config has the updated default pool name
    RemoteExecutorPoolGlobalConfig expectedConfig =
        new RemoteExecutorPoolGlobalConfig(existingConfig.data());
    expectedConfig.setDefaultExecutorPoolId(UrnUtils.getUrn(TEST_POOL_URN).getId());
    Assert.assertEquals(
        proposal.getAspect(),
        MutationUtils.buildMetadataChangeProposalWithUrn(
                Urn.createFromString(
                    AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_SINGLETON_URN),
                AcrylConstants.REMOTE_EXECUTOR_POOL_GLOBAL_CONFIG_ASPECT_NAME,
                expectedConfig)
            .getAspect());
  }

  @Test
  public void testUpdateDefaultPoolNoPermissions() throws RemoteInvocationException {
    // Mock insufficient permissions
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.DENY, "message"));

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());

    // Verify no interactions with entity client
    verify(_entityClient, never())
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testUpdateDefaultPoolNotFound() throws Exception {
    // Mock permissions check
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    // Mock pool not found
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            any(Urn.class),
            ArgumentMatchers.<Set<String>>any()))
        .thenReturn(null);

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());

    // Verify no proposal was ingested
    verify(_entityClient, never())
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testUpdateDefaultPoolEntityClientException() throws Exception {
    // Mock permissions check
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class), any()))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    // Mock entity client exception
    when(_entityClient.getV2(
            any(OperationContext.class),
            anyString(),
            any(Urn.class),
            ArgumentMatchers.<Set<String>>any()))
        .thenThrow(new RuntimeException("Test exception"));

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}
