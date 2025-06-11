package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.UpdateRemoteExecutorPoolInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.executorpool.RemoteExecutorPoolState;
import com.linkedin.executorpool.RemoteExecutorPoolStatus;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateRemoteExecutorPoolResolverTest {

  private static final String TEST_POOL_URN_STR = "urn:li:remoteExecutorPool:testPool";
  private static final Urn TEST_POOL_URN;
  private static final String TEST_DESCRIPTION = "Updated description";
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";

  static {
    try {
      TEST_POOL_URN = Urn.createFromString(TEST_POOL_URN_STR);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private UpdateRemoteExecutorPoolResolver _resolver;
  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private QueryContext _mockContext = null;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);

    // Setup input
    UpdateRemoteExecutorPoolInput input = new UpdateRemoteExecutorPoolInput();
    input.setUrn(TEST_POOL_URN_STR);
    input.setDescription(TEST_DESCRIPTION);
    input.setReprovision(false); // Default to false
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new UpdateRemoteExecutorPoolResolver(_entityClient);
  }

  @Test
  public void testUpdatePoolSuccess() throws Exception {
    _mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Mock existing pool info
    RemoteExecutorPoolInfo existingPoolInfo = new RemoteExecutorPoolInfo();
    existingPoolInfo.setDescription("Old description");
    existingPoolInfo.setCreatedAt(1234567890L);

    EnvelopedAspect mockEnvelopedAspect = new EnvelopedAspect();
    mockEnvelopedAspect.setValue(new Aspect(existingPoolInfo.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, mockEnvelopedAspect);

    EntityResponse mockResponse = mock(EntityResponse.class);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(TEST_POOL_URN),
            any(Set.class),
            eq(false)))
        .thenReturn(mockResponse);

    // Execute resolver
    Boolean result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result
    assertTrue(result);

    // Verify ingest proposal was called with correct parameters
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(false));

    // Verify proposal contents
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), TEST_POOL_URN);
    assertEquals(proposal.getAspectName(), AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);

    RemoteExecutorPoolInfo updatedPoolInfo =
        new RemoteExecutorPoolInfo(mockEnvelopedAspect.getValue().data());
    updatedPoolInfo.setDescription(TEST_DESCRIPTION);
    assertEquals(
        proposal.getAspect(),
        MutationUtils.buildMetadataChangeProposalWithUrn(
                TEST_POOL_URN,
                AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
                updatedPoolInfo)
            .getAspect());
  }

  @Test
  public void testUpdatePoolNoPermissions() throws RemoteInvocationException {
    // Mock insufficient permissions
    _mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());

    // Verify no interactions with entity client
    verify(_entityClient, never())
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testUpdatePoolNotFound() throws Exception {
    // Mock permissions check
    _mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Mock pool not found
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(TEST_POOL_URN),
            any(Set.class),
            eq(false)))
        .thenReturn(null);

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdatePoolAspectNotFound() throws Exception {
    // Mock permissions check
    _mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Mock response without aspect
    EntityResponse mockResponse = mock(EntityResponse.class);
    EnvelopedAspectMap mockMap = new EnvelopedAspectMap();
    when(mockResponse.getAspects()).thenReturn(mockMap);

    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(TEST_POOL_URN),
            any(Set.class),
            eq(false)))
        .thenReturn(mockResponse);

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testUpdatePoolWithReprovisionNoState() throws Exception {
    // Mock permissions check
    _mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Mock existing pool info without state
    RemoteExecutorPoolInfo existingPoolInfo = new RemoteExecutorPoolInfo();
    existingPoolInfo.setDescription("Old description");
    existingPoolInfo.setCreatedAt(1234567890L);

    EnvelopedAspect mockEnvelopedAspect = new EnvelopedAspect();
    mockEnvelopedAspect.setValue(new Aspect(existingPoolInfo.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, mockEnvelopedAspect);

    EntityResponse mockResponse = mock(EntityResponse.class);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(TEST_POOL_URN),
            any(Set.class),
            eq(false)))
        .thenReturn(mockResponse);

    // Setup input with reprovision true
    UpdateRemoteExecutorPoolInput input = new UpdateRemoteExecutorPoolInput();
    input.setUrn(TEST_POOL_URN_STR);
    input.setReprovision(true);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Execute resolver
    Boolean result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result
    assertTrue(result);

    // Verify ingest proposal was called with correct parameters
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(false));

    // Verify proposal contents
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), TEST_POOL_URN);
    assertEquals(proposal.getAspectName(), AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);

    RemoteExecutorPoolInfo updatedPoolInfo =
        new RemoteExecutorPoolInfo(mockEnvelopedAspect.getValue().data());
    assertTrue(updatedPoolInfo.hasState());
    assertEquals(
        updatedPoolInfo.getState().getStatus(), RemoteExecutorPoolStatus.PROVISIONING_PENDING);
  }

  @Test
  public void testUpdatePoolWithReprovisionFailedState() throws Exception {
    // Mock permissions check
    _mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Mock existing pool info with failed state
    RemoteExecutorPoolInfo existingPoolInfo = new RemoteExecutorPoolInfo();
    existingPoolInfo.setDescription("Old description");
    existingPoolInfo.setCreatedAt(1234567890L);
    RemoteExecutorPoolState state = new RemoteExecutorPoolState();
    state.setStatus(RemoteExecutorPoolStatus.PROVISIONING_FAILED);
    existingPoolInfo.setState(state);

    EnvelopedAspect mockEnvelopedAspect = new EnvelopedAspect();
    mockEnvelopedAspect.setValue(new Aspect(existingPoolInfo.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, mockEnvelopedAspect);

    EntityResponse mockResponse = mock(EntityResponse.class);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(TEST_POOL_URN),
            any(Set.class),
            eq(false)))
        .thenReturn(mockResponse);

    // Setup input with reprovision true
    UpdateRemoteExecutorPoolInput input = new UpdateRemoteExecutorPoolInput();
    input.setUrn(TEST_POOL_URN_STR);
    input.setReprovision(true);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Execute resolver
    Boolean result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result
    assertTrue(result);

    // Verify ingest proposal was called with correct parameters
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(false));

    // Verify proposal contents
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), TEST_POOL_URN);
    assertEquals(proposal.getAspectName(), AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);

    RemoteExecutorPoolInfo updatedPoolInfo =
        new RemoteExecutorPoolInfo(mockEnvelopedAspect.getValue().data());
    assertTrue(updatedPoolInfo.hasState());
    assertEquals(
        updatedPoolInfo.getState().getStatus(), RemoteExecutorPoolStatus.PROVISIONING_PENDING);
  }

  @Test
  public void testUpdatePoolWithReprovisionNonFailedState() throws Exception {
    // Mock permissions check
    _mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(_mockContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Mock existing pool info with non-failed state
    RemoteExecutorPoolInfo existingPoolInfo = new RemoteExecutorPoolInfo();
    existingPoolInfo.setDescription("Old description");
    existingPoolInfo.setCreatedAt(1234567890L);
    RemoteExecutorPoolState state = new RemoteExecutorPoolState();
    state.setStatus(RemoteExecutorPoolStatus.PROVISIONING_PENDING);
    existingPoolInfo.setState(state);

    EnvelopedAspect mockEnvelopedAspect = new EnvelopedAspect();
    mockEnvelopedAspect.setValue(new Aspect(existingPoolInfo.data()));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME, mockEnvelopedAspect);

    EntityResponse mockResponse = mock(EntityResponse.class);
    when(mockResponse.getAspects()).thenReturn(aspectMap);
    when(_entityClient.getV2(
            any(OperationContext.class),
            eq(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME),
            eq(TEST_POOL_URN),
            any(Set.class),
            eq(false)))
        .thenReturn(mockResponse);

    // Setup input with reprovision true
    UpdateRemoteExecutorPoolInput input = new UpdateRemoteExecutorPoolInput();
    input.setUrn(TEST_POOL_URN_STR);
    input.setReprovision(true);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Execute resolver
    Boolean result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result
    assertTrue(result);

    // Verify ingest proposal was called with correct parameters
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(false));

    // Verify proposal contents
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), TEST_POOL_URN);
    assertEquals(proposal.getAspectName(), AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);

    RemoteExecutorPoolInfo updatedPoolInfo =
        new RemoteExecutorPoolInfo(mockEnvelopedAspect.getValue().data());
    assertTrue(updatedPoolInfo.hasState());
    assertEquals(
        updatedPoolInfo.getState().getStatus(), RemoteExecutorPoolStatus.PROVISIONING_PENDING);
  }
}
