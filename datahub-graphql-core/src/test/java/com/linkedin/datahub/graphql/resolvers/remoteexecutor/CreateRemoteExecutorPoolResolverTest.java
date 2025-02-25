package com.linkedin.datahub.graphql.resolvers.remoteexecutor;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.CreateRemoteExecutorPoolInput;
import com.linkedin.datahub.graphql.resolvers.mutate.MutationUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.executorpool.RemoteExecutorPoolInfo;
import com.linkedin.metadata.AcrylConstants;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.r2.RemoteInvocationException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateRemoteExecutorPoolResolverTest {

  private static final String TEST_POOL_NAME = "testPool";
  private static final Urn TEST_POOL_URN =
      Urn.createFromTuple(AcrylConstants.REMOTE_EXECUTOR_POOL_ENTITY_NAME, "testPool");
  private static final String TEST_ACTOR_URN = "urn:li:corpuser:test";
  private static final long TEST_TIMESTAMP = 1234567890L;

  private CreateRemoteExecutorPoolResolver _resolver;
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
    when(_mockContext.getOperationContext()).thenReturn(_operationContext);
    when(_mockContext.getActorUrn()).thenReturn(TEST_ACTOR_URN);

    // Setup input
    CreateRemoteExecutorPoolInput input = new CreateRemoteExecutorPoolInput();
    input.setExecutorPoolId(TEST_POOL_NAME);
    input.setIsDefault(false);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    _resolver = new CreateRemoteExecutorPoolResolver(_entityClient, () -> TEST_TIMESTAMP);
  }

  @Test
  public void testCreatePoolSuccess() throws Exception {
    // Mock permissions check
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    // Execute resolver
    final String result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result is the correct URN
    assertEquals(result, TEST_POOL_URN.toString());

    // Verify ingest proposal was called with correct parameters
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture(), eq(false));

    // Verify proposal contents
    MetadataChangeProposal proposal = proposalCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), TEST_POOL_URN);
    assertEquals(proposal.getAspectName(), AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME);

    // Verify pool info contents
    RemoteExecutorPoolInfo expectedPoolInfo = new RemoteExecutorPoolInfo();
    expectedPoolInfo.setCreatedAt(TEST_TIMESTAMP);
    expectedPoolInfo.setCreator(UrnUtils.getUrn(TEST_ACTOR_URN));
    assertEquals(
        proposal.getAspect(),
        MutationUtils.buildMetadataChangeProposalWithUrn(
                TEST_POOL_URN,
                AcrylConstants.REMOTE_EXECUTOR_POOL_INFO_ASPECT_NAME,
                expectedPoolInfo)
            .getAspect());
  }

  @Test
  public void testCreateDefaultPoolSuccess() throws Exception {
    // Mock permissions check
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    // Setup input with isDefault=true
    CreateRemoteExecutorPoolInput input = new CreateRemoteExecutorPoolInput();
    input.setExecutorPoolId(TEST_POOL_NAME);
    input.setIsDefault(true);
    when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    // Execute resolver
    final String result = _resolver.get(_dataFetchingEnvironment).join();

    // Verify result is the correct URN
    assertEquals(result, TEST_POOL_URN.toString());

    // Verify pool creation proposal, and default pool update
    verify(_entityClient, times(2))
        .ingestProposal(any(OperationContext.class), any(MetadataChangeProposal.class), eq(false));
  }

  @Test
  public void testCreatePoolNoPermissions() throws RemoteInvocationException {
    // Mock insufficient permissions
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class)))
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
  public void testCreatePoolEntityClientException() throws Exception {
    // Mock permissions check
    when(_mockContext.getOperationContext().authorize(anyString(), any(EntitySpec.class)))
        .thenReturn(
            new AuthorizationResult(
                mock(AuthorizationRequest.class), AuthorizationResult.Type.ALLOW, "message"));

    // Mock entity client exception
    doThrow(new RuntimeException("Test exception"))
        .when(_entityClient)
        .ingestProposal(
            any(OperationContext.class), any(MetadataChangeProposal.class), anyBoolean());

    // Verify exception is thrown
    assertThrows(RuntimeException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}
