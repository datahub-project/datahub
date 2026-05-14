package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.CreateRoleInput;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class CreateRoleResolverTest {

  private static final String ROLE_NAME = "Data Steward";
  private static final String ROLE_DESCRIPTION = "Owns data quality for a domain";

  private EntityClient _entityClient;
  private CreateRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _resolver = new CreateRoleResolver(_entityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> _resolver.get(_dataFetchingEnvironment));
  }

  @Test
  public void testCreateRoleEmitsEditableMcp() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final CreateRoleInput input = new CreateRoleInput(ROLE_NAME, ROLE_DESCRIPTION);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    when(_entityClient.ingestProposal(any(), any(MetadataChangeProposal.class), eq(false)))
        .thenReturn("urn:li:dataHubRole:generated");

    final String resultUrn = _resolver.get(_dataFetchingEnvironment).get();

    assertEquals(resultUrn, "urn:li:dataHubRole:generated");

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    final MetadataChangeProposal proposal = mcpCaptor.getValue();
    assertEquals(proposal.getEntityType(), DATAHUB_ROLE_ENTITY_NAME);
    assertEquals(proposal.getAspectName(), DATAHUB_ROLE_INFO_ASPECT_NAME);

    // Custom roles must be created with editable=true so they can later be edited/deleted via
    // UpdateRoleResolver/DeleteRoleResolver, which both reject non-editable roles.
    final DataHubRoleInfo info =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            DataHubRoleInfo.class);
    assertTrue(info.isEditable());
    assertEquals(info.getName(), ROLE_NAME);
    assertEquals(info.getDescription(), ROLE_DESCRIPTION);
  }

  @Test
  public void testCreateRoleNullDescriptionDefaultsToEmpty() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final CreateRoleInput input = new CreateRoleInput(ROLE_NAME, null);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    when(_entityClient.ingestProposal(any(), any(MetadataChangeProposal.class), eq(false)))
        .thenReturn("urn:li:dataHubRole:generated");

    _resolver.get(_dataFetchingEnvironment).get();

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    final DataHubRoleInfo info =
        GenericRecordUtils.deserializeAspect(
            mcpCaptor.getValue().getAspect().getValue(),
            mcpCaptor.getValue().getAspect().getContentType(),
            DataHubRoleInfo.class);
    assertEquals(info.getDescription(), "");
  }

  @Test
  public void testCreateRoleEntityClientFailureWraps() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final CreateRoleInput input = new CreateRoleInput(ROLE_NAME, ROLE_DESCRIPTION);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    when(_entityClient.ingestProposal(any(), any(MetadataChangeProposal.class), eq(false)))
        .thenThrow(new RuntimeException("upstream failure"));

    // The resolver wraps lower-level failures so callers see a consistent message and the
    // role name is preserved in the error context for operators triaging in logs.
    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
  }
}
