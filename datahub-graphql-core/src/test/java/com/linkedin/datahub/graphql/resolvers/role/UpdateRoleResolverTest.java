package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.AuthorizationException;
import com.linkedin.datahub.graphql.generated.UpdateRoleInput;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.policy.DataHubRoleInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class UpdateRoleResolverTest {

  private static final String CUSTOM_ROLE_URN = "urn:li:dataHubRole:custom-uuid";
  private static final String BUILT_IN_ROLE_URN = "urn:li:dataHubRole:Admin";
  private static final String UPDATED_NAME = "Data Steward (renamed)";
  private static final String UPDATED_DESCRIPTION = "Renamed steward role";

  private EntityClient _entityClient;
  private UpdateRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;

  @BeforeMethod
  public void setupTest() {
    _entityClient = mock(EntityClient.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _resolver = new UpdateRoleResolver(_entityClient);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(AuthorizationException.class, () -> _resolver.get(_dataFetchingEnvironment));
  }

  @Test
  public void testBuiltInRoleRejected() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final UpdateRoleInput input =
        new UpdateRoleInput(BUILT_IN_ROLE_URN, UPDATED_NAME, UPDATED_DESCRIPTION);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    stubBatchGet(Urn.createFromString(BUILT_IN_ROLE_URN), /* editable */ false);

    // Built-in roles must remain immutable end-to-end. This protects against accidental edits
    // even when the caller holds MANAGE_ROLES, which is the only server-side guarantee that
    // built-in role semantics (Admin/Editor/Reader/No Role) stay intact across upgrades.
    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  @Test
  public void testCustomRoleUpdatedSuccessfully() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final UpdateRoleInput input =
        new UpdateRoleInput(CUSTOM_ROLE_URN, UPDATED_NAME, UPDATED_DESCRIPTION);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    final Urn roleUrn = Urn.createFromString(CUSTOM_ROLE_URN);
    stubBatchGet(roleUrn, /* editable */ true);

    assertTrue(_resolver.get(_dataFetchingEnvironment).get());

    ArgumentCaptor<MetadataChangeProposal> mcpCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(1)).ingestProposal(any(), mcpCaptor.capture(), eq(false));

    final MetadataChangeProposal proposal = mcpCaptor.getValue();
    assertEquals(proposal.getEntityUrn(), roleUrn);
    assertEquals(proposal.getAspectName(), DATAHUB_ROLE_INFO_ASPECT_NAME);

    final DataHubRoleInfo info =
        GenericRecordUtils.deserializeAspect(
            proposal.getAspect().getValue(),
            proposal.getAspect().getContentType(),
            DataHubRoleInfo.class);
    assertEquals(info.getName(), UPDATED_NAME);
    assertEquals(info.getDescription(), UPDATED_DESCRIPTION);
    assertTrue(info.isEditable());
  }

  @Test
  public void testMissingRoleFails() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final UpdateRoleInput input =
        new UpdateRoleInput(CUSTOM_ROLE_URN, UPDATED_NAME, UPDATED_DESCRIPTION);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    when(_entityClient.batchGetV2(any(), eq(DATAHUB_ROLE_ENTITY_NAME), any(), any()))
        .thenReturn(Collections.emptyMap());

    assertThrows(CompletionException.class, () -> _resolver.get(_dataFetchingEnvironment).join());
    verify(_entityClient, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), anyBoolean());
  }

  private void stubBatchGet(Urn roleUrn, boolean editable) throws Exception {
    final DataHubRoleInfo info = new DataHubRoleInfo();
    info.setName("Existing Name");
    info.setDescription("Existing Description");
    info.setEditable(editable);

    final EntityResponse response =
        new EntityResponse()
            .setUrn(roleUrn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        DATAHUB_ROLE_INFO_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(info.data())))));

    when(_entityClient.batchGetV2(any(), eq(DATAHUB_ROLE_ENTITY_NAME), any(), any()))
        .thenReturn(ImmutableMap.of(roleUrn, response));
  }
}
