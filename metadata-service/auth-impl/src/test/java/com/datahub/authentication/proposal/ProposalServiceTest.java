package com.datahub.authentication.proposal;

import static com.datahub.authentication.proposal.ProposalService.*;
import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.AuthorizedActors;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.actionrequest.ActionRequestInfo;
import com.linkedin.actionrequest.ActionRequestParams;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.DatasetUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.EditableDatasetProperties;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.aspect.ActionRequestAspect;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.resource.SubResourceType;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.mxe.GenericAspect;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.schema.EditableSchemaFieldInfo;
import com.linkedin.schema.EditableSchemaFieldInfoArray;
import com.linkedin.schema.EditableSchemaMetadata;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.Objects;
import java.util.Optional;
import org.mockito.ArgumentMatcher;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ProposalServiceTest {
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private static final String GLOSSARY_NODE_NAME = "GLOSSARY_NODE";
  private static final String GLOSSARY_TERM_NAME = "GLOSSARY_TERM";
  private static final Urn ACTOR_URN = new CorpuserUrn("mock@email.com");
  private static final Urn GROUP_URN = new CorpGroupUrn("group");
  private static final String GLOSSARY_NODE_URN_STRING =
      "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b";
  private static final String GLOSSARY_TERM_URN_STRING =
      "urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b";
  private static final String DATASET_URN_STRING =
      "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)";
  private static final String DESCRIPTION = "description";
  private static final String ACTION_REQUEST_STATUS_COMPLETE = "COMPLETE";
  private static final String ACTION_REQUEST_RESULT_ACCEPTED = "ACCEPTED";

  private static Authorizer _authorizer;
  private static Urn _glossaryNodeUrn;
  private static Urn _glossaryTermUrn;

  private static Urn _datasetUrn;

  private static String _fieldPath;

  private EntityService<?> _entityService;
  private EntityClient _entityClient;
  private GraphClient _graphClient;
  private OperationContext opContext;

  private ProposalService _proposalService;

  @BeforeMethod
  public void setupTest() throws Exception {
    _authorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(_authorizer.authorize(any())).thenReturn(result);

    _glossaryNodeUrn = GlossaryNodeUrn.createFromString(GLOSSARY_NODE_URN_STRING);
    _glossaryTermUrn = GlossaryTermUrn.createFromString(GLOSSARY_TERM_URN_STRING);
    _datasetUrn = DatasetUrn.createFromString(DATASET_URN_STRING);
    _fieldPath = "someField";

    _entityService = mock(EntityService.class);
    _entityClient = mock(EntityClient.class);
    _graphClient = mock(GraphClient.class);

    opContext =
        TestOperationContexts.userContextNoSearchAuthorization(_authorizer, SYSTEM_AUTHENTICATION);

    _proposalService = new ProposalService(_entityService, _entityClient, _graphClient);
  }

  @Test
  public void proposeCreateGlossaryNodeNullArguments() {
    assertThrows(
        () ->
            _proposalService.proposeCreateGlossaryNode(
                opContext, null, GLOSSARY_NODE_NAME, Optional.empty(), "test"));
    assertThrows(
        () ->
            _proposalService.proposeCreateGlossaryNode(
                opContext, ACTOR_URN, null, Optional.empty(), "test"));
    assertThrows(
        () ->
            _proposalService.proposeCreateGlossaryNode(
                opContext, ACTOR_URN, GLOSSARY_NODE_NAME, null, "test"));
  }

  @Test
  public void proposeCreateGlossaryNodePasses() {
    _proposalService.proposeCreateGlossaryNode(
        opContext, ACTOR_URN, GLOSSARY_NODE_NAME, Optional.empty(), "test");
    verify(_entityService).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void proposeCreateGlossaryTermNullArguments() {
    assertThrows(
        () ->
            _proposalService.proposeCreateGlossaryTerm(
                opContext, null, GLOSSARY_TERM_NAME, Optional.empty(), "test"));
    assertThrows(
        () ->
            _proposalService.proposeCreateGlossaryTerm(
                opContext, ACTOR_URN, null, Optional.empty(), "test"));
    assertThrows(
        () ->
            _proposalService.proposeCreateGlossaryTerm(
                opContext, ACTOR_URN, GLOSSARY_TERM_NAME, null, "test"));
  }

  @Test
  public void proposeCreateGlossaryTermPasses() {
    _proposalService.proposeCreateGlossaryTerm(
        opContext, ACTOR_URN, GLOSSARY_TERM_NAME, Optional.empty(), "test");
    verify(_entityService).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void proposeUpdateResourceDescriptionNullArguments() {
    assertThrows(
        () ->
            _proposalService.proposeUpdateResourceDescription(
                opContext, null, _glossaryNodeUrn, null, null, DESCRIPTION));
    assertThrows(
        () ->
            _proposalService.proposeUpdateResourceDescription(
                opContext, ACTOR_URN, null, null, null, DESCRIPTION));
    assertThrows(
        () ->
            _proposalService.proposeUpdateResourceDescription(
                opContext, ACTOR_URN, _glossaryNodeUrn, null, null, null));
  }

  private static class DescriptionUpdateActionRequestMatcher extends ActionRequestSnapshotMatcher {

    private final String description;

    private DescriptionUpdateActionRequestMatcher(
        String resourceUrn, String subResourceType, String subResource, String description) {
      super("UPDATE_DESCRIPTION", resourceUrn, subResourceType, subResource);
      this.description = description;
    }

    @Override
    boolean matchesActionRequestParams(ActionRequestParams params) {
      return params.hasUpdateDescriptionProposal()
          && Objects.equals(description, params.getUpdateDescriptionProposal().getDescription());
    }
  }

  @Test
  public void proposeUpdateResourceDescriptionPasses() {
    when(_entityService.exists(any(OperationContext.class), eq(_glossaryNodeUrn), anyBoolean()))
        .thenReturn(true);

    _proposalService.proposeUpdateResourceDescription(
        opContext, ACTOR_URN, _glossaryNodeUrn, null, null, DESCRIPTION);

    DescriptionUpdateActionRequestMatcher snapshotMatcher =
        new DescriptionUpdateActionRequestMatcher(
            _glossaryNodeUrn.toString(), null, null, DESCRIPTION);

    verify(_entityService)
        .ingestEntity(any(OperationContext.class), argThat(snapshotMatcher), any());
  }

  @Test
  public void proposeUpdateColumnDescriptionPasses() throws URISyntaxException {
    when(_entityService.exists(any(OperationContext.class), eq(_datasetUrn), anyBoolean()))
        .thenReturn(true);

    EntitySpec spec = new EntitySpec(_datasetUrn.getEntityType(), _datasetUrn.toString());
    AuthorizedActors actors =
        new Authorizer() {}.authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType(), Optional.of(spec));
    when(_authorizer.authorizedActors(any(), any())).thenReturn(actors);

    _proposalService.proposeUpdateResourceDescription(
        opContext,
        ACTOR_URN,
        _datasetUrn,
        SubResourceType.DATASET_FIELD.toString(),
        _fieldPath,
        DESCRIPTION);

    DescriptionUpdateActionRequestMatcher snapshotMatcher =
        new DescriptionUpdateActionRequestMatcher(
            _datasetUrn.toString(),
            SubResourceType.DATASET_FIELD.toString(),
            _fieldPath,
            DESCRIPTION);

    verify(_entityService)
        .ingestEntity(any(OperationContext.class), argThat(snapshotMatcher), any());
  }

  @Test
  public void proposeUpdateDescriptionErrPartialSpec() throws URISyntaxException {
    // error should occur when only one of subResourceType or subResource are specified
    Urn datasetUrn =
        Urn.createFromString(
            "urn:li:dataset:(urn:li:dataPlatform:bigquery,my-project.my-dataset.user-table,PROD)");

    String fieldPath = "someField";

    when(_entityService.exists(any(OperationContext.class), eq(datasetUrn), anyBoolean()))
        .thenReturn(true);

    EntitySpec spec = new EntitySpec(datasetUrn.getEntityType(), datasetUrn.toString());
    AuthorizedActors actors =
        new Authorizer() {}.authorizedActors(
            PoliciesConfig.MANAGE_ENTITY_DOCS_PROPOSALS_PRIVILEGE.getType(), Optional.of(spec));
    when(_authorizer.authorizedActors(any(), any())).thenReturn(actors);

    DescriptionUpdateActionRequestMatcher snapshotMatcher =
        new DescriptionUpdateActionRequestMatcher(
            datasetUrn.toString(),
            SubResourceType.DATASET_FIELD.toString(),
            fieldPath,
            DESCRIPTION);

    assertThrows(
        () ->
            _proposalService.proposeUpdateResourceDescription(
                opContext,
                ACTOR_URN,
                datasetUrn,
                SubResourceType.DATASET_FIELD.toString(),
                null,
                DESCRIPTION));

    assertThrows(
        () ->
            _proposalService.proposeUpdateResourceDescription(
                opContext, ACTOR_URN, datasetUrn, null, fieldPath, DESCRIPTION));

    verify(_entityService, never()).ingestEntity(any(OperationContext.class), any(), any());
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerNullArguments() {
    assertThrows(
        () ->
            _proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(
                opContext, null, Optional.of(_glossaryNodeUrn)));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerEmptyParentNode() {
    assertFalse(
        _proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(
            opContext, ACTOR_URN, Optional.empty()));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerActorIsOwnerPasses() {
    Ownership ownership =
        new Ownership()
            .setOwners(new OwnerArray(ImmutableList.of(new Owner().setOwner(ACTOR_URN))));
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_glossaryNodeUrn), eq(OWNERSHIP_ASPECT_NAME)))
        .thenReturn(ownership);

    assertTrue(
        _proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(
            opContext, ACTOR_URN, Optional.of(_glossaryNodeUrn)));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerActorIsInOwnerGroupPasses() {
    Ownership ownership =
        new Ownership()
            .setOwners(new OwnerArray(ImmutableList.of(new Owner().setOwner(GROUP_URN))));
    GroupMembership groupMembership =
        new GroupMembership().setGroups(new UrnArray(Collections.singletonList(GROUP_URN)));

    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_glossaryNodeUrn), eq(OWNERSHIP_ASPECT_NAME)))
        .thenReturn(ownership);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(ACTOR_URN), eq(GROUP_MEMBERSHIP_ASPECT_NAME)))
        .thenReturn(groupMembership);

    assertTrue(
        _proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(
            opContext, ACTOR_URN, Optional.of(_glossaryNodeUrn)));
  }

  @Test
  public void acceptCreateGlossaryNodeProposalNullArguments() {
    assertThrows(
        () ->
            _proposalService.acceptCreateGlossaryNodeProposal(
                opContext, null, new ActionRequestSnapshot(), true));
    assertThrows(
        () -> _proposalService.acceptCreateGlossaryNodeProposal(opContext, ACTOR_URN, null, true));
  }

  @Test
  public void acceptCreateGlossaryNodeProposalPasses() throws Exception {
    when(_entityClient.exists(any(), any())).thenReturn(false);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createCreateGlossaryNodeProposalActionRequest(
            ACTOR_URN,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            GLOSSARY_NODE_NAME,
            Optional.empty(),
            "test");
    _proposalService.acceptCreateGlossaryNodeProposal(
        opContext, ACTOR_URN, actionRequestSnapshot, true);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptCreateGlossaryTermProposalNullArguments() {
    assertThrows(
        () ->
            _proposalService.acceptCreateGlossaryTermProposal(
                opContext, null, new ActionRequestSnapshot(), true));
    assertThrows(
        () -> _proposalService.acceptCreateGlossaryTermProposal(opContext, ACTOR_URN, null, true));
  }

  @Test
  public void acceptCreateGlossaryTermProposalPasses() throws Exception {
    when(_entityClient.exists(any(), any())).thenReturn(false);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createCreateGlossaryTermProposalActionRequest(
            ACTOR_URN,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            GLOSSARY_TERM_NAME,
            Optional.empty(),
            "test");
    _proposalService.acceptCreateGlossaryTermProposal(
        opContext, ACTOR_URN, actionRequestSnapshot, true);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalNullArguments() {
    assertThrows(() -> _proposalService.acceptUpdateResourceDescriptionProposal(opContext, null));
    assertThrows(
        () ->
            _proposalService.acceptUpdateResourceDescriptionProposal(
                opContext, new ActionRequestSnapshot()));
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalForGlossaryNodePasses() throws Exception {
    GlossaryNodeInfo glossaryNodeInfo =
        _proposalService.mapGlossaryNodeInfo(
            GLOSSARY_NODE_NAME, Optional.empty(), Optional.of("test"));
    when(_entityClient.exists(any(), any())).thenReturn(false);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_glossaryNodeUrn), eq(GLOSSARY_NODE_INFO_ASPECT_NAME)))
        .thenReturn(glossaryNodeInfo);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _glossaryNodeUrn,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    _proposalService.acceptUpdateResourceDescriptionProposal(opContext, actionRequestSnapshot);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalForGlossaryTermPasses() throws Exception {
    GlossaryTermInfo glossaryTermInfo =
        _proposalService.mapGlossaryTermInfo(
            GLOSSARY_TERM_NAME, Optional.empty(), Optional.of("test"));
    when(_entityClient.exists(any(), any())).thenReturn(false);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_glossaryTermUrn), eq(GLOSSARY_TERM_INFO_ASPECT_NAME)))
        .thenReturn(glossaryTermInfo);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _glossaryTermUrn,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    _proposalService.acceptUpdateResourceDescriptionProposal(opContext, actionRequestSnapshot);

    verify(_entityClient).ingestProposal(any(), any());
  }

  private static class DatasetDescriptionMatcher extends MetadataChangeProposalMatcher {
    private final String description;

    private DatasetDescriptionMatcher(
        Urn urn, String entityType, String aspectName, ChangeType changeType, String description) {
      super(urn, entityType, aspectName, changeType);
      this.description = description;
    }

    @Override
    boolean matchesAspect(GenericAspect aspect) {
      EditableDatasetProperties editableDatasetProperties =
          GenericRecordUtils.deserializeAspect(
              aspect.getValue(), GenericRecordUtils.JSON, EditableDatasetProperties.class);

      return Objects.equals(description, editableDatasetProperties.getDescription());
    }
  }

  @Test
  public void acceptUpdateDatasetDescriptionProposalNewAspectPasses() throws Exception {
    when(_entityService.getLatestAspect(
            any(OperationContext.class),
            eq(_datasetUrn),
            eq(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)))
        .thenReturn(null);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _datasetUrn,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    _proposalService.acceptUpdateResourceDescriptionProposal(opContext, actionRequestSnapshot);

    DatasetDescriptionMatcher datasetDescriptionMatcher =
        new DatasetDescriptionMatcher(
            _datasetUrn,
            DATASET_ENTITY_NAME,
            EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), argThat(datasetDescriptionMatcher));
  }

  @Test
  public void acceptUpdateDatasetDescriptionProposalExistingAspectPasses() throws Exception {
    EditableDatasetProperties editableDatasetProperties = new EditableDatasetProperties();
    editableDatasetProperties.setDescription("OLD_" + DESCRIPTION);

    when(_entityService.getLatestAspect(
            any(OperationContext.class),
            eq(_datasetUrn),
            eq(EDITABLE_DATASET_PROPERTIES_ASPECT_NAME)))
        .thenReturn(editableDatasetProperties);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _datasetUrn,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    _proposalService.acceptUpdateResourceDescriptionProposal(opContext, actionRequestSnapshot);

    DatasetDescriptionMatcher datasetDescriptionMatcher =
        new DatasetDescriptionMatcher(
            _datasetUrn,
            DATASET_ENTITY_NAME,
            EDITABLE_DATASET_PROPERTIES_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), argThat(datasetDescriptionMatcher));
  }

  private static class ColumnDescriptionMatcher extends MetadataChangeProposalMatcher {
    private final String description;

    private ColumnDescriptionMatcher(
        Urn urn, String entityType, String aspectName, ChangeType changeType, String description) {
      super(urn, entityType, aspectName, changeType);
      this.description = description;
    }

    @Override
    boolean matchesAspect(GenericAspect aspect) {
      EditableSchemaMetadata editableSchemaMetadata =
          GenericRecordUtils.deserializeAspect(
              aspect.getValue(), GenericRecordUtils.JSON, EditableSchemaMetadata.class);

      EditableSchemaFieldInfo editableSchemaFieldInfo =
          DescriptionUtils.getFieldInfoFromSchema(editableSchemaMetadata, _fieldPath);

      return Objects.equals(description, editableSchemaFieldInfo.getDescription());
    }
  }

  @Test
  public void acceptUpdateColumnDescriptionProposalNewAspectPasses() throws Exception {
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_datasetUrn), eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(null);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _datasetUrn,
            SubResourceType.DATASET_FIELD.toString(),
            _fieldPath,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    _proposalService.acceptUpdateResourceDescriptionProposal(opContext, actionRequestSnapshot);

    ColumnDescriptionMatcher columnDescriptionMatcher =
        new ColumnDescriptionMatcher(
            _datasetUrn,
            DATASET_ENTITY_NAME,
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), argThat(columnDescriptionMatcher));
  }

  @Test
  public void acceptUpdateColumnDescriptionProposalExistingAspectPasses() throws Exception {
    EditableSchemaMetadata editableSchemaMetadata = new EditableSchemaMetadata();
    EditableSchemaFieldInfo editableSchemaFieldInfo = new EditableSchemaFieldInfo();
    editableSchemaMetadata.setEditableSchemaFieldInfo(
        new EditableSchemaFieldInfoArray(editableSchemaFieldInfo));
    editableSchemaFieldInfo.setDescription("OLD_" + DESCRIPTION);
    editableSchemaFieldInfo.setFieldPath(_fieldPath);

    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_datasetUrn), eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(editableSchemaMetadata);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _datasetUrn,
            SubResourceType.DATASET_FIELD.toString(),
            _fieldPath,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    _proposalService.acceptUpdateResourceDescriptionProposal(opContext, actionRequestSnapshot);

    ColumnDescriptionMatcher columnDescriptionMatcher =
        new ColumnDescriptionMatcher(
            _datasetUrn,
            DATASET_ENTITY_NAME,
            EDITABLE_SCHEMA_METADATA_ASPECT_NAME,
            ChangeType.UPSERT,
            DESCRIPTION);

    verify(_entityClient)
        .ingestProposal(any(OperationContext.class), argThat(columnDescriptionMatcher));
  }

  @Test
  public void acceptUpdateColumnDescriptionProposalNoSubResourceFails() throws Exception {
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_datasetUrn), eq(EDITABLE_SCHEMA_METADATA_ASPECT_NAME)))
        .thenReturn(null);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _datasetUrn,
            SubResourceType.DATASET_FIELD.toString(),
            _fieldPath,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);

    // set subresource to null
    actionRequestSnapshot.getAspects().get(1).getActionRequestInfo().removeSubResource();
    assertThrows(
        NullPointerException.class,
        () ->
            _proposalService.acceptUpdateResourceDescriptionProposal(
                opContext, actionRequestSnapshot));

    verify(_entityClient, never()).ingestProposal(any(), any());
  }

  @Test
  public void completeProposalNullArguments() {
    assertThrows(
        () ->
            _proposalService.completeProposal(
                opContext,
                null,
                ACTION_REQUEST_STATUS_COMPLETE,
                ACTION_REQUEST_RESULT_ACCEPTED,
                new Entity()));
    assertThrows(
        () ->
            _proposalService.completeProposal(
                opContext, ACTOR_URN, null, ACTION_REQUEST_RESULT_ACCEPTED, new Entity()));
    assertThrows(
        () ->
            _proposalService.completeProposal(
                opContext, ACTOR_URN, ACTION_REQUEST_STATUS_COMPLETE, null, new Entity()));
    assertThrows(
        () ->
            _proposalService.completeProposal(
                opContext,
                ACTOR_URN,
                ACTION_REQUEST_STATUS_COMPLETE,
                ACTION_REQUEST_RESULT_ACCEPTED,
                null));
  }

  @Test
  public void completeProposal() {
    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(
            ACTOR_URN,
            _glossaryNodeUrn,
            null,
            null,
            Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST,
            Collections.EMPTY_LIST,
            DESCRIPTION);
    Entity entity = new Entity().setValue(Snapshot.create(actionRequestSnapshot));
    _proposalService.completeProposal(
        opContext,
        ACTOR_URN,
        ACTION_REQUEST_STATUS_COMPLETE,
        ACTION_REQUEST_RESULT_ACCEPTED,
        entity);

    verify(_entityService).ingestEntity(any(OperationContext.class), any(), any());
  }

  private abstract static class ActionRequestSnapshotMatcher implements ArgumentMatcher<Entity> {

    private final String infoType;
    private final String resourceUrn;
    private final String subResourceType;
    private final String subResource;

    private ActionRequestSnapshotMatcher(
        String infoType, String resourceUrn, String subResourceType, String subResource) {
      this.infoType = infoType;
      this.resourceUrn = resourceUrn;
      this.subResourceType = subResourceType;
      this.subResource = subResource;
    }

    @Override
    public boolean matches(Entity entity) {
      Snapshot snapshot = entity.getValue();
      if (!snapshot.isActionRequestSnapshot()) {
        return false;
      }

      for (ActionRequestAspect aspect : snapshot.getActionRequestSnapshot().getAspects()) {
        if (aspect.isActionRequestInfo()) {
          ActionRequestInfo actual = aspect.getActionRequestInfo();
          return Objects.equals(infoType, actual.getType())
              && Objects.equals(resourceUrn, actual.getResource())
              && Objects.equals(subResourceType, actual.getSubResourceType())
              && Objects.equals(subResource, actual.getSubResource())
              && matchesActionRequestParams(actual.getParams());
        }
      }

      return false;
    }

    abstract boolean matchesActionRequestParams(ActionRequestParams params);
  }

  private abstract static class MetadataChangeProposalMatcher
      implements ArgumentMatcher<MetadataChangeProposal> {

    private final Urn urn;
    private final String entityType;
    private final String aspectName;
    private final ChangeType changeType;

    private MetadataChangeProposalMatcher(
        Urn urn, String entityType, String aspectName, ChangeType changeType) {
      this.urn = urn;
      this.entityType = entityType;
      this.aspectName = aspectName;
      this.changeType = changeType;
    }

    @Override
    public boolean matches(MetadataChangeProposal mcp) {
      return Objects.equals(urn, mcp.getEntityUrn())
          && Objects.equals(entityType, mcp.getEntityType())
          && Objects.equals(aspectName, mcp.getAspectName())
          && Objects.equals(changeType, mcp.getChangeType())
          && matchesAspect(mcp.getAspect());
    }

    abstract boolean matchesAspect(GenericAspect aspect);
  }
}
