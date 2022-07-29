package com.datahub.authentication.proposal;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.Authorizer;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.Owner;
import com.linkedin.common.OwnerArray;
import com.linkedin.common.Ownership;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpGroupUrn;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.GlossaryNodeUrn;
import com.linkedin.common.urn.GlossaryTermUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Entity;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.glossary.GlossaryNodeInfo;
import com.linkedin.glossary.GlossaryTermInfo;
import com.linkedin.identity.GroupMembership;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.snapshot.ActionRequestSnapshot;
import com.linkedin.metadata.snapshot.Snapshot;
import java.util.Collections;
import java.util.Optional;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class ProposalServiceTest {
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private static final String GLOSSARY_NODE_NAME = "GLOSSARY_NODE";
  private static final String GLOSSARY_TERM_NAME = "GLOSSARY_TERM";
  private static final Urn ACTOR_URN = new CorpuserUrn("mock@email.com");
  private static final Urn GROUP_URN = new CorpGroupUrn("group");
  private static final String GLOSSARY_NODE_URN_STRING = "urn:li:glossaryNode:12372c2ec7754c308993202dc44f548b";
  private static final String GLOSSARY_TERM_URN_STRING = "urn:li:glossaryTerm:12372c2ec7754c308993202dc44f548b";
  private static final String DESCRIPTION = "description";
  private static final String ACTION_REQUEST_STATUS_COMPLETE = "COMPLETE";
  private static final String ACTION_REQUEST_RESULT_ACCEPTED = "ACCEPTED";

  private static Authorizer _authorizer;
  private static Urn _glossaryNodeUrn;
  private static Urn _glossaryTermUrn;

  private EntityService _entityService;
  private EntityClient _entityClient;
  private ProposalService _proposalService;

  @BeforeMethod
  public void setupTest() throws Exception {
    _authorizer = mock(Authorizer.class);
    AuthorizationResult result = mock(AuthorizationResult.class);
    when(result.getType()).thenReturn(AuthorizationResult.Type.ALLOW);
    when(_authorizer.authorize(any())).thenReturn(result);

    _glossaryNodeUrn = GlossaryNodeUrn.createFromString(GLOSSARY_NODE_URN_STRING);
    _glossaryTermUrn = GlossaryTermUrn.createFromString(GLOSSARY_TERM_URN_STRING);

    _entityService = mock(EntityService.class);
    _entityClient = mock(EntityClient.class);

    _proposalService = new ProposalService(_entityService, _entityClient);
  }

  @Test
  public void proposeCreateGlossaryNodeNullArguments() {
    assertThrows(
        () -> _proposalService.proposeCreateGlossaryNode(null, GLOSSARY_NODE_NAME, Optional.empty(), _authorizer));
    assertThrows(() -> _proposalService.proposeCreateGlossaryNode(ACTOR_URN, null, Optional.empty(), _authorizer));
    assertThrows(() -> _proposalService.proposeCreateGlossaryNode(ACTOR_URN, GLOSSARY_NODE_NAME, null, _authorizer));
  }

  @Test
  public void proposeCreateGlossaryNodePasses() {
    _proposalService.proposeCreateGlossaryNode(ACTOR_URN, GLOSSARY_NODE_NAME, Optional.empty(), _authorizer);
    verify(_entityService).ingestEntity(any(), any());
  }

  @Test
  public void proposeCreateGlossaryTermNullArguments() {
    assertThrows(
        () -> _proposalService.proposeCreateGlossaryTerm(null, GLOSSARY_TERM_NAME, Optional.empty(), _authorizer));
    assertThrows(() -> _proposalService.proposeCreateGlossaryTerm(ACTOR_URN, null, Optional.empty(), _authorizer));
    assertThrows(() -> _proposalService.proposeCreateGlossaryTerm(ACTOR_URN, GLOSSARY_TERM_NAME, null, _authorizer));
  }

  @Test
  public void proposeCreateGlossaryTermPasses() {
    _proposalService.proposeCreateGlossaryTerm(ACTOR_URN, GLOSSARY_TERM_NAME, Optional.empty(), _authorizer);
    verify(_entityService).ingestEntity(any(), any());
  }

  @Test
  public void proposeUpdateResourceDescriptionNullArguments() {
    assertThrows(
        () -> _proposalService.proposeUpdateResourceDescription(null, _glossaryNodeUrn, DESCRIPTION, _authorizer));
    assertThrows(() -> _proposalService.proposeUpdateResourceDescription(ACTOR_URN, null, DESCRIPTION, _authorizer));
    assertThrows(
        () -> _proposalService.proposeUpdateResourceDescription(ACTOR_URN, _glossaryNodeUrn, null, _authorizer));
  }

  @Test
  public void proposeUpdateResourceDescriptionPasses() {
    when(_entityService.exists(eq(_glossaryNodeUrn))).thenReturn(true);

    _proposalService.proposeUpdateResourceDescription(ACTOR_URN, _glossaryNodeUrn, DESCRIPTION, _authorizer);
    verify(_entityService).ingestEntity(any(), any());
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerNullArguments() {
    assertThrows(
        () -> _proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(null, Optional.of(_glossaryNodeUrn)));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerEmptyParentNode() {
    assertFalse(_proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(ACTOR_URN, Optional.empty()));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerActorIsOwnerPasses() {
    Ownership ownership = new Ownership().setOwners(new OwnerArray(ImmutableList.of(new Owner().setOwner(ACTOR_URN))));
    when(_entityService.getLatestAspect(eq(_glossaryNodeUrn), eq(OWNERSHIP_ASPECT_NAME))).thenReturn(ownership);

    assertTrue(_proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(ACTOR_URN, Optional.of(_glossaryNodeUrn)));
  }

  @Test
  public void isAuthorizedToResolveGlossaryEntityAsOwnerActorIsInOwnerGroupPasses() {
    Ownership ownership = new Ownership().setOwners(new OwnerArray(ImmutableList.of(new Owner().setOwner(GROUP_URN))));
    GroupMembership groupMembership =
        new GroupMembership().setGroups(new UrnArray(Collections.singletonList(GROUP_URN)));

    when(_entityService.getLatestAspect(eq(_glossaryNodeUrn), eq(OWNERSHIP_ASPECT_NAME))).thenReturn(ownership);
    when(_entityService.getLatestAspect(eq(ACTOR_URN), eq(GROUP_MEMBERSHIP_ASPECT_NAME))).thenReturn(groupMembership);

    assertTrue(_proposalService.isAuthorizedToResolveGlossaryEntityAsOwner(ACTOR_URN, Optional.of(_glossaryNodeUrn)));
  }

  @Test
  public void acceptCreateGlossaryNodeProposalNullArguments() {
    assertThrows(() -> _proposalService.acceptCreateGlossaryNodeProposal(null, new ActionRequestSnapshot(), true,
        SYSTEM_AUTHENTICATION));
    assertThrows(() -> _proposalService.acceptCreateGlossaryNodeProposal(ACTOR_URN, null, true, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void acceptCreateGlossaryNodeProposalPasses() throws Exception {
    when(_entityClient.exists(any(), any())).thenReturn(false);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createCreateGlossaryNodeProposalActionRequest(ACTOR_URN, Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST, GLOSSARY_NODE_NAME, Optional.empty());
    _proposalService.acceptCreateGlossaryNodeProposal(ACTOR_URN, actionRequestSnapshot, true, SYSTEM_AUTHENTICATION);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptCreateGlossaryTermProposalNullArguments() {
    assertThrows(() -> _proposalService.acceptCreateGlossaryTermProposal(null, new ActionRequestSnapshot(), true,
        SYSTEM_AUTHENTICATION));
    assertThrows(() -> _proposalService.acceptCreateGlossaryTermProposal(ACTOR_URN, null, true, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void acceptCreateGlossaryTermProposalPasses() throws Exception {
    when(_entityClient.exists(any(), any())).thenReturn(false);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createCreateGlossaryTermProposalActionRequest(ACTOR_URN, Collections.singletonList(ACTOR_URN),
            Collections.EMPTY_LIST, GLOSSARY_TERM_NAME, Optional.empty());
    _proposalService.acceptCreateGlossaryTermProposal(ACTOR_URN, actionRequestSnapshot, true, SYSTEM_AUTHENTICATION);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalNullArguments() {
    assertThrows(() -> _proposalService.acceptUpdateResourceDescriptionProposal(null, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _proposalService.acceptUpdateResourceDescriptionProposal(new ActionRequestSnapshot(), null));
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalForGlossaryNodePasses() throws Exception {
    GlossaryNodeInfo glossaryNodeInfo = _proposalService.mapGlossaryNodeInfo(GLOSSARY_NODE_NAME, Optional.empty());
    when(_entityClient.exists(any(), any())).thenReturn(false);
    when(_entityService.getLatestAspect(eq(_glossaryNodeUrn), eq(GLOSSARY_NODE_INFO_ASPECT_NAME))).thenReturn(
        glossaryNodeInfo);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(ACTOR_URN, _glossaryNodeUrn,
            Collections.singletonList(ACTOR_URN), Collections.EMPTY_LIST, DESCRIPTION);
    _proposalService.acceptUpdateResourceDescriptionProposal(actionRequestSnapshot, SYSTEM_AUTHENTICATION);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void acceptUpdateResourceDescriptionProposalForGlossaryTermPasses() throws Exception {
    GlossaryTermInfo glossaryTermInfo = _proposalService.mapGlossaryTermInfo(GLOSSARY_TERM_NAME, Optional.empty());
    when(_entityClient.exists(any(), any())).thenReturn(false);
    when(_entityService.getLatestAspect(eq(_glossaryTermUrn), eq(GLOSSARY_TERM_INFO_ASPECT_NAME))).thenReturn(
        glossaryTermInfo);

    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(ACTOR_URN, _glossaryTermUrn,
            Collections.singletonList(ACTOR_URN), Collections.EMPTY_LIST, DESCRIPTION);
    _proposalService.acceptUpdateResourceDescriptionProposal(actionRequestSnapshot, SYSTEM_AUTHENTICATION);

    verify(_entityClient).ingestProposal(any(), any());
  }

  @Test
  public void completeProposalNullArguments() {
    assertThrows(
        () -> _proposalService.completeProposal(null, ACTION_REQUEST_STATUS_COMPLETE, ACTION_REQUEST_RESULT_ACCEPTED,
            new Entity()));
    assertThrows(
        () -> _proposalService.completeProposal(ACTOR_URN, null, ACTION_REQUEST_RESULT_ACCEPTED, new Entity()));
    assertThrows(
        () -> _proposalService.completeProposal(ACTOR_URN, ACTION_REQUEST_STATUS_COMPLETE, null, new Entity()));
    assertThrows(() -> _proposalService.completeProposal(ACTOR_URN, ACTION_REQUEST_STATUS_COMPLETE,
        ACTION_REQUEST_RESULT_ACCEPTED, null));
  }

  @Test
  public void completeProposal() {
    ActionRequestSnapshot actionRequestSnapshot =
        _proposalService.createUpdateDescriptionProposalActionRequest(ACTOR_URN, _glossaryNodeUrn,
            Collections.singletonList(ACTOR_URN), Collections.EMPTY_LIST, DESCRIPTION);
    Entity entity = new Entity().setValue(Snapshot.create(actionRequestSnapshot));
    _proposalService.completeProposal(ACTOR_URN, ACTION_REQUEST_STATUS_COMPLETE, ACTION_REQUEST_RESULT_ACCEPTED,
        entity);

    verify(_entityService).ingestEntity(any(), any());
  }
}
