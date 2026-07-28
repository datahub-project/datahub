package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.Origin;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class GroupServiceTest {
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";

  private static final String GROUP_NAME = "Group Name";
  private static final String GROUP_DESCRIPTION = "This is a group";
  private static final String GROUP_ID = "abcd";
  private static final String GROUP_URN_STRING = "urn:li:corpGroup:testNewGroup";
  private static final String NATIVE_GROUP_URN_STRING = "urn:li:corpGroup:testGroupNative";
  private static final String EXTERNAL_GROUP_URN_STRING = "urn:li:corpGroup:testGroupExternal";
  private static final String EMAIL = "mock@email.com";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:actor";
  private static final int RESTORE_INDICES_BATCH_SIZE = 100;
  private static final Urn USER_URN = new CorpuserUrn(EMAIL);
  private static final Urn OTHER_USER_URN = new CorpuserUrn("other@email.com");
  private static final List<Urn> USER_URN_LIST = new ArrayList<>(Collections.singleton(USER_URN));
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");

  private static Urn _groupUrn;
  private static CorpGroupKey _groupKey;
  private static Map<Urn, EntityResponse> _entityResponseMap;
  private static EntityRelationships _entityRelationships;

  private SystemEntityClient _entityClient;
  private EntityService<?> _entityService;
  private GraphClient _graphClient;
  private GroupService _groupService;

  private OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(SYSTEM_AUTHENTICATION);

  @BeforeMethod
  public void setupTest() throws Exception {
    _groupUrn = Urn.createFromString(GROUP_URN_STRING);
    _groupKey = new CorpGroupKey();
    _groupKey.setName(GROUP_ID);

    NativeGroupMembership nativeGroupMembership = new NativeGroupMembership();
    nativeGroupMembership.setNativeGroups(
        new UrnArray(Urn.createFromString(NATIVE_GROUP_URN_STRING)));
    GroupMembership groupMembership = new GroupMembership();
    groupMembership.setGroups(new UrnArray(Urn.createFromString(EXTERNAL_GROUP_URN_STRING)));
    _entityResponseMap =
        ImmutableMap.of(
            USER_URN,
            new EntityResponse()
                .setEntityName(CORP_USER_ENTITY_NAME)
                .setUrn(USER_URN)
                .setAspects(
                    new EnvelopedAspectMap(
                        ImmutableMap.of(
                            NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                            new EnvelopedAspect()
                                .setValue(new Aspect(nativeGroupMembership.data())),
                            GROUP_MEMBERSHIP_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(groupMembership.data()))))));

    _entityRelationships =
        new EntityRelationships()
            .setStart(0)
            .setCount(1)
            .setTotal(1)
            .setRelationships(
                new EntityRelationshipArray(
                    ImmutableList.of(
                        new EntityRelationship()
                            .setEntity(USER_URN)
                            .setType(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME))));

    _entityClient = mock(SystemEntityClient.class);
    _entityService = mock(EntityService.class);
    _graphClient = mock(GraphClient.class);

    _groupService = new GroupService(_entityClient, _entityService, _graphClient);
  }

  @Test
  public void testConstructor() {
    assertThrows(() -> new GroupService(null, _entityService, _graphClient));
    assertThrows(() -> new GroupService(_entityClient, null, _graphClient));
    assertThrows(() -> new GroupService(_entityClient, _entityService, null));

    // Succeeds!
    new GroupService(_entityClient, _entityService, _graphClient);
  }

  @Test
  public void testGroupExistsNullArguments() {
    assertThrows(() -> _groupService.groupExists(mock(OperationContext.class), null));
  }

  @Test
  public void testGroupExistsPasses() {
    when(_entityService.exists(any(OperationContext.class), eq(_groupUrn), eq(true)))
        .thenReturn(true);
    assertTrue(_groupService.groupExists(opContext, _groupUrn));
  }

  @Test
  public void testGetGroupOriginNullArguments() {
    assertThrows(() -> _groupService.getGroupOrigin(mock(OperationContext.class), null));
  }

  @Test
  public void testGetGroupOriginPasses() {
    Origin groupOrigin = mock(Origin.class);
    when(_entityService.getLatestAspect(
            any(OperationContext.class), eq(_groupUrn), eq(ORIGIN_ASPECT_NAME)))
        .thenReturn(groupOrigin);

    assertEquals(groupOrigin, _groupService.getGroupOrigin(opContext, _groupUrn));
  }

  @Test
  public void testAddUserToNativeGroupNullArguments() {
    assertThrows(
        () -> _groupService.addUserToNativeGroup(mock(OperationContext.class), null, _groupUrn));
    assertThrows(
        () -> _groupService.addUserToNativeGroup(mock(OperationContext.class), USER_URN, null));
  }

  @Test
  public void testAddUserToNativeGroupPasses() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.addUserToNativeGroup(opContext, USER_URN, _groupUrn);
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient).ingestProposal(any(OperationContext.class), proposalCaptor.capture());
    assertEquals(
        UI_SOURCE, proposalCaptor.getValue().getSystemMetadata().getProperties().get(APP_SOURCE));
    verify(_entityClient).batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
  }

  @Test
  public void testAddUserToNativeGroupWhenAspectMissing() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of());

    _groupService.addUserToNativeGroup(opContext, USER_URN, _groupUrn);

    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
    verify(_entityClient).batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
    verify(_entityClient, never()).batchGetV2(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
  }

  @Test
  public void testAddUsersToNativeGroupIssuesOneReadPerBatch() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN, OTHER_USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN, OTHER_USER_URN), _groupUrn);

    verify(_entityService, times(1)).exists(any(OperationContext.class), anyCollection(), eq(true));
    verify(_entityClient, times(1))
        .batchGetV2NoCache(any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any());
    verify(_entityClient, times(2)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testAddUsersToNativeGroupRejectsAbsentUserBeforeAnyWrite() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));

    assertThrows(
        () ->
            _groupService.addUsersToNativeGroup(
                opContext, List.of(USER_URN, OTHER_USER_URN), _groupUrn));

    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testAddUsersToNativeGroupRepairsMissingEdge() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseWithNativeGroups(USER_URN, _groupUrn));
    when(_graphClient.getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(relationshipsPage(0));

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);

    // Already-members still get an ingestProposal: content is unchanged so the MCL is
    // suppressed, but the write still refreshes actor/APP_SOURCE provenance on the row.
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
    verify(_entityService)
        .restoreIndices(
            any(OperationContext.class),
            eq(Set.of(USER_URN)),
            eq(Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)),
            eq(RESTORE_INDICES_BATCH_SIZE),
            eq(false));
  }

  @Test
  public void testAddUsersToNativeGroupSkipsRepairWhenEdgeExists() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseWithNativeGroups(USER_URN, _groupUrn));
    when(_graphClient.getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(relationshipsPage(1, USER_URN));

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);

    verify(_entityService, never())
        .restoreIndices(any(OperationContext.class), anySet(), any(), any(), anyBoolean());
  }

  @Test
  public void testAddUsersToNativeGroupSkipsRepairForNewMember() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);

    verify(_graphClient, never())
        .getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any());
    verify(_entityService, never())
        .restoreIndices(any(OperationContext.class), anySet(), any(), any(), anyBoolean());
  }

  @Test
  public void testAddUsersToNativeGroupTreatsGraphFailureAsDivergent() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseWithNativeGroups(USER_URN, _groupUrn));
    when(_graphClient.getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any()))
        .thenThrow(new RuntimeException("graph unavailable"));

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);

    verify(_entityService)
        .restoreIndices(
            any(OperationContext.class),
            eq(Set.of(USER_URN)),
            eq(Set.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)),
            eq(RESTORE_INDICES_BATCH_SIZE),
            eq(false));
  }

  @Test
  public void testAddUsersToNativeGroupDoesNotRethrowWhenRestoreIndicesFails() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseWithNativeGroups(USER_URN, _groupUrn));
    when(_graphClient.getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any()))
        .thenReturn(relationshipsPage(0));
    doThrow(new RuntimeException("index unavailable"))
        .when(_entityService)
        .restoreIndices(any(OperationContext.class), anySet(), any(), any(), anyBoolean());

    // Must not throw: the nativeGroupMembership aspect above was already ingested successfully,
    // so a restoreIndices failure here is a stale-index problem, not a failed add. Rethrowing
    // would misreport a successful, already-committed membership write as an error to the
    // caller. Pins the deliberate non-rethrow in repairMissingNativeGroupEdges's catch block —
    // if that catch is ever removed, this test starts throwing.
    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);
  }

  @Test
  public void testGetExistingNativeGroupMembershipUsesCachedRead() throws Exception {
    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    NativeGroupMembership membership =
        _groupService.getExistingNativeGroupMembership(opContext, USER_URN);

    assertEquals(1, membership.getNativeGroups().size());
    assertEquals(
        Urn.createFromString(NATIVE_GROUP_URN_STRING), membership.getNativeGroups().get(0));
    verify(_entityClient).batchGetV2(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
    verify(_entityClient, never()).batchGetV2NoCache(any(), any(), any(), any());
  }

  @Test
  public void testGetExistingGroupMembershipUsesCachedRead() throws Exception {
    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    GroupMembership membership = _groupService.getExistingGroupMembership(opContext, USER_URN);

    assertEquals(1, membership.getGroups().size());
    assertEquals(Urn.createFromString(EXTERNAL_GROUP_URN_STRING), membership.getGroups().get(0));
    verify(_entityClient).batchGetV2(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
    verify(_entityClient, never()).batchGetV2NoCache(any(), any(), any(), any());
  }

  @Test
  public void testRemoveExistingNativeGroupMembersNoOpWhenAspectMissing() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of());

    _groupService.removeExistingNativeGroupMembers(
        opContext, Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST);

    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testRemoveExistingGroupMembersNoOpWhenAspectMissing() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of());

    _groupService.removeExistingGroupMembers(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST);

    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testCreateNativeGroupNullArguments() {
    assertThrows(
        () ->
            _groupService.createNativeGroup(
                mock(OperationContext.class), null, GROUP_NAME, GROUP_DESCRIPTION));
    assertThrows(
        () ->
            _groupService.createNativeGroup(
                mock(OperationContext.class), _groupKey, null, GROUP_DESCRIPTION));
    assertThrows(
        () ->
            _groupService.createNativeGroup(
                mock(OperationContext.class), _groupKey, GROUP_NAME, null));
  }

  @Test
  public void testCreateNativeGroupPasses() throws Exception {
    _groupService.createNativeGroup(opContext, _groupKey, GROUP_NAME, GROUP_DESCRIPTION);
    verify(_entityClient, times(2)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testRemoveExistingNativeGroupMembersNullArguments() {
    assertThrows(
        () ->
            _groupService.removeExistingNativeGroupMembers(
                mock(OperationContext.class), null, USER_URN_LIST));
    assertThrows(
        () ->
            _groupService.removeExistingNativeGroupMembers(
                mock(OperationContext.class), _groupUrn, null));
  }

  @Test
  public void testRemoveExistingNativeGroupMembersGroupNotInNativeGroupMembership()
      throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingNativeGroupMembers(
        mock(OperationContext.class),
        Urn.createFromString(EXTERNAL_GROUP_URN_STRING),
        USER_URN_LIST);
    verify(_entityClient, never()).ingestProposal(any(), any(), anyBoolean());
  }

  @Test
  public void testRemoveExistingNativeGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingNativeGroupMembers(
        opContext, Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testMigrateGroupMembershipToNativeGroupMembershipNullArguments() {
    assertThrows(
        () ->
            _groupService.migrateGroupMembershipToNativeGroupMembership(
                mock(OperationContext.class), null, USER_URN.toString()));
  }

  @Test
  public void testMigrateGroupMembershipToNativeGroupMembershipPasses() throws Exception {
    when(_graphClient.getRelatedEntities(
            eq(EXTERNAL_GROUP_URN_STRING),
            eq(ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            any()))
        .thenReturn(_entityRelationships);
    when(_entityClient.batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));

    _groupService.migrateGroupMembershipToNativeGroupMembership(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString());
    verify(_entityClient, times(3)).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testMigrateGroupMembershipToNativeGroupMembershipDropsStaleMember() throws Exception {
    // The graph names two members, but OTHER_USER_URN no longer exists in SQL (e.g. a deleted
    // corpuser whose IsMemberOfGroup edge is stale). addUsersToNativeGroup rejects the whole
    // batch atomically if any requested URN is absent, so migration must filter stale URNs out
    // itself rather than let one bad edge empty the group's membership entirely.
    EntityRelationships relationshipsWithStaleMember =
        new EntityRelationships()
            .setStart(0)
            .setCount(2)
            .setTotal(2)
            .setRelationships(
                new EntityRelationshipArray(
                    ImmutableList.of(
                        new EntityRelationship()
                            .setEntity(USER_URN)
                            .setType(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME),
                        new EntityRelationship()
                            .setEntity(OTHER_USER_URN)
                            .setType(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME))));
    when(_graphClient.getRelatedEntities(
            eq(EXTERNAL_GROUP_URN_STRING),
            eq(ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            any()))
        .thenReturn(relationshipsWithStaleMember);
    when(_entityClient.batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));

    _groupService.migrateGroupMembershipToNativeGroupMembership(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString());

    // Must not throw despite the stale OTHER_USER_URN edge, and must still migrate the member
    // that does exist: one removeExistingGroupMembers proposal for USER_URN, one
    // createNativeGroupOrigin proposal, and one addUsersToNativeGroup proposal for USER_URN.
    ArgumentCaptor<MetadataChangeProposal> proposalCaptor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(_entityClient, times(3))
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture());
    assertTrue(
        proposalCaptor.getAllValues().stream()
            .anyMatch(
                proposal ->
                    USER_URN.equals(proposal.getEntityUrn())
                        && NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME.equals(proposal.getAspectName())));
    assertTrue(
        proposalCaptor.getAllValues().stream()
            .noneMatch(proposal -> OTHER_USER_URN.equals(proposal.getEntityUrn())));
  }

  @Test
  public void testCreateGroupInfoNullArguments() {
    assertThrows(
        () ->
            _groupService.createGroupInfo(
                mock(OperationContext.class), null, GROUP_NAME, GROUP_DESCRIPTION));
    assertThrows(
        () ->
            _groupService.createGroupInfo(
                mock(OperationContext.class), _groupKey, null, GROUP_DESCRIPTION));
    assertThrows(
        () ->
            _groupService.createGroupInfo(
                mock(OperationContext.class), _groupKey, GROUP_NAME, null));
  }

  @Test
  public void testCreateGroupInfoPasses() throws Exception {
    _groupService.createGroupInfo(opContext, _groupKey, GROUP_NAME, GROUP_DESCRIPTION);
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testCreateNativeGroupOriginNullArguments() {
    assertThrows(() -> _groupService.createNativeGroupOrigin(mock(OperationContext.class), null));
  }

  @Test
  public void testCreateNativeGroupOriginPasses() throws Exception {
    _groupService.createNativeGroupOrigin(opContext, _groupUrn);
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testGetExistingGroupMembersNullArguments() {
    assertThrows(() -> _groupService.getExistingGroupMembers(null, USER_URN.toString()));
  }

  @Test
  public void testGetExistingGroupMembersPasses() {
    when(_graphClient.getRelatedEntities(
            eq(GROUP_URN_STRING),
            eq(ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            any()))
        .thenReturn(_entityRelationships);

    assertEquals(
        USER_URN_LIST, _groupService.getExistingGroupMembers(_groupUrn, USER_URN.toString()));
  }

  @Test
  public void testRemoveExistingGroupMembersNullArguments() {
    assertThrows(
        () ->
            _groupService.removeExistingGroupMembers(
                mock(OperationContext.class), null, USER_URN_LIST));
    assertThrows(
        () ->
            _groupService.removeExistingGroupMembers(
                mock(OperationContext.class), _groupUrn, null));
  }

  @Test
  public void testRemoveExistingGroupMembersGroupNotInGroupMembership() throws Exception {
    when(_entityClient.batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingGroupMembers(
        mock(OperationContext.class), Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testRemoveExistingGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingGroupMembers(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testGetGroupsForUserUsesSessionCacheForSessionActor() throws Exception {
    Urn externalGroup = Urn.createFromString(EXTERNAL_GROUP_URN_STRING);
    Urn nativeGroup = Urn.createFromString(NATIVE_GROUP_URN_STRING);
    OperationContext sessionOpContext = mock(OperationContext.class);
    io.datahubproject.metadata.context.ActorContext actorContext =
        mock(io.datahubproject.metadata.context.ActorContext.class);
    when(sessionOpContext.getSessionActorContext()).thenReturn(actorContext);
    when(actorContext.getActorUrn()).thenReturn(USER_URN);
    when(actorContext.getGroupMembership())
        .thenReturn(ImmutableList.of(externalGroup, nativeGroup));

    List<Urn> groups = _groupService.getGroupsForUser(sessionOpContext, USER_URN);

    assertEquals(groups, ImmutableList.of(externalGroup, nativeGroup));
    verifyNoInteractions(_entityClient);
  }

  @Test
  public void testGetGroupsForUserFetchesForNonSessionActor() throws Exception {
    Urn otherUser = new CorpuserUrn("other@email.com");
    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), eq(Set.of(otherUser)), any()))
        .thenReturn(ImmutableMap.of(otherUser, _entityResponseMap.get(USER_URN)));

    List<Urn> groups = _groupService.getGroupsForUser(opContext, otherUser);

    assertEquals(
        groups,
        ImmutableList.of(
            Urn.createFromString(EXTERNAL_GROUP_URN_STRING),
            Urn.createFromString(NATIVE_GROUP_URN_STRING)));
    verify(_entityClient)
        .batchGetV2(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), eq(Set.of(otherUser)), any());
  }

  @Test
  public void testFetchUserIdentityMergesAndDedupesGroups() throws Exception {
    NativeGroupMembership nativeGroupMembership = new NativeGroupMembership();
    nativeGroupMembership.setNativeGroups(
        new UrnArray(
            Urn.createFromString(NATIVE_GROUP_URN_STRING),
            Urn.createFromString(EXTERNAL_GROUP_URN_STRING)));
    GroupMembership groupMembership = new GroupMembership();
    groupMembership.setGroups(new UrnArray(Urn.createFromString(EXTERNAL_GROUP_URN_STRING)));
    RoleMembership roleMembership = new RoleMembership();
    roleMembership.setRoles(new UrnArray(Urn.createFromString("urn:li:dataHubRole:Admin")));

    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    aspectMap.put(
        GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(groupMembership.data())));
    aspectMap.put(
        NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(nativeGroupMembership.data())));
    aspectMap.put(
        ROLE_MEMBERSHIP_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(roleMembership.data())));

    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            eq(CORP_USER_ENTITY_NAME),
            eq(Set.of(USER_URN)),
            eq(
                ImmutableSet.of(
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                    ROLE_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(Map.of(USER_URN, new EntityResponse().setAspects(aspectMap)));

    var identity = _groupService.fetchUserIdentity(opContext, USER_URN);

    assertEquals(identity.getGroups().size(), 2);
    assertTrue(identity.getGroups().contains(Urn.createFromString(EXTERNAL_GROUP_URN_STRING)));
    assertTrue(identity.getGroups().contains(Urn.createFromString(NATIVE_GROUP_URN_STRING)));
    assertEquals(
        identity.getDirectRoles(), Set.of(Urn.createFromString("urn:li:dataHubRole:Admin")));
  }

  @Test
  public void testFetchUserIdentityEmptyWhenUserMissing() throws Exception {
    when(_entityClient.batchGetV2(
            any(OperationContext.class),
            eq(CORP_USER_ENTITY_NAME),
            eq(Set.of(USER_URN)),
            eq(
                ImmutableSet.of(
                    GROUP_MEMBERSHIP_ASPECT_NAME,
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                    ROLE_MEMBERSHIP_ASPECT_NAME))))
        .thenReturn(Map.of());

    var identity = _groupService.fetchUserIdentity(opContext, USER_URN);

    assertTrue(identity.getGroups().isEmpty());
    assertTrue(identity.getDirectRoles().isEmpty());
  }

  @Test
  public void testGetNativeGroupMembersReturnsSinglePage() throws Exception {
    Urn userA = Urn.createFromString("urn:li:corpuser:a");
    when(_graphClient.getRelatedEntities(
            eq(_groupUrn.toString()),
            eq(ImmutableSet.of(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            eq(ACTOR_URN_STRING)))
        .thenReturn(relationshipsPage(1, userA));

    assertEquals(_groupService.getNativeGroupMembers(_groupUrn, ACTOR_URN_STRING), List.of(userA));
  }

  @Test
  public void testGetNativeGroupMembersFollowsPagination() throws Exception {
    Urn userA = Urn.createFromString("urn:li:corpuser:a");
    Urn userB = Urn.createFromString("urn:li:corpuser:b");
    Urn userC = Urn.createFromString("urn:li:corpuser:c");
    when(_graphClient.getRelatedEntities(
            any(), any(), any(), anyInt(), anyInt(), eq(ACTOR_URN_STRING)))
        .thenReturn(relationshipsPage(3, userA, userB), relationshipsPage(3, userC));

    assertEquals(
        _groupService.getNativeGroupMembers(_groupUrn, ACTOR_URN_STRING, 2),
        List.of(userA, userB, userC));
    verify(_graphClient, times(2))
        .getRelatedEntities(any(), any(), any(), anyInt(), anyInt(), any());
  }

  @Test
  public void testGetNativeGroupMembersHandlesNullResponse() {
    when(_graphClient.getRelatedEntities(
            any(), any(), any(), anyInt(), anyInt(), eq(ACTOR_URN_STRING)))
        .thenReturn(null);

    assertTrue(_groupService.getNativeGroupMembers(_groupUrn, ACTOR_URN_STRING).isEmpty());
  }

  private static Map<Urn, EntityResponse> responseWithNativeGroups(Urn userUrn, Urn... groups) {
    NativeGroupMembership membership = new NativeGroupMembership();
    membership.setNativeGroups(new UrnArray(Arrays.asList(groups)));
    return ImmutableMap.of(
        userUrn,
        new EntityResponse()
            .setEntityName(CORP_USER_ENTITY_NAME)
            .setUrn(userUrn)
            .setAspects(
                new EnvelopedAspectMap(
                    ImmutableMap.of(
                        NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                        new EnvelopedAspect().setValue(new Aspect(membership.data()))))));
  }

  private static EntityRelationships relationshipsPage(int total, Urn... members) {
    EntityRelationshipArray array = new EntityRelationshipArray();
    for (Urn member : members) {
      array.add(
          new EntityRelationship()
              .setEntity(member)
              .setType(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME));
    }
    return new EntityRelationships()
        .setStart(0)
        .setCount(members.length)
        .setTotal(total)
        .setRelationships(array);
  }
}
