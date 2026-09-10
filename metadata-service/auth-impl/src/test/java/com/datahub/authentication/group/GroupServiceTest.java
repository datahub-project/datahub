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
import com.google.common.net.HttpHeaders;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.Origin;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.CorpuserUrn;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.graph.GraphFilters;
import com.linkedin.metadata.graph.GraphService;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import com.linkedin.mxe.MetadataChangeProposal;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
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
  private static final int MEMBERSHIP_CLEANUP_BATCH_SIZE = 100;
  private static final long ASPECT_MODIFIED_MS = 1_700_000_000_000L;
  // The delete's own boundary, necessarily later than the aspects it is cleaning up.
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
  private GraphService _graphService;
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
    _graphService = mock(GraphService.class);

    _groupService = new GroupService(_entityClient, _entityService, _graphClient, _graphService);
  }

  @Test
  public void testConstructor() {
    assertThrows(() -> new GroupService(null, _entityService, _graphClient, _graphService));
    assertThrows(() -> new GroupService(_entityClient, null, _graphClient, _graphService));
    assertThrows(() -> new GroupService(_entityClient, _entityService, null, _graphService));
    assertThrows(() -> new GroupService(_entityClient, _entityService, _graphClient, null));

    // Succeeds!
    new GroupService(_entityClient, _entityService, _graphClient, _graphService);
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
    // APP_SOURCE=ui is what keeps UpdateIndicesService inline, so the relationship index is fresh
    // by the time the mutation returns. Batching the writes must not cost that.
    assertEquals(
        UI_SOURCE,
        capturedBatchProposals(1).get(0).getSystemMetadata().getProperties().get(APP_SOURCE));
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

    assertEquals(capturedBatchProposals(1).size(), 1);
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
    assertEquals(capturedBatchProposals(1).size(), 2);
  }

  @Test
  public void testAddUsersToNativeGroupIssuesOneWritePerBatch() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN, OTHER_USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN, OTHER_USER_URN), _groupUrn);

    // The read was already batched; the writes were not, and each carries inline indexing, so the
    // per-member cost is an ES round trip rather than a row write. The client partitions the
    // batch further, so this is one call rather than one transaction per member - not a single
    // transaction; the all-or-nothing guarantee comes from the existence check above, not here.
    verify(_entityClient, times(1))
        .batchIngestProposals(any(OperationContext.class), anyCollection(), eq(false));
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
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
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(memberEdges(null));

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);

    // Already-members are still written: content is unchanged so the MCL is suppressed, but the
    // write still refreshes actor/APP_SOURCE provenance on the row.
    assertEquals(capturedBatchProposals(1).size(), 1);
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
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(memberEdges(null, USER_URN));

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

    verify(_graphService, never())
        .scrollRelatedEntities(any(), any(), any(), any(), any(), any(), any(), any());
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
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
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
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(memberEdges(null));
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
    // Two single writes of its own - dropping the legacy membership and stamping the native origin
    // - plus the batched write that addUsersToNativeGroup now issues.
    verify(_entityClient, times(2)).ingestProposal(any(OperationContext.class), any());
    assertEquals(capturedBatchProposals(1).size(), 1);
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
    verify(_entityClient, times(2))
        .ingestProposal(any(OperationContext.class), proposalCaptor.capture());
    List<MetadataChangeProposal> written = new ArrayList<>(proposalCaptor.getAllValues());
    written.addAll(capturedBatchProposals(1));
    assertTrue(
        written.stream()
            .anyMatch(
                proposal ->
                    USER_URN.equals(proposal.getEntityUrn())
                        && NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME.equals(proposal.getAspectName())));
    assertTrue(
        written.stream().noneMatch(proposal -> OTHER_USER_URN.equals(proposal.getEntityUrn())));
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
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(memberEdges(null, userA));

    assertEquals(_groupService.getNativeGroupMembers(opContext, _groupUrn), List.of(userA));

    ArgumentCaptor<GraphFilters> filters = ArgumentCaptor.forClass(GraphFilters.class);
    verify(_graphService)
        .scrollRelatedEntities(any(), filters.capture(), any(), any(), any(), any(), any(), any());
    assertEquals(
        filters.getValue().getRelationshipTypes(),
        Set.of(IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME));
    assertEquals(
        filters.getValue().getRelationshipFilter().getDirection(), RelationshipDirection.INCOMING);
    assertEquals(
        criterionValues(filters.getValue().getSourceEntityFilter()), List.of(GROUP_URN_STRING));
    // No destination filter: every member of the group is wanted here.
    assertTrue(filters.getValue().getDestinationEntityFilter().getOr().isEmpty());
  }

  @Test
  public void testGetNativeGroupMembersFollowsScroll() throws Exception {
    // Paging must follow scrollIds rather than from/size offsets — offset paging is rejected once
    // from + size passes index.max_result_window, which would fail the read for the largest groups
    // instead of returning their tail.
    Urn userA = Urn.createFromString("urn:li:corpuser:a");
    Urn userB = Urn.createFromString("urn:li:corpuser:b");
    Urn userC = Urn.createFromString("urn:li:corpuser:c");
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(memberEdges("page-2", userA, userB), memberEdges(null, userC));

    assertEquals(
        _groupService.getNativeGroupMembers(opContext, _groupUrn, 2), List.of(userA, userB, userC));

    ArgumentCaptor<String> scrollIds = ArgumentCaptor.forClass(String.class);
    verify(_graphService, times(2))
        .scrollRelatedEntities(
            any(), any(), any(), scrollIds.capture(), any(), any(), any(), any());
    assertEquals(scrollIds.getAllValues(), Arrays.asList(null, "page-2"));
  }

  @Test
  public void testGetNativeGroupMembersHandlesNullResponse() {
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(null);

    assertTrue(_groupService.getNativeGroupMembers(opContext, _groupUrn).isEmpty());
  }

  @Test
  public void testAddUsersToNativeGroupChecksEdgesOfRequestedUsersOnly() throws Exception {
    // The divergence check filters both endpoints, so its cost tracks the size of the request
    // rather than the size of the group — and it never has to page a large group's membership.
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of(USER_URN));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(responseWithNativeGroups(USER_URN, _groupUrn));
    when(_graphService.scrollRelatedEntities(
            any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(memberEdges(null, USER_URN));

    _groupService.addUsersToNativeGroup(opContext, List.of(USER_URN), _groupUrn);

    ArgumentCaptor<GraphFilters> filters = ArgumentCaptor.forClass(GraphFilters.class);
    verify(_graphService)
        .scrollRelatedEntities(any(), filters.capture(), any(), any(), any(), any(), any(), any());
    assertEquals(
        criterionValues(filters.getValue().getSourceEntityFilter()), List.of(GROUP_URN_STRING));
    assertEquals(
        criterionValues(filters.getValue().getDestinationEntityFilter()),
        List.of(USER_URN.toString()));
  }

  @Test
  public void testRemoveStaleNativeGroupMembershipWritesOneBatchPerChunk() throws Exception {
    List<Urn> captured = new ArrayList<>();
    for (int i = 0; i < MEMBERSHIP_CLEANUP_BATCH_SIZE + 50; i++) {
      captured.add(new CorpuserUrn("user" + i + "@email.com"));
    }
    mockNativeGroupMembershipReads(_groupUrn);

    _groupService.removeStaleNativeGroupMembership(opContext, _groupUrn, captured);

    // One aspect read and one write per batch, not a pair per member.
    verify(_entityClient, times(2))
        .batchGetV2NoCache(any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any());
    assertEquals(capturedBatchProposals(2).size(), captured.size());
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
    // The sweep works purely from the captured list and the aspects themselves; it never reads the
    // graph. A graph read here would mistake a not-yet-indexed edge for an absent member — the
    // same lag that makes deleteEntityReferences miss a member added just before the delete.
    verify(_graphService, never())
        .scrollRelatedEntities(any(), any(), any(), any(), any(), any(), any(), any());
    verify(_entityService, never())
        .exists(any(OperationContext.class), eq(_groupUrn), anyBoolean());
  }

  @Test
  public void testRemoveStaleNativeGroupMembershipWritesWithoutAPrecondition() throws Exception {
    mockNativeGroupMembershipReads(_groupUrn);

    _groupService.removeStaleNativeGroupMembership(opContext, _groupUrn, List.of(USER_URN));

    // A re-add that lands while the aspect still names the group writes byte-identical content, so
    // it never advances the audit stamp an If-Unmodified-Since would read. The guard therefore
    // cannot fire for the delete/recreate race it was meant to cover, and only ever blocks
    // unrelated writes - leaving a stale reference where it claimed to protect a membership. The
    // sweep cleans every captured member instead; an admin who loses that race re-adds the member.
    MetadataChangeProposal proposal = capturedBatchProposals(1).get(0);
    assertFalse(
        proposal.hasHeaders()
            && proposal.getHeaders().containsKey(HttpHeaders.IF_UNMODIFIED_SINCE));
  }

  @Test
  public void testRemoveStaleNativeGroupMembershipDoesNotRetryPerMemberWhenTheBatchFails()
      throws Exception {
    mockNativeGroupMembershipReads(_groupUrn);
    when(_entityClient.batchIngestProposals(
            any(OperationContext.class), anyCollection(), eq(false)))
        .thenThrow(new RuntimeException("simulated storage failure"));

    _groupService.removeStaleNativeGroupMembership(
        opContext, _groupUrn, List.of(USER_URN, OTHER_USER_URN));

    // With no precondition to reject it, a batch only fails for reasons a per-member replay would
    // hit again. The batch is counted failed and left for reconciliation rather than retried.
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testRemoveStaleNativeGroupMembershipLeavesBatchAloneWhenTheReadFails()
      throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenThrow(new RuntimeException("simulated storage failure"));

    _groupService.removeStaleNativeGroupMembership(
        opContext, _groupUrn, List.of(USER_URN, OTHER_USER_URN));

    // Nothing is known about these members. A reference left behind is repaired by the next add to
    // this urn; one stripped by mistake silently revokes access.
    verify(_entityClient, never())
        .batchIngestProposals(any(OperationContext.class), anyCollection(), anyBoolean());
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testRemoveStaleNativeGroupMembershipSkipsMembersNotNamingTheGroup() throws Exception {
    mockNativeGroupMembershipReads(Urn.createFromString(EXTERNAL_GROUP_URN_STRING));

    _groupService.removeStaleNativeGroupMembership(opContext, _groupUrn, List.of(USER_URN));

    verify(_entityClient, never())
        .batchIngestProposals(any(OperationContext.class), anyCollection(), anyBoolean());
  }

  @SuppressWarnings("rawtypes")
  private List<MetadataChangeProposal> capturedBatchProposals(int expectedBatches)
      throws Exception {
    ArgumentCaptor<Collection> batches = ArgumentCaptor.forClass(Collection.class);
    verify(_entityClient, times(expectedBatches))
        .batchIngestProposals(any(OperationContext.class), batches.capture(), eq(false));
    List<MetadataChangeProposal> proposals = new ArrayList<>();
    for (Collection batch : batches.getAllValues()) {
      for (Object proposal : batch) {
        proposals.add((MetadataChangeProposal) proposal);
      }
    }
    return proposals;
  }

  private void mockNativeGroupMembershipReads(Urn... groups) throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              Set<Urn> requested = invocation.getArgument(2);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Urn userUrn : requested) {
                responses.put(userUrn, stampedResponseWithNativeGroups(userUrn, groups));
              }
              return responses;
            });
  }

  /** Carries an audit stamp, which the delete-cleanup write turns into its precondition. */
  private static EntityResponse stampedResponseWithNativeGroups(Urn userUrn, Urn... groups) {
    NativeGroupMembership membership = new NativeGroupMembership();
    membership.setNativeGroups(new UrnArray(Arrays.asList(groups)));
    return new EntityResponse()
        .setEntityName(CORP_USER_ENTITY_NAME)
        .setUrn(userUrn)
        .setAspects(
            new EnvelopedAspectMap(
                ImmutableMap.of(
                    NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
                    new EnvelopedAspect()
                        .setValue(new Aspect(membership.data()))
                        .setCreated(
                            new AuditStamp()
                                .setTime(ASPECT_MODIFIED_MS)
                                .setActor(UrnUtils.getUrn(ACTOR_URN_STRING))))));
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

  private static RelatedEntitiesScrollResult memberEdges(String nextScrollId, Urn... members) {
    List<RelatedEntities> entities = new ArrayList<>();
    for (Urn member : members) {
      entities.add(
          new RelatedEntities(
              IS_MEMBER_OF_NATIVE_GROUP_RELATIONSHIP_NAME,
              member.toString(),
              GROUP_URN_STRING,
              RelationshipDirection.INCOMING,
              null));
    }
    return RelatedEntitiesScrollResult.builder()
        .entities(entities)
        .pageSize(entities.size())
        .numResults(entities.size())
        .scrollId(nextScrollId)
        .build();
  }

  private static List<String> criterionValues(Filter filter) {
    return filter.getOr().get(0).getAnd().get(0).getValues();
  }
}
