package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.ArgumentMatchers.argThat;
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
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
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
  private static final Urn USER_URN = new CorpuserUrn(EMAIL);
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
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<MetadataChangeProposal>> proposalCaptor =
        ArgumentCaptor.forClass(Collection.class);
    verify(_entityClient)
        .batchIngestProposals(any(OperationContext.class), proposalCaptor.capture(), eq(false));
    assertEquals(
        UI_SOURCE,
        proposalCaptor
            .getValue()
            .iterator()
            .next()
            .getSystemMetadata()
            .getProperties()
            .get(APP_SOURCE));
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

    verify(_entityClient).batchIngestProposals(any(OperationContext.class), any(), eq(false));
    verify(_entityClient).batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
    verify(_entityClient, never()).batchGetV2(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
  }

  @Test
  public void testAddUsersToNativeGroupRoundTripsDoNotScaleWithUserCount() throws Exception {
    final List<Urn> userUrns =
        IntStream.range(0, 25)
            .mapToObj(i -> (Urn) new CorpuserUrn("user" + i + "@email.com"))
            .collect(Collectors.toList());
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(new HashSet<>(userUrns));
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of());

    _groupService.addUsersToNativeGroup(opContext, userUrns, _groupUrn);

    // One existence check, one aspect read and one write for the whole set - not three per user.
    verify(_entityService).exists(any(OperationContext.class), anyCollection(), eq(true));
    verify(_entityClient).batchGetV2NoCache(any(), eq(CORP_USER_ENTITY_NAME), any(), any());
    @SuppressWarnings("unchecked")
    ArgumentCaptor<Collection<MetadataChangeProposal>> proposalCaptor =
        ArgumentCaptor.forClass(Collection.class);
    verify(_entityClient)
        .batchIngestProposals(any(OperationContext.class), proposalCaptor.capture(), eq(false));
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
    assertEquals(proposalCaptor.getValue().size(), userUrns.size());
  }

  @Test
  public void testAddUsersToNativeGroupRejectsMissingUsersBeforeWriting() throws Exception {
    when(_entityService.exists(any(OperationContext.class), anyCollection(), eq(true)))
        .thenReturn(Set.of());

    assertThrows(() -> _groupService.addUsersToNativeGroup(opContext, USER_URN_LIST, _groupUrn));

    // The whole request is rejected up front, rather than committing a prefix of the users and
    // then failing partway through the list.
    verify(_entityClient, never()).batchIngestProposals(any(), any(), anyBoolean());
    verify(_entityClient, never()).batchGetV2NoCache(any(), any(), any(), any());
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

    verify(_entityClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testRemoveExistingGroupMembersNoOpWhenAspectMissing() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of());

    _groupService.removeExistingGroupMembers(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST);

    verify(_entityClient, never()).batchIngestProposals(any(), any(), anyBoolean());
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
  public void testRemoveExistingNativeGroupMembersStripsLegacyMembership() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    // The fixture user belongs to EXTERNAL_GROUP via the legacy groupMembership aspect only. A
    // native-only removal would silently leave them in the group while reporting success.
    _groupService.removeExistingNativeGroupMembers(
        mock(OperationContext.class),
        Urn.createFromString(EXTERNAL_GROUP_URN_STRING),
        USER_URN_LIST);

    verify(_entityClient)
        .batchIngestProposals(
            any(OperationContext.class),
            argThat(mcps -> allHaveAspect(mcps, GROUP_MEMBERSHIP_ASPECT_NAME)),
            eq(false));
  }

  @Test
  public void testRemoveExistingNativeGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingNativeGroupMembers(
        opContext, Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient).batchIngestProposals(any(OperationContext.class), any(), eq(false));
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
    when(_entityService.exists(any(), anyCollection(), eq(true))).thenReturn(Set.of(USER_URN));

    _groupService.migrateGroupMembershipToNativeGroupMembership(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString());
    // Two batched membership writes (native grant, old revoke) plus the single Origin write.
    verify(_entityClient, times(2))
        .batchIngestProposals(any(OperationContext.class), any(), eq(false));
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testMigrateGroupMembershipWritesOriginLast() throws Exception {
    mockMigrationDependencies();

    _groupService.migrateGroupMembershipToNativeGroupMembership(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString());

    // Native membership must be granted before the old membership is revoked, because the member
    // list is read from graph edges derived from GroupMembership. Origin comes last so that an
    // interrupted run stays re-migratable.
    final InOrder inOrder = inOrder(_entityClient);
    inOrder
        .verify(_entityClient)
        .batchIngestProposals(
            any(OperationContext.class),
            argThat(mcps -> allHaveAspect(mcps, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)),
            eq(false));
    inOrder
        .verify(_entityClient)
        .batchIngestProposals(
            any(OperationContext.class),
            argThat(mcps -> allHaveAspect(mcps, GROUP_MEMBERSHIP_ASPECT_NAME)),
            eq(false));
    inOrder
        .verify(_entityClient)
        .ingestProposal(
            any(OperationContext.class),
            argThat(mcp -> ORIGIN_ASPECT_NAME.equals(mcp.getAspectName())));
  }

  @Test
  public void testMigrateGroupMembershipInterruptedLeavesOriginUnset() throws Exception {
    mockMigrationDependencies();
    // Fail the native membership grant specifically: that is the step which used to be sequenced
    // after the Origin write, so a failure there left members stripped of GroupMembership, never
    // granted NativeGroupMembership, and permanently ineligible for re-migration.
    when(_entityClient.batchIngestProposals(
            any(OperationContext.class),
            argThat(mcps -> allHaveAspect(mcps, NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME)),
            anyBoolean()))
        .thenThrow(new RuntimeException("Migration interrupted"));

    assertThrows(
        () ->
            _groupService.migrateGroupMembershipToNativeGroupMembership(
                opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString()));

    // A set Origin permanently disables the migration guard in the Add/RemoveGroupMembers
    // resolvers, so leaving it unset is what allows the next call to retry the migration.
    verify(_entityClient, never())
        .ingestProposal(
            any(OperationContext.class),
            argThat(mcp -> ORIGIN_ASPECT_NAME.equals(mcp.getAspectName())));
  }

  @Test
  public void testMigrateGroupMembershipSkipsUsersThatNoLongerExist() throws Exception {
    mockMigrationDependencies();
    // A graph edge can outlive a hard-deleted user. Failing on it would leave the group unable to
    // ever finish migrating, since Origin is only written on a completed run.
    when(_entityService.exists(any(), anyCollection(), eq(true))).thenReturn(Set.of());

    _groupService.migrateGroupMembershipToNativeGroupMembership(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString());

    verify(_entityClient)
        .ingestProposal(
            any(OperationContext.class),
            argThat(mcp -> ORIGIN_ASPECT_NAME.equals(mcp.getAspectName())));
  }

  private void mockMigrationDependencies() throws Exception {
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
    when(_entityService.exists(any(), anyCollection(), eq(true))).thenReturn(Set.of(USER_URN));
  }

  private static boolean allHaveAspect(
      Collection<MetadataChangeProposal> proposals, String aspectName) {
    return !proposals.isEmpty()
        && proposals.stream().allMatch(mcp -> aspectName.equals(mcp.getAspectName()));
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
  public void testGetExistingGroupMembersPagesPastTheFirstPage() {
    // First page comes back full, so a second page must be requested.
    final EntityRelationships fullPage = relationshipsPage(0, 500, 501);
    final EntityRelationships lastPage = relationshipsPage(500, 1, 501);
    when(_graphClient.getRelatedEntities(
            eq(GROUP_URN_STRING),
            eq(ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            anyInt(),
            any()))
        .thenReturn(fullPage);
    when(_graphClient.getRelatedEntities(
            eq(GROUP_URN_STRING),
            eq(ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            eq(500),
            anyInt(),
            any()))
        .thenReturn(lastPage);

    assertEquals(_groupService.getExistingGroupMembers(_groupUrn, USER_URN.toString()).size(), 501);
  }

  @Test
  public void testGetExistingGroupMembersStopsAtTheOffsetPagingCeiling() {
    // A graph that always returns a full page would otherwise loop forever.
    when(_graphClient.getRelatedEntities(
            eq(GROUP_URN_STRING),
            eq(ImmutableSet.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            any()))
        .thenReturn(relationshipsPage(0, 500, Integer.MAX_VALUE));

    assertEquals(
        _groupService.getExistingGroupMembers(_groupUrn, USER_URN.toString()).size(), 10_000);
  }

  private static EntityRelationships relationshipsPage(int start, int count, int total) {
    final List<EntityRelationship> page =
        IntStream.range(0, count)
            .mapToObj(
                i ->
                    new EntityRelationship()
                        .setEntity(new CorpuserUrn("user" + (start + i) + "@email.com"))
                        .setType(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME))
            .collect(Collectors.toList());
    return new EntityRelationships()
        .setStart(start)
        .setCount(count)
        .setTotal(total)
        .setRelationships(new EntityRelationshipArray(page));
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
    verify(_entityClient, never()).batchIngestProposals(any(), any(), anyBoolean());
  }

  @Test
  public void testRemoveExistingGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2NoCache(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingGroupMembers(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient).batchIngestProposals(any(OperationContext.class), any(), eq(false));
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
}
