package com.datahub.authentication.group;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import com.linkedin.entity.client.EntityClient;
import com.linkedin.identity.GroupMembership;
import com.linkedin.identity.NativeGroupMembership;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.key.CorpGroupKey;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


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

  private EntityClient _entityClient;
  private EntityService _entityService;
  private GraphClient _graphClient;
  private GroupService _groupService;

  @BeforeMethod
  public void setupTest() throws Exception {
    _groupUrn = Urn.createFromString(GROUP_URN_STRING);
    _groupKey = new CorpGroupKey();
    _groupKey.setName(GROUP_ID);

    NativeGroupMembership nativeGroupMembership = new NativeGroupMembership();
    nativeGroupMembership.setNativeGroups(new UrnArray(Urn.createFromString(NATIVE_GROUP_URN_STRING)));
    GroupMembership groupMembership = new GroupMembership();
    groupMembership.setGroups(new UrnArray(Urn.createFromString(EXTERNAL_GROUP_URN_STRING)));
    _entityResponseMap = ImmutableMap.of(USER_URN, new EntityResponse().setEntityName(CORP_USER_ENTITY_NAME)
        .setUrn(USER_URN)
        .setAspects(new EnvelopedAspectMap(ImmutableMap.of(NATIVE_GROUP_MEMBERSHIP_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(nativeGroupMembership.data())), GROUP_MEMBERSHIP_ASPECT_NAME,
            new EnvelopedAspect().setValue(new Aspect(groupMembership.data()))))));

    _entityRelationships = new EntityRelationships().setStart(0)
        .setCount(1)
        .setTotal(1)
        .setRelationships(new EntityRelationshipArray(ImmutableList.of(
            new EntityRelationship().setEntity(USER_URN).setType(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME))));

    _entityClient = mock(EntityClient.class);
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
    assertThrows(() -> _groupService.groupExists(null));
  }

  @Test
  public void testGroupExistsPasses() {
    when(_entityService.exists(_groupUrn)).thenReturn(true);
    assertTrue(_groupService.groupExists(_groupUrn));
  }

  @Test
  public void testGetGroupOriginNullArguments() {
    assertThrows(() -> _groupService.getGroupOrigin(null));
  }

  @Test
  public void testGetGroupOriginPasses() {
    Origin groupOrigin = mock(Origin.class);
    when(_entityService.getLatestAspect(eq(_groupUrn), eq(ORIGIN_ASPECT_NAME))).thenReturn(groupOrigin);

    assertEquals(groupOrigin, _groupService.getGroupOrigin(_groupUrn));
  }

  @Test
  public void testAddUserToNativeGroupNullArguments() {
    assertThrows(() -> _groupService.addUserToNativeGroup(null, _groupUrn, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.addUserToNativeGroup(USER_URN, null, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testAddUserToNativeGroupPasses() throws Exception {
    when(_entityService.exists(USER_URN)).thenReturn(true);
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), any(), any(), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        _entityResponseMap);

    _groupService.addUserToNativeGroup(USER_URN, _groupUrn, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateNativeGroupNullArguments() {
    assertThrows(() -> _groupService.createNativeGroup(null, GROUP_NAME, GROUP_DESCRIPTION, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.createNativeGroup(_groupKey, null, GROUP_DESCRIPTION, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.createNativeGroup(_groupKey, GROUP_NAME, null, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateNativeGroupPasses() throws Exception {
    _groupService.createNativeGroup(_groupKey, GROUP_NAME, GROUP_DESCRIPTION, SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(2)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testRemoveExistingNativeGroupMembersNullArguments() {
    assertThrows(() -> _groupService.removeExistingNativeGroupMembers(null, USER_URN_LIST, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.removeExistingNativeGroupMembers(_groupUrn, null, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testRemoveExistingNativeGroupMembersGroupNotInNativeGroupMembership() throws Exception {
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), any(), any(), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        _entityResponseMap);

    _groupService.removeExistingNativeGroupMembers(Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, never()).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testRemoveExistingNativeGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), any(), any(), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        _entityResponseMap);

    _groupService.removeExistingNativeGroupMembers(Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testMigrateGroupMembershipToNativeGroupMembershipNullArguments() {
    assertThrows(() -> _groupService.migrateGroupMembershipToNativeGroupMembership(null, USER_URN.toString(),
        SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testMigrateGroupMembershipToNativeGroupMembershipPasses() throws Exception {
    when(_graphClient.getRelatedEntities(eq(EXTERNAL_GROUP_URN_STRING),
        eq(ImmutableList.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)), eq(RelationshipDirection.INCOMING), anyInt(),
        anyInt(), any())).thenReturn(_entityRelationships);
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), any(), any(), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        _entityResponseMap);
    when(_entityService.exists(USER_URN)).thenReturn(true);

    _groupService.migrateGroupMembershipToNativeGroupMembership(Urn.createFromString(EXTERNAL_GROUP_URN_STRING),
        USER_URN.toString(), SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(3)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateGroupInfoNullArguments() {
    assertThrows(() -> _groupService.createGroupInfo(null, GROUP_NAME, GROUP_DESCRIPTION, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.createGroupInfo(_groupKey, null, GROUP_DESCRIPTION, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.createGroupInfo(_groupKey, GROUP_NAME, null, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateGroupInfoPasses() throws Exception {
    _groupService.createGroupInfo(_groupKey, GROUP_NAME, GROUP_DESCRIPTION, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateNativeGroupOriginNullArguments() {
    assertThrows(() -> _groupService.createNativeGroupOrigin(null, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testCreateNativeGroupOriginPasses() throws Exception {
    _groupService.createNativeGroupOrigin(_groupUrn, SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testGetExistingGroupMembersNullArguments() {
    assertThrows(() -> _groupService.getExistingGroupMembers(null, USER_URN.toString()));
  }

  @Test
  public void testGetExistingGroupMembersPasses() {
    when(_graphClient.getRelatedEntities(eq(GROUP_URN_STRING),
        eq(ImmutableList.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)), eq(RelationshipDirection.INCOMING), anyInt(),
        anyInt(), any())).thenReturn(_entityRelationships);

    assertEquals(USER_URN_LIST, _groupService.getExistingGroupMembers(_groupUrn, USER_URN.toString()));
  }

  @Test
  public void testRemoveExistingGroupMembersNullArguments() {
    assertThrows(() -> _groupService.removeExistingGroupMembers(null, USER_URN_LIST, SYSTEM_AUTHENTICATION));
    assertThrows(() -> _groupService.removeExistingGroupMembers(_groupUrn, null, SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testRemoveExistingGroupMembersGroupNotInGroupMembership() throws Exception {
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), any(), any(), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        _entityResponseMap);

    _groupService.removeExistingGroupMembers(Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, never()).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testRemoveExistingGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2(eq(CORP_USER_ENTITY_NAME), any(), any(), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        _entityResponseMap);

    _groupService.removeExistingGroupMembers(Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }
}
