package com.datahub.authentication.group;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

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
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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

  private EntityClient _entityClient;
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
    when(_entityService.exists(any(OperationContext.class), eq(USER_URN), eq(true)))
        .thenReturn(true);
    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.addUserToNativeGroup(opContext, USER_URN, _groupUrn);
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
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
    when(_entityClient.batchGetV2(
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
    when(_entityClient.batchGetV2(
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
            eq(ImmutableList.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            any()))
        .thenReturn(_entityRelationships);
    when(_entityClient.batchGetV2(any(), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);
    when(_entityService.exists(any(), eq(USER_URN), eq(true))).thenReturn(true);

    _groupService.migrateGroupMembershipToNativeGroupMembership(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN.toString());
    verify(_entityClient, times(3)).ingestProposal(any(OperationContext.class), any());
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
            eq(ImmutableList.of(IS_MEMBER_OF_GROUP_RELATIONSHIP_NAME)),
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
    when(_entityClient.batchGetV2(any(), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingGroupMembers(
        mock(OperationContext.class), Urn.createFromString(NATIVE_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any());
  }

  @Test
  public void testRemoveExistingGroupMembersPasses() throws Exception {
    when(_entityClient.batchGetV2(
            any(OperationContext.class), eq(CORP_USER_ENTITY_NAME), any(), any()))
        .thenReturn(_entityResponseMap);

    _groupService.removeExistingGroupMembers(
        opContext, Urn.createFromString(EXTERNAL_GROUP_URN_STRING), USER_URN_LIST);
    verify(_entityClient).ingestProposal(any(OperationContext.class), any());
  }
}
