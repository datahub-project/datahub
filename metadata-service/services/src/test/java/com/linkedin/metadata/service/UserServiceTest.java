package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableSet;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import io.datahubproject.metadata.context.ActorContext;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@Test
public class UserServiceTest {

  private static final String USER1_URN = "urn:li:corpuser:user1";
  private static final String USER2_URN = "urn:li:corpuser:user2";
  private static final String USER3_URN = "urn:li:corpuser:user3";
  private static final String GROUP1_URN = "urn:li:corpGroup:group1";
  private static final String GROUP2_URN = "urn:li:corpGroup:group2";
  private static final String ROLE1_URN = "urn:li:dataHubRole:role1";
  private static final String ROLE2_URN = "urn:li:dataHubRole:role2";
  private static final String NATIVE_GROUP_URN = "urn:li:corpGroup:native-group";
  private static final String SYSTEM_ACTOR_URN = "urn:li:corpuser:system";

  private UserService _userService;
  private EntityClient _entityClient;
  private GraphClient _graphClient;
  private OperationContext _operationContext;
  private ActorContext _actorContext;

  @BeforeMethod
  public void setup() {
    _entityClient = mock(EntityClient.class);
    _graphClient = mock(GraphClient.class);
    _operationContext = mock(OperationContext.class);
    _actorContext = mock(ActorContext.class);

    // Mock the ActorContext and ActorUrn
    when(_operationContext.getActorContext()).thenReturn(_actorContext);
    when(_actorContext.getActorUrn()).thenReturn(UrnUtils.getUrn(SYSTEM_ACTOR_URN));

    _userService = new UserService(_entityClient, _operationContext, _graphClient);
  }

  @Test
  public void testResolveGroupUsers() throws Exception {
    // Setup mock relationships
    List<Urn> groups = Arrays.asList(UrnUtils.getUrn(GROUP1_URN), UrnUtils.getUrn(GROUP2_URN));

    // Mock group1 members
    EntityRelationships group1Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN), UrnUtils.getUrn(USER2_URN)));

    // Mock group2 members
    EntityRelationships group2Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER2_URN), UrnUtils.getUrn(USER3_URN)));

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(group1Relationships);

    when(_graphClient.getRelatedEntities(
            eq(GROUP2_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(group2Relationships);

    // Execute
    List<Urn> result = _userService.resolveGroupUsers(_operationContext, groups);

    // Verify - should contain all users, deduplicated
    assertEquals(result.size(), 3);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER2_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER3_URN)));
  }

  @Test
  public void testResolveGroupUsersWithPagination() throws Exception {
    List<Urn> groups = Arrays.asList(UrnUtils.getUrn(GROUP1_URN));

    // Mock paginated results - make total > batch size (1000) to trigger pagination
    EntityRelationships page1 =
        createMockRelationships(Arrays.asList(UrnUtils.getUrn(USER1_URN)), 1500);
    page1.setStart(0);
    page1.setCount(1);

    // Second page has 1 result, total is still 1500
    EntityRelationships page2 =
        createMockRelationships(Arrays.asList(UrnUtils.getUrn(USER2_URN)), 1500);
    page2.setStart(1000);
    page2.setCount(1);

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            eq(0),
            anyInt(),
            anyString()))
        .thenReturn(page1);

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            eq(1000),
            anyInt(),
            anyString()))
        .thenReturn(page2);

    // Execute
    List<Urn> result = _userService.resolveGroupUsers(_operationContext, groups);

    // Verify
    assertEquals(result.size(), 2);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER2_URN)));

    // Verify pagination calls
    verify(_graphClient, times(2))
        .getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString());
  }

  @Test
  public void testResolveGroupUsersEmpty() throws Exception {
    List<Urn> result = _userService.resolveGroupUsers(_operationContext, Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetUsersInGroup() throws Exception {
    Urn groupUrn = UrnUtils.getUrn(GROUP1_URN);

    EntityRelationships relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN), UrnUtils.getUrn(USER2_URN)));

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(relationships);

    // Execute
    List<Urn> result = _userService.getUsersInGroup(_operationContext, groupUrn);

    // Verify
    assertEquals(result.size(), 2);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER2_URN)));
  }

  @Test
  public void testResolveActorsFromRoles() throws Exception {
    List<Urn> roles = Arrays.asList(UrnUtils.getUrn(ROLE1_URN), UrnUtils.getUrn(ROLE2_URN));

    // Mock role1 members (users and groups)
    EntityRelationships role1Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN), UrnUtils.getUrn(GROUP1_URN)));

    // Mock role2 members
    EntityRelationships role2Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER2_URN), UrnUtils.getUrn(GROUP2_URN)));

    when(_graphClient.getRelatedEntities(
            eq(ROLE1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(role1Relationships);

    when(_graphClient.getRelatedEntities(
            eq(ROLE2_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(role2Relationships);

    // Execute
    List<Urn> result = _userService.resolveActorsFromRoles(_operationContext, roles);

    // Verify - should contain all actors (users and groups)
    assertEquals(result.size(), 4);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER2_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(GROUP1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(GROUP2_URN)));
  }

  @Test
  public void testResolveActorsFromRolesEmpty() throws Exception {
    List<Urn> result =
        _userService.resolveActorsFromRoles(_operationContext, Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testGetActorsWithRole() throws Exception {
    Urn roleUrn = UrnUtils.getUrn(ROLE1_URN);

    EntityRelationships relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN), UrnUtils.getUrn(GROUP1_URN)));

    when(_graphClient.getRelatedEntities(
            eq(ROLE1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(relationships);

    // Execute
    List<Urn> result = _userService.getActorsWithRole(_operationContext, roleUrn);

    // Verify
    assertEquals(result.size(), 2);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(GROUP1_URN)));
  }

  @Test
  public void testResolveToIndividualUsers() throws Exception {
    // Setup test data
    List<Urn> users = Arrays.asList(UrnUtils.getUrn(USER1_URN));
    List<Urn> groups = Arrays.asList(UrnUtils.getUrn(GROUP1_URN));
    List<Urn> roles = Arrays.asList(UrnUtils.getUrn(ROLE1_URN));

    // Mock group resolution
    EntityRelationships groupRelationships =
        createMockRelationships(Arrays.asList(UrnUtils.getUrn(USER2_URN)));

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(groupRelationships);

    // Mock role resolution (returns users and groups)
    EntityRelationships roleRelationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER3_URN), UrnUtils.getUrn(GROUP2_URN)));

    when(_graphClient.getRelatedEntities(
            eq(ROLE1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(roleRelationships);

    // Mock group2 resolution from role
    EntityRelationships group2Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN))); // Duplicate user for deduplication test

    when(_graphClient.getRelatedEntities(
            eq(GROUP2_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(group2Relationships);

    // Execute
    List<Urn> result =
        _userService.resolveToIndividualUsers(_operationContext, users, groups, roles);

    // Verify - should contain all users, deduplicated
    assertEquals(result.size(), 3);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER2_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER3_URN)));
  }

  @Test
  public void testResolveToIndividualUsersWithNullInputs() throws Exception {
    // Test with null inputs
    List<Urn> result = _userService.resolveToIndividualUsers(_operationContext, null, null, null);
    assertTrue(result.isEmpty());
  }

  @Test
  public void testResolveToIndividualUsersWithEmptyInputs() throws Exception {
    // Test with empty inputs
    List<Urn> result =
        _userService.resolveToIndividualUsers(
            _operationContext,
            Collections.emptyList(),
            Collections.emptyList(),
            Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  @Test
  public void testResolveToIndividualUsersDeduplication() throws Exception {
    // Test deduplication when same user appears in multiple sources
    List<Urn> users = Arrays.asList(UrnUtils.getUrn(USER1_URN));
    List<Urn> groups = Arrays.asList(UrnUtils.getUrn(GROUP1_URN));

    // Mock group that contains the same user
    EntityRelationships groupRelationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN))); // Same user as in direct users

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(groupRelationships);

    // Execute
    List<Urn> result =
        _userService.resolveToIndividualUsers(
            _operationContext, users, groups, Collections.emptyList());

    // Verify - should deduplicate
    assertEquals(result.size(), 1);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
  }

  @Test
  public void testResolveToIndividualUsersWithGraphClientException() throws Exception {
    // Test error handling when GraphClient throws exception
    List<Urn> groups = Arrays.asList(UrnUtils.getUrn(GROUP1_URN));

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenThrow(new RuntimeException("Graph client error"));

    // Execute - should not throw exception
    List<Urn> result =
        _userService.resolveToIndividualUsers(
            _operationContext, Collections.emptyList(), groups, Collections.emptyList());

    // Verify - should return empty list due to error
    assertTrue(result.isEmpty());
  }

  @Test
  public void testResolveRoleUsers() throws Exception {
    List<Urn> roles = Arrays.asList(UrnUtils.getUrn(ROLE1_URN), UrnUtils.getUrn(ROLE2_URN));

    // Mock role1 members (users and groups)
    EntityRelationships role1Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN), UrnUtils.getUrn(GROUP1_URN)));

    // Mock role2 members
    EntityRelationships role2Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER2_URN), UrnUtils.getUrn(GROUP2_URN)));

    when(_graphClient.getRelatedEntities(
            eq(ROLE1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(role1Relationships);

    when(_graphClient.getRelatedEntities(
            eq(ROLE2_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(role2Relationships);

    // Mock group resolution
    EntityRelationships group1Relationships =
        createMockRelationships(Arrays.asList(UrnUtils.getUrn(USER3_URN)));
    EntityRelationships group2Relationships =
        createMockRelationships(
            Arrays.asList(UrnUtils.getUrn(USER1_URN))); // Duplicate for deduplication test

    when(_graphClient.getRelatedEntities(
            eq(GROUP1_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(group1Relationships);

    when(_graphClient.getRelatedEntities(
            eq(GROUP2_URN),
            any(ImmutableSet.class),
            eq(RelationshipDirection.INCOMING),
            anyInt(),
            anyInt(),
            anyString()))
        .thenReturn(group2Relationships);

    // Execute
    List<Urn> result = _userService.resolveRoleUsers(_operationContext, roles);

    // Verify - should contain all users from roles and groups, deduplicated
    assertEquals(result.size(), 3);
    assertTrue(result.contains(UrnUtils.getUrn(USER1_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER2_URN)));
    assertTrue(result.contains(UrnUtils.getUrn(USER3_URN)));
  }

  @Test
  public void testResolveRoleUsersEmpty() throws Exception {
    List<Urn> result = _userService.resolveRoleUsers(_operationContext, Collections.emptyList());
    assertTrue(result.isEmpty());
  }

  private EntityRelationships createMockRelationships(List<Urn> relatedUrns) {
    return createMockRelationships(relatedUrns, relatedUrns.size());
  }

  private EntityRelationships createMockRelationships(List<Urn> relatedUrns, int total) {
    EntityRelationships relationships = new EntityRelationships();
    relationships.setTotal(total);
    relationships.setStart(0);
    relationships.setCount(relatedUrns.size());

    List<EntityRelationship> relationshipList = new ArrayList<>();
    for (Urn urn : relatedUrns) {
      EntityRelationship relationship = new EntityRelationship();
      relationship.setEntity(urn);
      relationship.setType("IS_MEMBER_OF_GROUP");
      relationshipList.add(relationship);
    }

    relationships.setRelationships(new EntityRelationshipArray(relationshipList));
    return relationships;
  }
}
