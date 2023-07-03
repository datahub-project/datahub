package com.datahub.authorization;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;


public class RoleServiceTest {
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String FIRST_ACTOR_URN_STRING = "urn:li:corpuser:foo";
  private static final String SECOND_ACTOR_URN_STRING = "urn:li:corpuser:bar";
  private static final String DATAHUB_SYSTEM_CLIENT_ID = "__datahub_system";
  private static final Authentication SYSTEM_AUTHENTICATION =
      new Authentication(new Actor(ActorType.USER, DATAHUB_SYSTEM_CLIENT_ID), "");
  private Urn roleUrn;
  private EntityClient _entityClient;
  private RoleService _roleService;

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    _entityClient = mock(EntityClient.class);
    when(_entityClient.exists(eq(roleUrn), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);

    _roleService = new RoleService(_entityClient);
  }

  @Test
  public void testBatchAssignRoleNoActorExists() throws Exception {
    when(_entityClient.exists(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        false);

    _roleService.batchAssignRoleToActors(ImmutableList.of(FIRST_ACTOR_URN_STRING),
        roleUrn,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, never()).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION), eq(false));
  }

  @Test
  public void testBatchAssignRoleSomeActorExists() throws Exception {
    when(_entityClient.exists(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        true);

    _roleService.batchAssignRoleToActors(ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING),
        roleUrn,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(1)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION), eq(false));
  }

  @Test
  public void testBatchAssignRoleAllActorsExist() throws Exception {
    when(_entityClient.exists(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        true);
    when(_entityClient.exists(eq(Urn.createFromString(SECOND_ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        true);

    _roleService.batchAssignRoleToActors(ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING),
        roleUrn,
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(2)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION), eq(false));
  }

  @Test
  public void testAssignNullRoleToActorAllActorsExist() throws Exception {
    when(_entityClient.exists(eq(Urn.createFromString(FIRST_ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        true);

    _roleService.batchAssignRoleToActors(ImmutableList.of(FIRST_ACTOR_URN_STRING), null, SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(1)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION), eq(false));
  }
}
