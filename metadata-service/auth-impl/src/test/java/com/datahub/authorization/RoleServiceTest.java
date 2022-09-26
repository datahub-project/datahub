package com.datahub.authorization;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class RoleServiceTest {
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:foo";
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

    _roleService = new RoleService(_entityClient);
  }

  @Test
  public void testRoleExists() throws Exception {
    when(_entityClient.exists(eq(roleUrn), eq(SYSTEM_AUTHENTICATION))).thenReturn(true);
    assertTrue(_roleService.exists(Urn.createFromString(ROLE_URN_STRING), SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testRoleDoesNotExist() throws Exception {
    when(_entityClient.exists(eq(roleUrn), eq(SYSTEM_AUTHENTICATION))).thenReturn(false);
    assertFalse(_roleService.exists(Urn.createFromString(ROLE_URN_STRING), SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testAssignRoleToActorDoesNotExist() throws Exception {
    when(_entityClient.exists(eq(Urn.createFromString(ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        false);

    _roleService.assignRoleToActor(ACTOR_URN_STRING, Urn.createFromString(ROLE_URN_STRING),
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, never()).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }

  @Test
  public void testAssignRoleToActorExists() throws Exception {
    when(_entityClient.exists(eq(Urn.createFromString(ACTOR_URN_STRING)), eq(SYSTEM_AUTHENTICATION))).thenReturn(
        true);

    _roleService.assignRoleToActor(ACTOR_URN_STRING, Urn.createFromString(ROLE_URN_STRING),
        SYSTEM_AUTHENTICATION);
    verify(_entityClient, times(1)).ingestProposal(any(), eq(SYSTEM_AUTHENTICATION));
  }
}
