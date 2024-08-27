package com.datahub.authorization;

import static org.mockito.Mockito.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.role.RoleService;
import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.client.EntityClient;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
  private OperationContext opContext =
      TestOperationContexts.userContextNoSearchAuthorization(SYSTEM_AUTHENTICATION);

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    _entityClient = mock(EntityClient.class);
    when(_entityClient.exists(any(), eq(roleUrn))).thenReturn(true);

    _roleService = new RoleService(_entityClient);
  }

  @Test
  public void testBatchAssignRoleNoActorExists() throws Exception {
    when(_entityClient.exists(
            any(OperationContext.class), eq(Urn.createFromString(FIRST_ACTOR_URN_STRING))))
        .thenReturn(false);

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING), roleUrn);
    verify(_entityClient, never()).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testBatchAssignRoleSomeActorExists() throws Exception {
    when(_entityClient.exists(
            any(OperationContext.class), eq(Urn.createFromString(FIRST_ACTOR_URN_STRING))))
        .thenReturn(true);

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING), roleUrn);
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testBatchAssignRoleAllActorsExist() throws Exception {
    when(_entityClient.exists(
            any(OperationContext.class), eq(Urn.createFromString(FIRST_ACTOR_URN_STRING))))
        .thenReturn(true);
    when(_entityClient.exists(
            any(OperationContext.class), eq(Urn.createFromString(SECOND_ACTOR_URN_STRING))))
        .thenReturn(true);

    _roleService.batchAssignRoleToActors(
        opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING, SECOND_ACTOR_URN_STRING), roleUrn);
    verify(_entityClient, times(2)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }

  @Test
  public void testAssignNullRoleToActorAllActorsExist() throws Exception {
    when(_entityClient.exists(
            any(OperationContext.class), eq(Urn.createFromString(FIRST_ACTOR_URN_STRING))))
        .thenReturn(true);

    _roleService.batchAssignRoleToActors(opContext, ImmutableList.of(FIRST_ACTOR_URN_STRING), null);
    verify(_entityClient, times(1)).ingestProposal(any(OperationContext.class), any(), eq(false));
  }
}
