package com.linkedin.datahub.graphql.resolvers.role;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AcceptRoleInput;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class AcceptRoleResolverTest {
  private static final String INVITE_TOKEN_URN_STRING = "urn:li:inviteToken:admin-invite-token";
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:user";
  private static final String INVITE_TOKEN_STRING = "inviteToken";
  private Urn roleUrn;
  private Urn inviteTokenUrn;
  private RoleService _roleService;
  private InviteTokenService _inviteTokenService;
  private AcceptRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    inviteTokenUrn = Urn.createFromString(INVITE_TOKEN_URN_STRING);
    _roleService = mock(RoleService.class);
    _inviteTokenService = mock(InviteTokenService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new AcceptRoleResolver(_roleService, _inviteTokenService);
  }

  @Test
  public void testNotAuthorizedFails() {
    QueryContext mockContext = getMockDenyContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testInvalidInviteToken() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.isInviteTokenValid(eq(inviteTokenUrn), eq(_authentication))).thenReturn(false);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertThrows(() -> _resolver.get(_dataFetchingEnvironment).join());
  }

  @Test
  public void testNoRoleUrn() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(eq(inviteTokenUrn), eq(_authentication))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(eq(inviteTokenUrn), eq(_authentication))).thenReturn(null);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }

  @Test
  public void testAssignRolePasses() throws Exception {
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(eq(inviteTokenUrn), eq(_authentication))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(eq(inviteTokenUrn), eq(_authentication))).thenReturn(roleUrn);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }
}
