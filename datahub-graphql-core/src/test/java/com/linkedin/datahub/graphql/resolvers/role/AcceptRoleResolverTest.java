package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AcceptRoleInput;
import com.linkedin.identity.CorpUserInvitationStatus;
import com.linkedin.identity.InvitationStatus;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.mxe.MetadataChangeProposal;
import graphql.schema.DataFetchingEnvironment;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class AcceptRoleResolverTest {
  private static final String INVITE_TOKEN_URN_STRING = "urn:li:inviteToken:admin-invite-token";
  private static final String ROLE_URN_STRING = "urn:li:dataHubRole:Admin";
  private static final String ACTOR_URN_STRING = "urn:li:corpuser:user";
  private static final String INVITE_TOKEN_STRING = "inviteToken";
  private Urn roleUrn;
  private Urn inviteTokenUrn;
  private RoleService _roleService;
  private InviteTokenService _inviteTokenService;
  private EntityService<?> _entityService;
  private AcceptRoleResolver _resolver;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private Authentication _authentication;

  @BeforeMethod
  public void setupTest() throws Exception {
    roleUrn = Urn.createFromString(ROLE_URN_STRING);
    inviteTokenUrn = Urn.createFromString(INVITE_TOKEN_URN_STRING);
    _roleService = mock(RoleService.class);
    _inviteTokenService = mock(InviteTokenService.class);
    _entityService = mock(EntityService.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver = new AcceptRoleResolver(_roleService, _inviteTokenService, _entityService);
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
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(false);

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
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn))).thenReturn(null);
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
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn))).thenReturn(roleUrn);
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
  public void testInvitationStatusUpdatedToAccepted() throws Exception {
    // Setup
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn))).thenReturn(roleUrn);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    // Mock existing invitation status (SENT)
    CorpUserInvitationStatus existingStatus = new CorpUserInvitationStatus();
    existingStatus.setStatus(InvitationStatus.SENT);
    existingStatus.setInvitationToken(INVITE_TOKEN_STRING);
    existingStatus.setRole(roleUrn);
    when(_entityService.getLatestAspect(any(), any(), eq("corpUserInvitationStatus")))
        .thenReturn(existingStatus);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Execute
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // Verify role assignment
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());

    // Verify invitation status was updated
    verify(_entityService, times(1))
        .ingestProposal(any(), any(MetadataChangeProposal.class), any(), eq(false));
  }

  @Test
  public void testInvitationStatusNotUpdatedWhenNotSent() throws Exception {
    // Setup - existing status is already ACCEPTED
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn))).thenReturn(roleUrn);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    // Mock existing invitation status (already ACCEPTED)
    CorpUserInvitationStatus existingStatus = new CorpUserInvitationStatus();
    existingStatus.setStatus(InvitationStatus.ACCEPTED);
    existingStatus.setInvitationToken(INVITE_TOKEN_STRING);
    existingStatus.setRole(roleUrn);
    when(_entityService.getLatestAspect(any(), any(), eq("corpUserInvitationStatus")))
        .thenReturn(existingStatus);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Execute
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // Verify role assignment still happens
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());

    // Verify invitation status was NOT updated (since it wasn't SENT)
    verify(_entityService, never())
        .ingestProposal(any(), any(MetadataChangeProposal.class), any(), eq(false));
  }

  @Test
  public void testRoleAssignmentSucceedsEvenIfInvitationStatusUpdateFails() throws Exception {
    // Setup
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn))).thenReturn(roleUrn);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    // Mock invitation status update to throw exception
    when(_entityService.getLatestAspect(any(), any(), eq("corpUserInvitationStatus")))
        .thenThrow(new RuntimeException("Status update failed"));

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Execute - should succeed despite status update failure
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // Verify role assignment still succeeded
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }
}
