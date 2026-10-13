package com.linkedin.datahub.graphql.resolvers.role;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.authentication.Actor;
import com.datahub.authentication.Authentication;
import com.datahub.authentication.invite.InviteTokenService;
import com.datahub.authorization.role.RoleService;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AcceptRoleInput;
import com.linkedin.entity.client.EntityClientCache;
import com.linkedin.entity.client.SystemEntityClient;
import com.linkedin.identity.RoleMembership;
import com.linkedin.metadata.entity.EntityService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Set;
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
  private SystemEntityClient _systemEntityClient;
  private OperationContext _systemOperationContext;
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
    _systemEntityClient = mock(SystemEntityClient.class);
    _systemOperationContext = mock(OperationContext.class);
    _dataFetchingEnvironment = mock(DataFetchingEnvironment.class);
    _authentication = mock(Authentication.class);

    _resolver =
        new AcceptRoleResolver(
            _roleService,
            _inviteTokenService,
            _entityService,
            _systemEntityClient,
            _systemOperationContext);
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
    // The accepting user holds no privileges, so the grant must be issued on the system context -
    // a user-context write is rejected by PrivilegeGrantAuthorizationValidator.
    verify(_roleService, times(1))
        .batchAssignRoleToActors(
            eq(_systemOperationContext), eq(List.of(ACTOR_URN_STRING)), eq(roleUrn));
  }

  @Test
  public void testCacheInvalidationIsCalledAfterRoleAssignment() throws Exception {
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

    // Mock EntityClientCache
    EntityClientCache mockEntityClientCache = mock(EntityClientCache.class);
    when(_systemEntityClient.getEntityClientCache()).thenReturn(mockEntityClientCache);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Execute
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // Verify cache invalidation was called with correct URN and aspect
    verify(_systemEntityClient, atLeastOnce()).getEntityClientCache();
    verify(mockEntityClientCache, times(1))
        .invalidate(eq(Urn.createFromString(ACTOR_URN_STRING)), eq(Set.of("roleMembership")));
  }

  @Test
  public void testCacheInvalidationSkippedWhenCacheIsNull() throws Exception {
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

    // Mock EntityClientCache as null (cache not available)
    when(_systemEntityClient.getEntityClientCache()).thenReturn(null);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Execute - should succeed even without cache
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // Verify role assignment still succeeded
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }

  @Test
  public void testHigherRoleNotDowngradedByLowerRoleInviteLink() throws Exception {
    // Admin clicks a Reader invite link — their role must NOT be downgraded
    Urn readerRoleUrn = Urn.createFromString("urn:li:dataHubRole:Reader");
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn)))
        .thenReturn(readerRoleUrn);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    // User currently has Admin role (higher than Reader)
    RoleMembership currentMembership = new RoleMembership();
    currentMembership.setRoles(new UrnArray(roleUrn)); // roleUrn = urn:li:dataHubRole:Admin
    when(_entityService.getLatestAspect(any(), any(), eq("roleMembership")))
        .thenReturn(currentMembership);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, never()).batchAssignRoleToActors(any(), any(), any());
  }

  @Test
  public void testLowerRoleIsUpgradedByHigherRoleInviteLink() throws Exception {
    // Reader clicks an Admin invite link — they should be upgraded
    Urn readerRoleUrn = Urn.createFromString("urn:li:dataHubRole:Reader");
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn)))
        .thenReturn(roleUrn); // roleUrn = urn:li:dataHubRole:Admin
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    // User currently has Reader role (lower than Admin)
    RoleMembership currentMembership = new RoleMembership();
    currentMembership.setRoles(new UrnArray(readerRoleUrn));
    when(_entityService.getLatestAspect(any(), any(), eq("roleMembership")))
        .thenReturn(currentMembership);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }

  @Test
  public void testUserWithNoRoleGetsRoleAssignedFromInviteLink() throws Exception {
    // User with no existing role clicks an invite link — they should get the role
    QueryContext mockContext = getMockAllowContext();
    when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);
    when(mockContext.getAuthentication()).thenReturn(_authentication);
    when(_inviteTokenService.getInviteTokenUrn(eq(INVITE_TOKEN_STRING))).thenReturn(inviteTokenUrn);
    when(_inviteTokenService.isInviteTokenValid(any(), eq(inviteTokenUrn))).thenReturn(true);
    when(_inviteTokenService.getInviteTokenRole(any(), eq(inviteTokenUrn))).thenReturn(roleUrn);
    Actor actor = mock(Actor.class);
    when(_authentication.getActor()).thenReturn(actor);
    when(actor.toUrnStr()).thenReturn(ACTOR_URN_STRING);

    // User has no RoleMembership aspect
    when(_entityService.getLatestAspect(any(), any(), eq("roleMembership"))).thenReturn(null);

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    assertTrue(_resolver.get(_dataFetchingEnvironment).join());
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }

  @Test
  public void testRoleAssignmentSucceedsEvenIfCacheInvalidationFails() throws Exception {
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

    // Mock EntityClientCache to throw exception
    EntityClientCache mockEntityClientCache = mock(EntityClientCache.class);
    when(_systemEntityClient.getEntityClientCache()).thenReturn(mockEntityClientCache);
    doThrow(new RuntimeException("Cache invalidation failed"))
        .when(mockEntityClientCache)
        .invalidate(any(), any());

    AcceptRoleInput input = new AcceptRoleInput();
    input.setInviteToken(INVITE_TOKEN_STRING);
    when(_dataFetchingEnvironment.getArgument(eq("input"))).thenReturn(input);

    // Execute - should succeed despite cache invalidation failure
    assertTrue(_resolver.get(_dataFetchingEnvironment).join());

    // Verify role assignment still succeeded
    verify(_roleService, times(1)).batchAssignRoleToActors(any(), any(), any());
  }
}
