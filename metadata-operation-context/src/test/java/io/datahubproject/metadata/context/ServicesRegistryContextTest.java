package io.datahubproject.metadata.context;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.SessionActorIdentity;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class ServicesRegistryContextTest {

  private static final Urn USER_URN = UrnUtils.getUrn("urn:li:corpuser:testuser");
  private static final Urn GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");
  private static final Urn ROLE_URN = UrnUtils.getUrn("urn:li:dataHubRole:Admin");

  @Test
  public void fetchUserIdentity_returnsEmptyWhenMembershipServiceMissing() {
    ServicesRegistryContext context =
        ServicesRegistryContext.builder().restrictedService(mock(RestrictedService.class)).build();
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();

    SessionActorIdentity identity = context.fetchUserIdentity(opContext, USER_URN);

    assertEquals(identity.getActorUrn(), USER_URN);
    assertTrue(identity.getGroups().isEmpty());
    assertTrue(identity.getDirectRoles().isEmpty());
  }

  @Test
  public void fetchUserIdentity_delegatesToMembershipService() {
    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity =
        new SessionActorIdentity(USER_URN, List.of(GROUP_URN), Set.of());
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(membershipService.fetchUserIdentity(opContext, USER_URN)).thenReturn(identity);

    ServicesRegistryContext context =
        ServicesRegistryContext.builder()
            .restrictedService(mock(RestrictedService.class))
            .actorGroupMembershipService(membershipService)
            .build();

    assertEquals(context.fetchUserIdentity(opContext, USER_URN), identity);
    verify(membershipService).fetchUserIdentity(opContext, USER_URN);
  }

  @Test
  public void getGroupsForUser_returnsEmptyWhenMembershipServiceMissing() {
    ServicesRegistryContext context =
        ServicesRegistryContext.builder().restrictedService(mock(RestrictedService.class)).build();

    assertEquals(
        context.getGroupsForUser(TestOperationContexts.systemContextNoValidate(), USER_URN),
        Collections.emptyList());
  }

  @Test
  public void getGroupsForUser_delegatesToMembershipService() {
    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(membershipService.getGroupsForUser(opContext, USER_URN)).thenReturn(List.of(GROUP_URN));

    ServicesRegistryContext context =
        ServicesRegistryContext.builder()
            .restrictedService(mock(RestrictedService.class))
            .actorGroupMembershipService(membershipService)
            .build();

    assertEquals(context.getGroupsForUser(opContext, USER_URN), List.of(GROUP_URN));
  }

  @Test
  public void fetchRolesViaGroups_returnsEmptyWhenMembershipServiceMissing() {
    ServicesRegistryContext context =
        ServicesRegistryContext.builder().restrictedService(mock(RestrictedService.class)).build();

    assertEquals(
        context.fetchRolesViaGroups(
            TestOperationContexts.systemContextNoValidate(), List.of(GROUP_URN)),
        Collections.emptySet());
  }

  @Test
  public void fetchRolesViaGroups_delegatesToMembershipService() {
    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(membershipService.fetchRolesViaGroups(opContext, List.of(GROUP_URN)))
        .thenReturn(Set.of(ROLE_URN));

    ServicesRegistryContext context =
        ServicesRegistryContext.builder()
            .restrictedService(mock(RestrictedService.class))
            .actorGroupMembershipService(membershipService)
            .build();

    assertEquals(context.fetchRolesViaGroups(opContext, List.of(GROUP_URN)), Set.of(ROLE_URN));
    verify(membershipService).fetchRolesViaGroups(eq(opContext), eq(List.of(GROUP_URN)));
  }
}
