package io.datahubproject.metadata.context;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.datahub.authentication.Actor;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.SessionActorIdentity;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.authorization.PoliciesConfig;
import com.linkedin.policy.DataHubActorFilter;
import com.linkedin.policy.DataHubPolicyInfo;
import io.datahubproject.metadata.services.RestrictedService;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.Test;

public class AuthorizationContextTest {

  private static final DataHubPolicyInfo POLICY =
      new DataHubPolicyInfo()
          .setState(PoliciesConfig.ACTIVE_POLICY_STATE)
          .setActors(
              new DataHubActorFilter()
                  .setUsers(new UrnArray(UrnUtils.getUrn("urn:li:corpuser:testuser"))))
          .setPrivileges(new StringArray(List.of("VIEW", "EDIT")));

  @Test
  public void authorizePassesActorPoliciesByPrivilegeForResourceOnly() {
    Authorizer authorizer = mock(Authorizer.class);
    ArgumentCaptor<AuthorizationRequest> captor =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    when(authorizer.authorize(captor.capture()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.DENY, null));

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext = sessionActor(actorPoliciesByPrivilege);
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    EntitySpec resource = new EntitySpec("dataset", "urn:li:dataset:test");
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();

    authorizationContext.authorize(opContext, actorContext, "VIEW", resource);

    AuthorizationRequest request = captor.getValue();
    assertEquals(request.getActorPoliciesByPrivilege(), actorPoliciesByPrivilege);
    assertEquals(request.getPrivilege(), "VIEW");
    assertEquals(request.getResourceSpec().orElse(null), resource);
  }

  @Test
  public void authorizePassesActorPoliciesByPrivilegeWithSubResources() {
    Authorizer authorizer = mock(Authorizer.class);
    ArgumentCaptor<AuthorizationRequest> captor =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    when(authorizer.authorize(captor.capture()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.DENY, null));

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext = sessionActor(actorPoliciesByPrivilege);
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    EntitySpec resource = new EntitySpec("dataset", "urn:li:dataset:test");
    EntitySpec subResource = new EntitySpec("tag", "urn:li:tag:test");
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();

    authorizationContext.authorize(opContext, actorContext, "EDIT", resource, List.of(subResource));

    AuthorizationRequest request = captor.getValue();
    assertEquals(request.getActorPoliciesByPrivilege(), actorPoliciesByPrivilege);
    assertEquals(request.getSubResources(), List.of(subResource));
    verify(authorizer, times(1)).authorize(any());
  }

  @Test
  public void authorizeUsesOperationContextAuthorizerWhenThreeArgReturnsResult() {
    Authorizer authorizer =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) authorizer;
    when(operationContextAuthorizer.authorize(any(), any(), any()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, null));

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext = sessionActor(actorPoliciesByPrivilege);
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();

    AuthorizationResult result =
        authorizationContext.authorize(
            opContext, actorContext, "VIEW", new EntitySpec("dataset", "urn:li:dataset:test"));

    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    verify(operationContextAuthorizer, times(1)).authorize(any(), any(), any());
    verify(authorizer, never()).authorize(any(AuthorizationRequest.class));
  }

  private static final Urn ACTOR_URN = UrnUtils.getUrn("urn:li:corpuser:testuser");
  private static final Urn GROUP_URN = UrnUtils.getUrn("urn:li:corpGroup:test");
  private static final Urn DIRECT_ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Editor");
  private static final Urn INHERITED_ROLE = UrnUtils.getUrn("urn:li:dataHubRole:Admin");

  @Test
  public void getOrResolveSessionActorIdentity_cachesResult() {
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(Authorizer.EMPTY).build();
    AtomicInteger fetchCount = new AtomicInteger();
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));

    SessionActorIdentity first =
        authorizationContext.getOrResolveSessionActorIdentity(
            ACTOR_URN, () -> identityForFetch(fetchCount, identity));
    SessionActorIdentity second =
        authorizationContext.getOrResolveSessionActorIdentity(
            ACTOR_URN, () -> identityForFetch(fetchCount, identity));

    assertEquals(first, identity);
    assertEquals(second, identity);
    assertEquals(fetchCount.get(), 1);
  }

  @Test
  public void getSessionActorIdentity_returnsNullWhenNotCached() {
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(Authorizer.EMPTY).build();

    assertNull(authorizationContext.getSessionActorIdentity(ACTOR_URN));
  }

  @Test
  public void buildSessionActor_resolvesIdentityAndPoliciesViaOperationContextAuthorizer() {
    Authorizer authorizer =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) authorizer;
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(operationContextAuthorizer.resolveSessionActorIdentity(ACTOR_URN, opContext))
        .thenReturn(Optional.of(identity));
    when(operationContextAuthorizer.getActorPolicies(
            eq(ACTOR_URN),
            eq(identity),
            eq(identity.getGroups()),
            eq(identity.getDirectRoles()),
            eq(opContext)))
        .thenReturn(Set.of(POLICY));

    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    Authentication authentication = new Authentication(new Actor(ActorType.USER, "testuser"), "");

    ActorContext actorContext =
        authorizationContext.buildSessionActor(opContext, authentication, null, false);

    assertEquals(actorContext.getGroupMembership(), List.of(GROUP_URN));
    assertEquals(actorContext.getDirectRoleMembership(), Set.of(DIRECT_ROLE));
    assertEquals(actorContext.getPolicyInfoSet(), Set.of(POLICY));
    assertEquals(authorizationContext.getSessionActorIdentity(ACTOR_URN), identity);
  }

  @Test
  public void buildSessionActor_usesEmptyIdentityWhenAuthorizerReturnsEmpty() {
    Authorizer authorizer = mock(Authorizer.class);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(authorizer.resolveSessionActorIdentity(ACTOR_URN)).thenReturn(Optional.empty());
    when(authorizer.getActorPolicies(ACTOR_URN, List.of(), Set.of())).thenReturn(Set.of());

    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    Authentication authentication = new Authentication(new Actor(ActorType.USER, "testuser"), "");

    ActorContext actorContext =
        authorizationContext.buildSessionActor(opContext, authentication, null, false);

    assertTrue(actorContext.getGroupMembership().isEmpty());
    assertTrue(actorContext.getDirectRoleMembership().isEmpty());
    verify(authorizer).resolveSessionActorIdentity(ACTOR_URN);
  }

  @Test
  public void resolveSessionActorIdentity_usesPlainAuthorizerWhenNotOperationContextAuthorizer() {
    Authorizer authorizer = mock(Authorizer.class);
    SessionActorIdentity identity = SessionActorIdentity.empty(ACTOR_URN);
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(authorizer.resolveSessionActorIdentity(ACTOR_URN)).thenReturn(Optional.of(identity));

    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();

    assertEquals(
        authorizationContext.resolveSessionActorIdentity(opContext, ACTOR_URN),
        Optional.of(identity));
    verify(authorizer).resolveSessionActorIdentity(ACTOR_URN);
  }

  @Test
  public void resolveActorPolicies_usesPlainAuthorizerWhenNotOperationContextAuthorizer() {
    Authorizer authorizer = mock(Authorizer.class);
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();
    when(authorizer.getActorPolicies(ACTOR_URN, identity.getGroups(), identity.getDirectRoles()))
        .thenReturn(Set.of(POLICY));

    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();

    assertEquals(
        authorizationContext.resolveActorPolicies(opContext, ACTOR_URN, identity), Set.of(POLICY));
  }

  @Test
  public void resolveSessionActorRoles_resolvesInheritedRolesFromServicesRegistry() {
    ActorGroupMembershipService membershipService = mock(ActorGroupMembershipService.class);
    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));
    OperationContext opContext =
        TestOperationContexts.systemContext(
            null,
            null,
            () ->
                ServicesRegistryContext.builder()
                    .restrictedService(mock(RestrictedService.class))
                    .actorGroupMembershipService(membershipService)
                    .build(),
            null,
            null,
            null,
            null,
            () -> ValidationContext.builder().alternateValidation(true).build());
    when(membershipService.fetchRolesViaGroups(opContext, List.of(GROUP_URN)))
        .thenReturn(Set.of(INHERITED_ROLE));

    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(Authorizer.EMPTY).build();
    authorizationContext.getOrResolveSessionActorIdentity(ACTOR_URN, () -> identity);
    ActorContext actorContext =
        ActorContext.builder()
            .authentication(new Authentication(new Actor(ActorType.USER, "testuser"), ""))
            .groupMembership(identity.getGroups())
            .directRoleMembership(identity.getDirectRoles())
            .build();

    assertEquals(
        authorizationContext.resolveSessionActorRoles(opContext, actorContext),
        Set.of(DIRECT_ROLE, INHERITED_ROLE));
    verify(membershipService).fetchRolesViaGroups(opContext, List.of(GROUP_URN));
  }

  @Test
  public void resolveSessionActorRoles_fallsBackToDirectRolesWhenIdentityNotCached() {
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(Authorizer.EMPTY).build();
    ActorContext actorContext =
        ActorContext.builder()
            .authentication(new Authentication(new Actor(ActorType.USER, "testuser"), ""))
            .directRoleMembership(Set.of(DIRECT_ROLE))
            .build();

    assertEquals(
        authorizationContext.resolveSessionActorRoles(
            TestOperationContexts.systemContextNoValidate(), actorContext),
        Set.of(DIRECT_ROLE));
  }

  @Test
  public void authorizeIncludesSessionActorIdentityWhenCached() {
    Authorizer authorizer = mock(Authorizer.class);
    ArgumentCaptor<AuthorizationRequest> captor =
        ArgumentCaptor.forClass(AuthorizationRequest.class);
    when(authorizer.authorize(captor.capture()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.DENY, null));

    SessionActorIdentity identity =
        new SessionActorIdentity(ACTOR_URN, List.of(GROUP_URN), Set.of(DIRECT_ROLE));
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    authorizationContext.getOrResolveSessionActorIdentity(ACTOR_URN, () -> identity);

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext =
        ActorContext.builder()
            .authentication(new Authentication(new Actor(ActorType.USER, "testuser"), ""))
            .policyInfoSet(Set.of(POLICY))
            .actorPoliciesByPrivilege(actorPoliciesByPrivilege)
            .groupMembership(identity.getGroups())
            .directRoleMembership(identity.getDirectRoles())
            .build();

    authorizationContext.authorize(
        TestOperationContexts.systemContextNoValidate(),
        actorContext,
        "VIEW",
        new EntitySpec("dataset", "urn:li:dataset:test"));

    assertEquals(captor.getValue().getSessionActorIdentity(), identity);
    assertEquals(captor.getValue().getActorGroupMembership(), List.of(GROUP_URN));
    assertEquals(captor.getValue().getActorDirectRoles(), Set.of(DIRECT_ROLE));
  }

  @Test
  public void authorizeFallsBackToOneArgWhenOperationContextAuthorizerReturnsNull() {
    Authorizer authorizer =
        mock(Authorizer.class, withSettings().extraInterfaces(OperationContextAuthorizer.class));
    OperationContextAuthorizer operationContextAuthorizer = (OperationContextAuthorizer) authorizer;
    when(operationContextAuthorizer.authorize(any(), any(), any())).thenReturn(null);
    when(authorizer.authorize(any()))
        .thenAnswer(
            invocation ->
                new AuthorizationResult(
                    invocation.getArgument(0), AuthorizationResult.Type.ALLOW, null));

    Map<String, List<RecordTemplate>> actorPoliciesByPrivilege =
        ActorContext.indexPoliciesByPrivilege(Set.of(POLICY));
    ActorContext actorContext = sessionActor(actorPoliciesByPrivilege);
    AuthorizationContext authorizationContext =
        AuthorizationContext.builder().authorizer(authorizer).build();
    OperationContext opContext = TestOperationContexts.systemContextNoValidate();

    AuthorizationResult result =
        authorizationContext.authorize(
            opContext, actorContext, "VIEW", new EntitySpec("dataset", "urn:li:dataset:test"));

    assertEquals(result.getType(), AuthorizationResult.Type.ALLOW);
    verify(operationContextAuthorizer, times(1)).authorize(any(), any(), any());
    verify(authorizer, times(1)).authorize(any(AuthorizationRequest.class));
  }

  private static SessionActorIdentity identityForFetch(
      AtomicInteger fetchCount, SessionActorIdentity identity) {
    fetchCount.incrementAndGet();
    return identity;
  }

  private static ActorContext sessionActor(
      Map<String, List<RecordTemplate>> actorPoliciesByPrivilege) {
    Authentication authentication = new Authentication(new Actor(ActorType.USER, "testuser"), "");
    return ActorContext.builder()
        .authentication(authentication)
        .policyInfoSet(Set.of(POLICY))
        .actorPoliciesByPrivilege(actorPoliciesByPrivilege)
        .groupMembership(Collections.emptyList())
        .build();
  }
}
