package io.datahubproject.metadata.context;

import com.datahub.authentication.Authentication;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.ResolvedEntitySpec;
import com.datahub.authorization.SessionActorIdentity;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.plugins.auth.authorization.ResourceSpecCachingAuthorizer;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.policy.DataHubPolicyInfo;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Getter;

@Getter
@Builder
public class AuthorizationContext implements ContextInterface {

  public static final AuthorizationContext EMPTY =
      AuthorizationContext.builder().authorizer(Authorizer.EMPTY).build();

  @Nonnull private final Authorizer authorizer;

  @Builder.Default
  private final ConcurrentHashMap<AuthorizationRequest, AuthorizationResult>
      sessionAuthorizationCache = new ConcurrentHashMap<>();

  /**
   * Request-scoped cache of resolved resource specs. Resolving a resource's attributes
   * (domain/container/owner) is actor-independent and can be expensive (recursive hierarchy walks),
   * so each resource is resolved once even when a page authorizes it for many privileges.
   */
  @Builder.Default
  private final ConcurrentHashMap<EntitySpec, ResolvedEntitySpec> sessionResourceSpecCache =
      new ConcurrentHashMap<>();

  /**
   * Request-scoped cache of session actor identity (groups + direct roles). Populated once per
   * session actor per request.
   */
  @Builder.Default
  private final ConcurrentHashMap<Urn, SessionActorIdentity> sessionActorIdentityByUrn =
      new ConcurrentHashMap<>();

  /**
   * Returns cached session actor identity, resolving via {@code fetcher} on first access for this
   * request.
   */
  @Nonnull
  public SessionActorIdentity getOrResolveSessionActorIdentity(
      @Nonnull final Urn sessionActorUrn, @Nonnull final Supplier<SessionActorIdentity> fetcher) {
    return sessionActorIdentityByUrn.computeIfAbsent(sessionActorUrn, urn -> fetcher.get());
  }

  @Nullable
  public SessionActorIdentity getSessionActorIdentity(@Nonnull final Urn sessionActorUrn) {
    return sessionActorIdentityByUrn.get(sessionActorUrn);
  }

  /**
   * Builds a fully populated session {@link ActorContext} (identity, policies, membership) using
   * the session {@link OperationContext} for authorizer storage reads.
   */
  @Nonnull
  public ActorContext buildSessionActor(
      @Nonnull final OperationContext opContext,
      @Nonnull final Authentication sessionAuthentication,
      @Nullable final Authentication systemAuthentication,
      boolean enforceExistenceEnabled) {
    final Urn actorUrn = UrnUtils.getUrn(sessionAuthentication.getActor().toUrnStr());
    final SessionActorIdentity sessionActorIdentity =
        getOrResolveSessionActorIdentity(
            actorUrn,
            () ->
                resolveSessionActorIdentity(opContext, actorUrn)
                    .orElse(SessionActorIdentity.empty(actorUrn)));
    final Set<DataHubPolicyInfo> policyInfoSet =
        resolveActorPolicies(opContext, actorUrn, sessionActorIdentity);
    return ActorContext.builder()
        .authentication(sessionAuthentication)
        .systemAuth(ActorContext.isSystemSession(sessionAuthentication, systemAuthentication))
        .policyInfoSet(policyInfoSet)
        .actorPoliciesByPrivilege(ActorContext.indexPoliciesByPrivilege(policyInfoSet))
        .groupMembership(sessionActorIdentity.getGroups())
        .directRoleMembership(sessionActorIdentity.getDirectRoles())
        .enforceExistenceEnabled(enforceExistenceEnabled)
        .build();
  }

  /**
   * Session actor's roles (direct + inherited via groups). Group role inheritance is resolved at
   * most once per request via the cached {@link SessionActorIdentity}.
   */
  @Nonnull
  public Set<Urn> resolveSessionActorRoles(
      @Nonnull final OperationContext opContext, @Nonnull final ActorContext sessionActor) {
    final SessionActorIdentity identity = getSessionActorIdentity(sessionActor.getActorUrn());
    if (identity != null) {
      return identity.resolveAllRoles(
          groups ->
              Optional.ofNullable(opContext.getServicesRegistryContext())
                  .map(ctx -> ctx.fetchRolesViaGroups(opContext, groups))
                  .orElse(Collections.emptySet()));
    }
    return Set.copyOf(sessionActor.getDirectRoleMembership());
  }

  @Nonnull
  Optional<SessionActorIdentity> resolveSessionActorIdentity(
      @Nonnull final OperationContext opContext, @Nonnull final Urn actorUrn) {
    if (authorizer instanceof OperationContextAuthorizer) {
      return ((OperationContextAuthorizer) authorizer)
          .resolveSessionActorIdentity(actorUrn, opContext);
    }
    return authorizer.resolveSessionActorIdentity(actorUrn);
  }

  @Nonnull
  Set<DataHubPolicyInfo> resolveActorPolicies(
      @Nonnull final OperationContext opContext,
      @Nonnull final Urn actorUrn,
      @Nonnull final SessionActorIdentity sessionActorIdentity) {
    if (authorizer instanceof OperationContextAuthorizer) {
      return ((OperationContextAuthorizer) authorizer)
          .getActorPolicies(
              actorUrn,
              sessionActorIdentity,
              sessionActorIdentity.getGroups(),
              sessionActorIdentity.getDirectRoles(),
              opContext);
    }
    return authorizer.getActorPolicies(
        actorUrn, sessionActorIdentity.getGroups(), sessionActorIdentity.getDirectRoles());
  }

  /**
   * Run authorization through the actor's session cache
   *
   * @param actorContext the actor context
   * @param privilege privilege
   * @param resourceSpec resource to access
   * @return authorization result
   */
  public AuthorizationResult authorize(
      @Nonnull OperationContext opContext,
      @Nonnull ActorContext actorContext,
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec) {
    final AuthorizationRequest request =
        buildAuthorizationRequest(actorContext, privilege, resourceSpec, Collections.emptyList());
    // Graphql CompletableFutures causes a recursive exception, we avoid computeIfAbsent and do work
    // outside a blocking function
    AuthorizationResult result = sessionAuthorizationCache.get(request);
    if (result == null) {
      result = runAuthorize(opContext, request);
      sessionAuthorizationCache.putIfAbsent(request, result);
    }
    return result;
  }

  public AuthorizationResult authorize(
      @Nonnull OperationContext opContext,
      @Nonnull ActorContext actorContext,
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {
    final AuthorizationRequest request =
        buildAuthorizationRequest(actorContext, privilege, resourceSpec, subResources);
    // Graphql CompletableFutures causes a recursive exception, we avoid computeIfAbsent and do work
    // outside a blocking function
    AuthorizationResult result = sessionAuthorizationCache.get(request);
    if (result == null) {
      result = runAuthorize(opContext, request);
      sessionAuthorizationCache.putIfAbsent(request, result);
    }
    return result;
  }

  @Nonnull
  private AuthorizationRequest buildAuthorizationRequest(
      @Nonnull final ActorContext actorContext,
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {
    return new AuthorizationRequest(
        actorContext.getActorUrn().toString(),
        privilege,
        Optional.ofNullable(resourceSpec),
        subResources,
        actorContext.getActorPoliciesByPrivilege(),
        actorContext.getGroupMembership(),
        actorContext.getDirectRoleMembership(),
        getSessionActorIdentity(actorContext.getActorUrn()));
  }

  /**
   * Run authorization, sharing this session's resolved-spec cache with authorizers that support it.
   * A capability-advertising authorizer that returns no result (e.g. a partially stubbed test
   * double) falls back to the standard {@link Authorizer#authorize(AuthorizationRequest)} path,
   * which is behaviourally identical aside from the per-request spec cache.
   */
  private AuthorizationResult runAuthorize(
      @Nonnull final OperationContext opContext, @Nonnull final AuthorizationRequest request) {
    if (authorizer instanceof OperationContextAuthorizer) {
      final AuthorizationResult result =
          ((OperationContextAuthorizer) authorizer)
              .authorize(request, sessionResourceSpecCache, opContext);
      if (result != null) {
        return result;
      }
    }
    if (authorizer instanceof ResourceSpecCachingAuthorizer) {
      final AuthorizationResult result =
          ((ResourceSpecCachingAuthorizer) authorizer).authorize(request, sessionResourceSpecCache);
      if (result != null) {
        return result;
      }
    }
    return authorizer.authorize(request);
  }

  /**
   * No need to consider the authorizer in the cache context since it is ultimately determined by
   * the underlying search context and actor context
   *
   * @return
   */
  @Override
  public Optional<Integer> getCacheKeyComponent() {
    return Optional.empty();
  }
}
