package io.datahubproject.metadata.context;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.ResolvedEntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.plugins.auth.authorization.ResourceSpecCachingAuthorizer;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
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
   * Run authorization through the actor's session cache
   *
   * @param actorContext the actor context
   * @param privilege privilege
   * @param resourceSpec resource to access
   * @return authorization result
   */
  public AuthorizationResult authorize(
      @Nonnull ActorContext actorContext,
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec) {
    final AuthorizationRequest request =
        new AuthorizationRequest(
            actorContext.getActorUrn().toString(),
            privilege,
            Optional.ofNullable(resourceSpec),
            Collections.emptyList());
    // Graphql CompletableFutures causes a recursive exception, we avoid computeIfAbsent and do work
    // outside a blocking function
    AuthorizationResult result = sessionAuthorizationCache.get(request);
    if (result == null) {
      result = runAuthorize(request);
      sessionAuthorizationCache.putIfAbsent(request, result);
    }
    return result;
  }

  public AuthorizationResult authorize(
      @Nonnull ActorContext actorContext,
      @Nonnull final String privilege,
      @Nullable final EntitySpec resourceSpec,
      @Nonnull final Collection<EntitySpec> subResources) {
    final AuthorizationRequest request =
        new AuthorizationRequest(
            actorContext.getActorUrn().toString(),
            privilege,
            Optional.ofNullable(resourceSpec),
            subResources);
    // Graphql CompletableFutures causes a recursive exception, we avoid computeIfAbsent and do work
    // outside a blocking function
    AuthorizationResult result = sessionAuthorizationCache.get(request);
    if (result == null) {
      result = runAuthorize(request);
      sessionAuthorizationCache.putIfAbsent(request, result);
    }
    return result;
  }

  /**
   * Run authorization, sharing this session's resolved-spec cache with authorizers that support it.
   * A capability-advertising authorizer that returns no result (e.g. a partially stubbed test
   * double) falls back to the standard {@link Authorizer#authorize(AuthorizationRequest)} path,
   * which is behaviourally identical aside from the per-request spec cache.
   */
  private AuthorizationResult runAuthorize(@Nonnull final AuthorizationRequest request) {
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
