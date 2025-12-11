package io.datahubproject.metadata.context;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.BatchAuthorizationRequest;
import com.datahub.authorization.BatchAuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.LazyAuthorizationResultMap;
import com.datahub.plugins.auth.authorization.Authorizer;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
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
   * Run authorization through the actor's session cache
   *
   * @param actorContext the actor context
   * @param privileges privileges
   * @param resourceSpec resource to access
   * @param subResources
   * @return batch authorization result
   */
  public BatchAuthorizationResult authorize(
      @Nonnull ActorContext actorContext,
      @Nonnull Set<String> privileges,
      @Nullable EntitySpec resourceSpec,
      Collection<EntitySpec> subResources) {
    BatchAuthorizationRequest request =
        new BatchAuthorizationRequest(
            actorContext.getActorUrn().toString(),
            privileges,
            Optional.ofNullable(resourceSpec),
            subResources);

    BatchAuthorizationResult result = authorizer.authorizeBatch(request);

    return new BatchAuthorizationResult(request, toCacheFirstResult(request, result));
  }

  private LazyAuthorizationResultMap toCacheFirstResult(
      BatchAuthorizationRequest request, BatchAuthorizationResult result) {
    return new LazyAuthorizationResultMap(
        privilege -> {
          var authorizationRequest =
              new AuthorizationRequest(
                  request.getActorUrn(),
                  privilege,
                  request.getResourceSpec(),
                  request.getSubResources());
          // Graphql CompletableFutures causes a recursive exception, we avoid computeIfAbsent
          // and do
          // work outside a blocking function
          AuthorizationResult authorizationResult =
              sessionAuthorizationCache.get(authorizationRequest);
          if (authorizationResult != null) {
            return authorizationResult;
          }
          AuthorizationResult nonCachedResult = result.getResults().get(privilege);
          sessionAuthorizationCache.putIfAbsent(authorizationRequest, nonCachedResult);
          return nonCachedResult;
        });
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
