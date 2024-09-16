package io.datahubproject.metadata.context;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.plugins.auth.authorization.Authorizer;
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
            actorContext.getActorUrn().toString(), privilege, Optional.ofNullable(resourceSpec));
    // Graphql CompletableFutures causes a recursive exception, we avoid computeIfAbsent and do work
    // outside a blocking function
    AuthorizationResult result = sessionAuthorizationCache.get(request);
    if (result == null) {
      result = authorizer.authorize(request);
      sessionAuthorizationCache.putIfAbsent(request, result);
    }
    return result;
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
