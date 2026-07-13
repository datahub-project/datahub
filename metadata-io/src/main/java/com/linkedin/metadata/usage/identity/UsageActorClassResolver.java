package com.linkedin.metadata.usage.identity;

import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider.CorpUserFlags;
import com.linkedin.metadata.usage.instrumentation.UsageRequestState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageActorClass;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Classifies actor buckets at record time for usage aggregation. */
public class UsageActorClassResolver {

  private static final Set<String> KNOWN_SYSTEM_ACTOR_URNS =
      Set.of(Constants.SYSTEM_ACTOR, Constants.UNKNOWN_ACTOR, Constants.ANONYMOUS_ACTOR);

  private final CorpUserFlagsProvider corpUserFlagsProvider;

  public UsageActorClassResolver(@Nonnull CorpUserFlagsProvider corpUserFlagsProvider) {
    this.corpUserFlagsProvider = corpUserFlagsProvider;
  }

  @Nonnull
  public UsageActorClass resolve(
      @Nonnull OperationContext opContext,
      @Nonnull RequestContext requestContext,
      @Nonnull Authentication authentication) {
    ResolveCacheKey cacheKey = ResolveCacheKey.of(requestContext, authentication);
    Map<Object, UsageActorClass> actorClassCache = UsageRequestState.current().actorClassCache();
    UsageActorClass cached = actorClassCache.get(cacheKey);
    if (cached != null) {
      return cached;
    }

    UsageActorClass resolved = resolveUncached(opContext, requestContext, authentication);
    actorClassCache.put(cacheKey, resolved);
    return resolved;
  }

  @Nonnull
  private UsageActorClass resolveUncached(
      @Nonnull OperationContext opContext,
      @Nonnull RequestContext requestContext,
      @Nonnull Authentication authentication) {
    String usageIdentity = requestContext.getUsageIdentity();
    String actorUrn = requestContext.getActorUrn();

    if (usageIdentity != null && KNOWN_SYSTEM_ACTOR_URNS.contains(usageIdentity)) {
      return UsageActorClass.SYSTEM;
    }
    if (isSystemAuthentication(authentication)) {
      return UsageActorClass.SYSTEM;
    }

    String corpUserUrn = resolveCorpUserUrn(usageIdentity, actorUrn);
    if (corpUserUrn != null) {
      if (authentication.getActor() == null) {
        return UsageActorClass.REGULAR;
      }
      CorpUserFlags flags = requestCorpUserFlags(opContext, corpUserUrn);
      if (flags.system()) {
        return UsageActorClass.SYSTEM;
      }
      if (flags.supportUser()) {
        return UsageActorClass.SUPPORT;
      }
    }
    return UsageActorClass.REGULAR;
  }

  @Nullable
  private static String resolveCorpUserUrn(
      @Nullable String usageIdentity, @Nonnull String actorUrn) {
    if (usageIdentity != null && usageIdentity.startsWith(Constants.URN_LI_PREFIX + "corpuser:")) {
      return usageIdentity;
    }
    if (actorUrn.startsWith(Constants.URN_LI_PREFIX + "corpuser:")) {
      return actorUrn;
    }
    return null;
  }

  @Nonnull
  private CorpUserFlags requestCorpUserFlags(
      @Nonnull OperationContext opContext, @Nonnull String corpUserUrn) {
    Map<String, CorpUserFlags> cache = UsageRequestState.current().corpUserFlags();
    return cache.computeIfAbsent(
        corpUserUrn, urn -> corpUserFlagsProvider.resolveWithContext(opContext, urn));
  }

  private boolean isSystemAuthentication(@Nonnull Authentication authentication) {
    if (authentication.getActor() == null) {
      return false;
    }
    if (authentication.getActor().getType() != ActorType.USER) {
      return true;
    }
    String actorUrn = authentication.getActor().toUrnStr();
    return Constants.SYSTEM_ACTOR.equals(actorUrn);
  }

  /**
   * Inputs that affect {@link #resolveUncached}; stable for the lifetime of a tagged HTTP request.
   */
  private record ResolveCacheKey(
      @Nullable String usageIdentity,
      @Nonnull String actorUrn,
      @Nullable String sessionActorUrn,
      @Nullable ActorType sessionActorType) {

    static ResolveCacheKey of(
        @Nonnull RequestContext requestContext, @Nonnull Authentication authentication) {
      String sessionActorUrn =
          authentication.getActor() != null ? authentication.getActor().toUrnStr() : null;
      ActorType sessionActorType =
          authentication.getActor() != null ? authentication.getActor().getType() : null;
      return new ResolveCacheKey(
          requestContext.getUsageIdentity(),
          requestContext.getActorUrn(),
          sessionActorUrn,
          sessionActorType);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (!(o instanceof ResolveCacheKey that)) {
        return false;
      }
      return Objects.equals(usageIdentity, that.usageIdentity)
          && actorUrn.equals(that.actorUrn)
          && Objects.equals(sessionActorUrn, that.sessionActorUrn)
          && sessionActorType == that.sessionActorType;
    }

    @Override
    public int hashCode() {
      return Objects.hash(usageIdentity, actorUrn, sessionActorUrn, sessionActorType);
    }
  }
}
