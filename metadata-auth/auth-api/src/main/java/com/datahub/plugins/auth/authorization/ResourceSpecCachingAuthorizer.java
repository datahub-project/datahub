package com.datahub.plugins.auth.authorization;

import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizationResult;
import com.datahub.authorization.EntitySpec;
import com.datahub.authorization.ResolvedEntitySpec;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Opt-in capability for {@link Authorizer}s that can share resolved resource specs across the many
 * authorization checks issued within a single request.
 *
 * <p>Rendering a page's per-result action affordances authorizes each entity for ~15-22 privileges,
 * and resolving a resource's authorization attributes (domain, container, owner) can force a
 * recursive hierarchy walk against GMS. That resolution is actor-independent, so an implementor can
 * resolve each resource at most once per request via the supplied cache.
 *
 * <p>The public {@link Authorizer} contract is unchanged: non-implementors resolve per call, and
 * callers without a cache pass {@code null}.
 */
public interface ResourceSpecCachingAuthorizer {

  /**
   * @param resourceSpecCache request-scoped cache of resolved specs, or {@code null} to resolve
   *     without caching
   */
  AuthorizationResult authorize(
      @Nonnull AuthorizationRequest request,
      @Nullable Map<EntitySpec, ResolvedEntitySpec> resourceSpecCache);
}
