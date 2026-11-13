package com.linkedin.metadata.graph.cache.service.invalidation;

import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Declarative policy for mapping sync writes to cache invalidation actions. */
public final class SyncInvalidationPolicy {

  private SyncInvalidationPolicy() {}

  public enum InvalidationAction {
    DROP_GRAPH,
    DROP_PARTIAL,
    SURGICAL_REMOVE,
    NO_OP
  }

  @Nonnull
  public static InvalidationAction forCreate(
      @Nonnull CacheStatus status, @Nonnull ScopeMode scopeMode) {
    if (status == CacheStatus.INVALID || status == CacheStatus.OVER_LIMIT) {
      return InvalidationAction.NO_OP;
    }
    if (status == CacheStatus.ABSENT
        || status == CacheStatus.ACTIVE
        || status == CacheStatus.COOLDOWN
        || status == CacheStatus.BUILDING) {
      return scopeMode == ScopeMode.PARTIAL
          ? InvalidationAction.DROP_PARTIAL
          : InvalidationAction.DROP_GRAPH;
    }
    return InvalidationAction.NO_OP;
  }

  @Nonnull
  public static InvalidationAction forUpdate(
      @Nonnull CacheStatus status, @Nonnull ScopeMode scopeMode, @Nullable String aspectName) {
    if (Constants.STATUS_ASPECT_NAME.equals(aspectName)) {
      return InvalidationAction.SURGICAL_REMOVE;
    }
    return forCreate(status, scopeMode);
  }

  @Nonnull
  public static InvalidationAction forDelete(
      @Nonnull CacheStatus status, @Nonnull ScopeMode scopeMode) {
    if (status == CacheStatus.INVALID || status == CacheStatus.ABSENT) {
      return InvalidationAction.NO_OP;
    }
    if (scopeMode == ScopeMode.PARTIAL) {
      if (status == CacheStatus.ACTIVE
          || status == CacheStatus.COOLDOWN
          || status == CacheStatus.BUILDING) {
        return InvalidationAction.DROP_PARTIAL;
      }
      return InvalidationAction.NO_OP;
    }
    return InvalidationAction.SURGICAL_REMOVE;
  }
}
