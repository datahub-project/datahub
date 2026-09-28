package com.linkedin.metadata.graph.cache.service.strategy;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import javax.annotation.Nonnull;

/** Scope-specific cache key and rebuild behavior for FULL graphs. */
public final class FullGraphScopeStrategy {

  private FullGraphScopeStrategy() {}

  @Nonnull
  public static String cacheKey(
      @Nonnull EntityGraphDefinition definition, @Nonnull GraphSnapshotSource source) {
    return EntityGraphCacheKeys.fullCacheKey(definition.getGraphId(), source);
  }

  public static boolean matches(@Nonnull EntityGraphDefinition definition) {
    return definition.getScope().getMode() == ScopeMode.FULL;
  }
}
