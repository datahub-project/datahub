package com.linkedin.metadata.graph.cache.service.strategy;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import javax.annotation.Nonnull;

/** Scope-specific cache key helpers for PARTIAL graphs. */
public final class PartialGraphScopeStrategy {

  private PartialGraphScopeStrategy() {}

  @Nonnull
  public static String failureMarkerKey(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull String root) {
    return EntityGraphCacheKeys.partialFailureMarkerKey(graphId, source, root);
  }

  public static boolean matches(@Nonnull EntityGraphDefinition definition) {
    return definition.getScope().getMode() == ScopeMode.PARTIAL;
  }
}
