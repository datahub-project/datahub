package com.linkedin.metadata.graph.cache.service.read;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import javax.annotation.Nonnull;

/** Resolves per-call expand/walk depth against graph scope rules. */
public final class GraphReadDepthResolver {

  private GraphReadDepthResolver() {}

  /**
   * FULL scope: {@link EntityGraphCache#USE_DEFINITION_MAX_DEPTH} walks the entire materialized
   * snapshot (size capped at build by {@code bounds.*}). PARTIAL scope: {@code scope.maxDepth} from
   * config caps in-memory traversal for reads as well as builds; explicit caller depths are clamped
   * to that cap.
   */
  public static int resolve(@Nonnull EntityGraphDefinition definition, int callerMaxDepth) {
    if (definition.getScope().getMode() == ScopeMode.PARTIAL) {
      int configMaxDepth = definition.getScope().getMaxDepth();
      if (callerMaxDepth == EntityGraphCache.USE_DEFINITION_MAX_DEPTH) {
        return configMaxDepth;
      }
      if (callerMaxDepth > 0) {
        return Math.min(callerMaxDepth, configMaxDepth);
      }
      return configMaxDepth;
    }
    if (callerMaxDepth == EntityGraphCache.USE_DEFINITION_MAX_DEPTH) {
      return Integer.MAX_VALUE;
    }
    return callerMaxDepth;
  }
}
