package com.linkedin.metadata.graph.cache.client;

import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphBinding;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.MembershipNeighborResult;
import com.linkedin.metadata.graph.cache.ReadMode;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;

/** Call-site helpers for {@link EntityGraphCache} including skip-cache read behavior. */
public final class EntityGraphCacheClients {

  private EntityGraphCacheClients() {}

  @Nonnull
  public static GraphReadResult expand(@Nonnull GraphExpandRequest request) {
    if (request.getRoots().isEmpty()) {
      return GraphReadResult.fromVertices(Collections.emptySet());
    }
    String graphId;
    GraphSnapshotSource source;
    if (request.getBinding() != null) {
      graphId = request.getBinding().getGraphId();
      source = request.getBinding().getSource();
    } else if (request.getGraphId() != null && request.getSource() != null) {
      graphId = request.getGraphId();
      source = request.getSource();
    } else {
      throw new IllegalArgumentException(
          "GraphExpandRequest requires binding or graphId and source");
    }
    ReadMode mode = shouldSkipCache(request.getOpContext()) ? ReadMode.EPHEMERAL : ReadMode.CACHED;
    return request
        .getCache()
        .expand(
            graphId,
            source,
            request.getDirection(),
            request.getRoots(),
            request.getLimit(),
            request.getMaxDepth(),
            mode);
  }

  @Nonnull
  public static AncestorWalkResult walkOrderedForwardAncestors(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphCache cache,
      @Nonnull EntityGraphBinding binding,
      @Nonnull String seed,
      int maxDepth) {
    if (maxDepth <= 0) {
      return AncestorWalkResult.fromAncestors(List.of());
    }
    ReadMode mode = shouldSkipCache(opContext) ? ReadMode.EPHEMERAL : ReadMode.CACHED;
    return cache.walkOrderedForwardAncestors(
        binding.getGraphId(), binding.getSource(), seed, maxDepth, mode);
  }

  @Nonnull
  public static MembershipNeighborResult listRelated(
      @Nonnull OperationContext opContext,
      @Nonnull EntityGraphCache cache,
      @Nonnull EntityGraphBinding binding,
      @Nonnull String seedUrn,
      @Nonnull TraversalDirection direction,
      @Nonnull java.util.Set<String> relationshipTypes,
      int maxDepth,
      int start,
      int count) {
    ReadMode mode = shouldSkipCache(opContext) ? ReadMode.EPHEMERAL : ReadMode.CACHED;
    return cache.listRelated(
        binding.getGraphId(),
        binding.getSource(),
        seedUrn,
        direction,
        relationshipTypes,
        maxDepth,
        start,
        count,
        mode);
  }

  private static boolean shouldSkipCache(@Nonnull OperationContext opContext) {
    return opContext.getSearchContext().getSearchFlags().isSkipCache() != null
        && opContext.getSearchContext().getSearchFlags().isSkipCache();
  }
}
