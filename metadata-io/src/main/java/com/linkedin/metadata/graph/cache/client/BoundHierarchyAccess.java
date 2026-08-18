package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;

/** Cache-first hierarchy reads with aspect and graph-scroll fallbacks. */
public final class BoundHierarchyAccess {

  private static final int DIRECT_CHILD_MAX_DEPTH = 1;

  private BoundHierarchyAccess() {}

  @Nonnull
  public static Set<Urn> expandAncestors(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Collection<Urn> roots) {
    return expandAncestors(opContext, spec, roots, false);
  }

  @Nonnull
  public static Set<Urn> expandAncestors(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Collection<Urn> roots,
      boolean includeSoftDelete) {
    if (roots.isEmpty()) {
      return Collections.emptySet();
    }
    if (shouldUseCache(includeSoftDelete)) {
      Set<String> rootStrings =
          roots.stream().map(Urn::toString).collect(Collectors.toCollection(LinkedHashSet::new));
      GraphReadResult result =
          EntityGraphCacheClients.expand(
              GraphExpandRequest.builder()
                  .opContext(opContext)
                  .cache(opContext.getEntityGraphCache())
                  .binding(spec.getBinding())
                  .direction(TraversalDirection.FORWARD)
                  .roots(rootStrings)
                  .limit(Integer.MAX_VALUE)
                  .build());
      if (result.isHit()) {
        return result.verticesOrEmpty().stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toCollection(LinkedHashSet::new));
      }
    }
    return AspectParentWalker.expandAncestors(opContext, spec, new LinkedHashSet<>(roots));
  }

  @Nonnull
  public static List<Urn> orderedParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn seedUrn,
      int maxDepth) {
    return orderedParents(opContext, spec, seedUrn, maxDepth, false);
  }

  @Nonnull
  public static List<Urn> orderedParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn seedUrn,
      int maxDepth,
      boolean includeSoftDelete) {
    if (maxDepth <= 0) {
      return Collections.emptyList();
    }
    if (shouldUseCache(includeSoftDelete)) {
      AncestorWalkResult walkResult =
          EntityGraphCacheClients.walkOrderedForwardAncestors(
              opContext,
              opContext.getEntityGraphCache(),
              spec.getBinding(),
              seedUrn.toString(),
              maxDepth);
      if (walkResult.isHit()) {
        return walkResult.ancestorsOrEmpty().stream()
            .map(UrnUtils::getUrn)
            .collect(Collectors.toList());
      }
    }
    return AspectParentWalker.orderedParents(opContext, spec, seedUrn, maxDepth);
  }

  @Nonnull
  public static Set<Urn> expandDescendants(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn rootUrn,
      int maxDepth) {
    return expandDescendants(opContext, spec, rootUrn, maxDepth, false);
  }

  @Nonnull
  public static Set<Urn> expandDescendants(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn rootUrn,
      int maxDepth,
      boolean includeSoftDelete) {
    if (!shouldUseCache(includeSoftDelete)) {
      return GraphScrollFallback.allDescendants(opContext, spec, rootUrn);
    }
    int cacheMaxDepth =
        maxDepth == Integer.MAX_VALUE ? EntityGraphCache.USE_DEFINITION_MAX_DEPTH : maxDepth;
    GraphReadResult result =
        EntityGraphCacheClients.expand(
            GraphExpandRequest.builder()
                .opContext(opContext)
                .cache(opContext.getEntityGraphCache())
                .binding(spec.getBinding())
                .direction(TraversalDirection.REVERSE)
                .roots(Set.of(rootUrn.toString()))
                .limit(Integer.MAX_VALUE)
                .maxDepth(cacheMaxDepth)
                .build());
    if (result.isHit()) {
      return result.verticesOrEmpty().stream()
          .filter(urn -> !urn.equals(rootUrn.toString()))
          .map(UrnUtils::getUrn)
          .collect(Collectors.toCollection(LinkedHashSet::new));
    }
    return GraphScrollFallback.allDescendants(opContext, spec, rootUrn);
  }

  @Nonnull
  public static Set<Urn> directChildUrns(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn parentUrn) {
    return directChildUrns(opContext, spec, parentUrn, false);
  }

  @Nonnull
  public static Set<Urn> directChildUrns(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn parentUrn,
      boolean includeSoftDelete) {
    return directChildren(opContext, spec, parentUrn, includeSoftDelete).getChildUrns();
  }

  @Nonnull
  public static DirectChildrenResult directChildren(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn parentUrn) {
    return directChildren(opContext, spec, parentUrn, false);
  }

  @Nonnull
  public static DirectChildrenResult directChildren(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn parentUrn,
      boolean includeSoftDelete) {
    if (!shouldUseCache(includeSoftDelete)) {
      return GraphScrollFallback.directChildren(opContext, spec, parentUrn);
    }
    GraphReadResult result =
        EntityGraphCacheClients.expand(
            GraphExpandRequest.builder()
                .opContext(opContext)
                .cache(opContext.getEntityGraphCache())
                .binding(spec.getBinding())
                .direction(TraversalDirection.REVERSE)
                .roots(Set.of(parentUrn.toString()))
                .limit(Integer.MAX_VALUE)
                .maxDepth(DIRECT_CHILD_MAX_DEPTH)
                .build());
    if (result.isHit()) {
      Set<Urn> children =
          result.verticesOrEmpty().stream()
              .filter(urn -> !urn.equals(parentUrn.toString()))
              .map(UrnUtils::getUrn)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      return new DirectChildrenResult(children, false);
    }
    return GraphScrollFallback.directChildren(opContext, spec, parentUrn);
  }

  public static boolean isDescendant(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn candidateUrn,
      @Nonnull Urn ancestorUrn) {
    return isDescendant(opContext, spec, candidateUrn, ancestorUrn, false);
  }

  public static boolean isDescendant(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn candidateUrn,
      @Nonnull Urn ancestorUrn,
      boolean includeSoftDelete) {
    if (candidateUrn.equals(ancestorUrn)) {
      return false;
    }
    if (shouldUseCache(includeSoftDelete)) {
      GraphReadResult result =
          EntityGraphCacheClients.expand(
              GraphExpandRequest.builder()
                  .opContext(opContext)
                  .cache(opContext.getEntityGraphCache())
                  .binding(spec.getBinding())
                  .direction(TraversalDirection.REVERSE)
                  .roots(Set.of(ancestorUrn.toString()))
                  .limit(Integer.MAX_VALUE)
                  .build());
      if (result.isHit() && result.verticesOrEmpty().contains(candidateUrn.toString())) {
        return true;
      }
    }
    return AspectParentWalker.isAncestorOf(opContext, spec, candidateUrn, ancestorUrn);
  }

  private static boolean shouldUseCache(boolean includeSoftDelete) {
    return !includeSoftDelete;
  }
}
