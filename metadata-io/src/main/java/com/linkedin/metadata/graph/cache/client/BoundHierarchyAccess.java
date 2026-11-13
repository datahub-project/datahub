package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphEndpoints;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Value;

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
    if (shouldUseCache(opContext, spec, includeSoftDelete)) {
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
        return EntityGraphEndpoints.toUrnSet(result.verticesOrEmpty());
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
    if (shouldUseCache(opContext, spec, includeSoftDelete)) {
      AncestorWalkResult walkResult =
          EntityGraphCacheClients.walkOrderedForwardAncestors(
              opContext,
              opContext.getEntityGraphCache(),
              spec.getBinding(),
              seedUrn.toString(),
              maxDepth);
      if (walkResult.isHit()) {
        return EntityGraphEndpoints.toUrnList(walkResult.ancestorsOrEmpty());
      }
    }
    return AspectParentWalker.orderedParents(opContext, spec, seedUrn, maxDepth);
  }

  /** Result of {@link #orderedParentsBatch}: a chain or a failure for every distinct seed. */
  @Value
  public static class BatchParents {
    @Nonnull Map<Urn, List<Urn>> parents;
    @Nonnull Map<Urn, RuntimeException> failures;
  }

  /**
   * {@link #orderedParents} for many seeds at once, on the calling thread, with bounded DB reads.
   *
   * <ul>
   *   <li>Seeds whose entity type is a vertex in the bound graph (for example containers) are tried
   *       against the cache one by one, as {@link #orderedParents} does. A cache error fails only
   *       that seed. After the first miss for any reason other than {@link ReadMissReason#ABSENT}
   *       (the cache cannot serve right now), the rest of the batch skips the cache.
   *   <li>Every other seed (for example a dataset, which the container graph never holds) skips the
   *       cache entirely, as do seeds the cache missed. Like {@link #orderedParents} after a miss,
   *       their chains come from live aspect reads: one batched read for their direct parents, then
   *       a level-by-level batched walk from each distinct direct parent, shared by all seeds under
   *       it.
   * </ul>
   *
   * <p>A failed DB read fails only the seeds whose chain needed it, rather than shortening their
   * chains; seeds in other read groups, and chains that finished first, are unaffected.
   */
  @Nonnull
  public static BatchParents orderedParentsBatch(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Collection<Urn> seeds,
      int maxDepth) {
    Map<Urn, List<Urn>> parents = new LinkedHashMap<>();
    Map<Urn, RuntimeException> failures = new LinkedHashMap<>();
    Set<Urn> distinctSeeds = new LinkedHashSet<>(seeds);
    if (maxDepth <= 0) {
      distinctSeeds.forEach(seed -> parents.put(seed, Collections.emptyList()));
      return new BatchParents(parents, failures);
    }
    boolean useCache = shouldUseCache(opContext, spec, false);

    // 1. Seeds the cache can hold: per-seed cache read, as before.
    Set<Urn> uncached = new LinkedHashSet<>();
    for (Urn seed : distinctSeeds) {
      if (!useCache || !isCacheVertex(spec, seed)) {
        uncached.add(seed);
        continue;
      }
      try {
        AncestorWalkResult walkResult = cachedOrderedParents(opContext, spec, seed, maxDepth);
        if (walkResult instanceof AncestorWalkResult.Hit hit) {
          parents.put(seed, EntityGraphEndpoints.toUrnList(hit.ancestors()));
          continue;
        }
        if (walkResult instanceof AncestorWalkResult.Miss miss
            && miss.reason() != ReadMissReason.ABSENT) {
          // The cache cannot serve this graph right now; don't make every remaining seed wait on
          // it in turn.
          useCache = false;
        }
        uncached.add(seed);
      } catch (RuntimeException e) {
        failures.put(seed, e);
      }
    }
    if (uncached.isEmpty()) {
      return orderedAs(distinctSeeds, parents, failures);
    }

    // 2. Direct parents of the remaining seeds: one batched read per entity type. A failed read
    //    fails only the seeds in that read.
    AspectParentWalker.ParentLookup direct =
        AspectParentWalker.batchParents(opContext, spec, uncached);
    failures.putAll(direct.getFailures());
    Map<Urn, Urn> directParents = direct.getParents();

    // 3. Ancestors above each distinct direct parent, resolved once from live aspect reads (not the
    //    cache, which these seeds missed or can never hit). Chains that finish are kept even if a
    //    deeper read for another chain fails.
    Map<Urn, List<Urn>> above = new HashMap<>();
    Map<Urn, RuntimeException> aboveFailures = new HashMap<>();
    if (maxDepth > 1 && !directParents.isEmpty()) {
      AspectParentWalker.ChainLookup walked =
          AspectParentWalker.orderedParentsBatch(
              opContext, spec, new LinkedHashSet<>(directParents.values()), maxDepth - 1);
      above.putAll(walked.getChains());
      aboveFailures.putAll(walked.getFailures());
    }

    // 4. Stitch: seed -> direct parent -> ancestors, stopping at a cycle or maxDepth.
    for (Urn seed : uncached) {
      if (failures.containsKey(seed)) {
        continue;
      }
      Urn directParent = directParents.get(seed);
      if (directParent == null) {
        parents.put(seed, Collections.emptyList());
        continue;
      }
      RuntimeException failure = aboveFailures.get(directParent);
      if (failure != null) {
        failures.put(seed, failure);
        continue;
      }
      List<Urn> chain = new ArrayList<>();
      Set<Urn> visited = new HashSet<>();
      visited.add(seed);
      List<Urn> candidates = new ArrayList<>();
      candidates.add(directParent);
      candidates.addAll(above.getOrDefault(directParent, Collections.emptyList()));
      for (Urn ancestor : candidates) {
        if (chain.size() >= maxDepth || !visited.add(ancestor)) {
          break;
        }
        chain.add(ancestor);
      }
      parents.put(seed, chain);
    }
    return orderedAs(distinctSeeds, parents, failures);
  }

  /** Cache-only ordered walk for one seed. */
  @Nonnull
  private static AncestorWalkResult cachedOrderedParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn seedUrn,
      int maxDepth) {
    return EntityGraphCacheClients.walkOrderedForwardAncestors(
        opContext,
        opContext.getEntityGraphCache(),
        spec.getBinding(),
        seedUrn.toString(),
        maxDepth);
  }

  /**
   * Whether the bound graph can hold this urn as a child vertex. A seed of any other type (a
   * dataset under the container graph) can never hit, and each attempt costs blocking cache reads.
   */
  private static boolean isCacheVertex(@Nonnull HierarchyReadSpec spec, @Nonnull Urn urn) {
    return spec.getScrollSourceEntityTypes().contains(urn.getEntityType());
  }

  @Nonnull
  private static BatchParents orderedAs(
      @Nonnull Set<Urn> seeds,
      @Nonnull Map<Urn, List<Urn>> parents,
      @Nonnull Map<Urn, RuntimeException> failures) {
    Map<Urn, List<Urn>> ordered = new LinkedHashMap<>();
    for (Urn seed : seeds) {
      if (parents.containsKey(seed)) {
        ordered.put(seed, parents.get(seed));
      }
    }
    return new BatchParents(ordered, failures);
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
    if (!shouldUseCache(opContext, spec, includeSoftDelete)) {
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
      return EntityGraphEndpoints.toUrnSet(
          result.verticesOrEmpty().stream()
              .filter(urn -> !urn.equals(rootUrn.toString()))
              .collect(Collectors.toCollection(LinkedHashSet::new)));
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
    if (!shouldUseCache(opContext, spec, includeSoftDelete)) {
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
          EntityGraphEndpoints.toUrnSet(
              result.verticesOrEmpty().stream()
                  .filter(urn -> !urn.equals(parentUrn.toString()))
                  .collect(Collectors.toCollection(LinkedHashSet::new)));
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
    if (shouldUseCache(opContext, spec, includeSoftDelete)) {
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

  private static boolean shouldUseCache(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      boolean includeSoftDelete) {
    return !includeSoftDelete
        && EntityGraphCacheClients.isBoundKnownGraph(
            opContext.getEntityGraphCache(), spec.getBinding());
  }
}
