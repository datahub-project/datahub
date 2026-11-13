package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
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
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

/** Batch aspect walks for parent chains when the entity graph cache cannot satisfy a read. */
@Slf4j
public final class AspectParentWalker {

  private AspectParentWalker() {}

  @Nonnull
  public static Set<Urn> expandAncestors(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Set<Urn> initialRoots) {
    Set<Urn> expanded = new LinkedHashSet<>(initialRoots);
    Set<Urn> batchedParents = getBatchedParents(opContext, spec, expanded);
    batchedParents.removeAll(expanded);

    while (!batchedParents.isEmpty()) {
      expanded.addAll(batchedParents);
      batchedParents = getBatchedParents(opContext, spec, batchedParents);
      batchedParents.removeAll(expanded);
    }
    return expanded;
  }

  @Nonnull
  public static List<Urn> orderedParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn seed,
      int maxDepth) {
    List<Urn> parents = new ArrayList<>();
    Set<String> visited = new HashSet<>();
    visited.add(seed.toString());
    Urn current = seed;

    for (int depth = 0; depth < maxDepth; depth++) {
      Urn parent = getParentFromAspects(opContext, spec, current);
      if (parent == null || !visited.add(parent.toString())) {
        break;
      }
      parents.add(parent);
      current = parent;
    }
    return parents;
  }

  /** Direct parents from a batched read, plus the urns whose read failed. */
  @Value
  public static class ParentLookup {
    @Nonnull Map<Urn, Urn> parents;
    @Nonnull Map<Urn, RuntimeException> failures;
  }

  /** Parent chains from a batched walk, plus the seeds whose walk hit a failed read. */
  @Value
  public static class ChainLookup {
    @Nonnull Map<Urn, List<Urn>> chains;
    @Nonnull Map<Urn, RuntimeException> failures;
  }

  /**
   * Ordered parent chains for many seeds, reading each level once for all seeds instead of once per
   * seed. A chain shares the lookup of any ancestor it has in common with another chain, so a batch
   * of siblings costs one read per level.
   *
   * <p>Unlike {@link #orderedParents}, a failed read does not end a chain early. The seeds whose
   * chain needed that read are reported in {@link ChainLookup#getFailures()}; every other seed,
   * including one whose chain finished before the failure, keeps its chain.
   */
  @Nonnull
  public static ChainLookup orderedParentsBatch(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Collection<Urn> seeds,
      int maxDepth) {
    Map<Urn, List<Urn>> chains = new LinkedHashMap<>();
    Map<Urn, RuntimeException> failures = new LinkedHashMap<>();
    Map<Urn, Set<Urn>> visitedBySeed = new HashMap<>();
    Map<Urn, Urn> frontier = new LinkedHashMap<>();
    for (Urn seed : seeds) {
      if (chains.containsKey(seed)) {
        continue;
      }
      chains.put(seed, new ArrayList<>());
      Set<Urn> visited = new HashSet<>();
      visited.add(seed);
      visitedBySeed.put(seed, visited);
      frontier.put(seed, seed);
    }

    Map<Urn, Urn> parentCache = new HashMap<>();
    Map<Urn, RuntimeException> failedReads = new HashMap<>();
    for (int depth = 0; depth < maxDepth && !frontier.isEmpty(); depth++) {
      Set<Urn> toRead = new LinkedHashSet<>();
      for (Urn current : frontier.values()) {
        if (!parentCache.containsKey(current) && !failedReads.containsKey(current)) {
          toRead.add(current);
        }
      }
      if (!toRead.isEmpty()) {
        ParentLookup read = readParents(opContext, spec, toRead);
        failedReads.putAll(read.getFailures());
        for (Urn urn : toRead) {
          if (!read.getFailures().containsKey(urn)) {
            parentCache.put(urn, read.getParents().get(urn));
          }
        }
      }

      Map<Urn, Urn> next = new LinkedHashMap<>();
      for (Map.Entry<Urn, Urn> entry : frontier.entrySet()) {
        Urn seed = entry.getKey();
        RuntimeException failure = failedReads.get(entry.getValue());
        if (failure != null) {
          // Fail this seed rather than return its chain cut short at the failed level.
          chains.remove(seed);
          failures.put(seed, failure);
          continue;
        }
        Urn parent = parentCache.get(entry.getValue());
        if (parent == null || !visitedBySeed.get(seed).add(parent)) {
          continue;
        }
        chains.get(seed).add(parent);
        next.put(seed, parent);
      }
      frontier = next;
    }
    return new ChainLookup(chains, failures);
  }

  /**
   * Direct parent of each URN, one batched read per entity type and parent aspect. A failed read is
   * reported against the urns of its group only, not logged and treated as "no parent".
   */
  @Nonnull
  public static ParentLookup batchParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Set<Urn> urns) {
    return readParents(opContext, spec, urns);
  }

  public static boolean isAncestorOf(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Urn candidate,
      @Nonnull Urn ancestor) {
    Set<String> visited = new HashSet<>();
    Urn current = candidate;
    while (current != null && visited.add(current.toString())) {
      if (current.equals(ancestor)) {
        return true;
      }
      current = getParentFromAspects(opContext, spec, current);
    }
    return false;
  }

  @Nullable
  public static Urn getParent(
      @Nonnull OperationContext opContext, @Nonnull HierarchyReadSpec spec, @Nonnull Urn urn) {
    return getParentFromAspects(opContext, spec, urn);
  }

  @Nonnull
  private static Set<Urn> getBatchedParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Set<Urn> urns) {
    Map<Urn, Urn> parentsByUrn = batchGetParents(opContext, spec, urns);
    Set<Urn> parentUrns = new LinkedHashSet<>();
    for (Urn urn : urns) {
      Urn parent = parentsByUrn.get(urn);
      if (parent != null) {
        parentUrns.add(parent);
      }
    }
    return parentUrns;
  }

  @Nullable
  private static Urn getParentFromAspects(
      @Nonnull OperationContext opContext, @Nonnull HierarchyReadSpec spec, @Nonnull Urn urn) {
    return batchGetParents(opContext, spec, Set.of(urn)).get(urn);
  }

  /**
   * Single-chain walks keep their long-standing behaviour: a failed read is logged and treated as
   * "no parent".
   */
  @Nonnull
  private static Map<Urn, Urn> batchGetParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Set<Urn> urns) {
    return readParents(opContext, spec, urns).getParents();
  }

  @Nonnull
  private static ParentLookup readParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Set<Urn> urns) {
    if (urns.isEmpty()) {
      return new ParentLookup(Collections.emptyMap(), Collections.emptyMap());
    }

    // Group by entity type as well as aspect: the aspect retriever reads a whole batch under the
    // first urn's entity type, and seeds such as a data product's assets mix several types.
    Map<List<String>, Set<Urn>> urnsByTypeAndAspect = new LinkedHashMap<>();
    for (Urn urn : urns) {
      ParentAspectSpec parentAspect = spec.parentAspectFor(urn);
      if (parentAspect == null) {
        continue;
      }
      urnsByTypeAndAspect
          .computeIfAbsent(
              List.of(urn.getEntityType(), parentAspect.getAspectName()),
              ignored -> new LinkedHashSet<>())
          .add(urn);
    }

    Map<Urn, Urn> parentsByUrn = new LinkedHashMap<>();
    Map<Urn, RuntimeException> failures = new LinkedHashMap<>();
    AspectRetriever aspectRetriever = opContext.getRetrieverContext().getAspectRetriever();
    for (Map.Entry<List<String>, Set<Urn>> entry : urnsByTypeAndAspect.entrySet()) {
      String aspectName = entry.getKey().get(1);
      Set<Urn> batch = entry.getValue();
      try {
        Map<Urn, Map<String, Aspect>> aspects =
            aspectRetriever.getLatestAspectObjects(opContext, batch, Set.of(aspectName));
        for (Urn urn : batch) {
          ParentAspectSpec parentAspect = spec.parentAspectFor(urn);
          if (parentAspect == null) {
            continue;
          }
          Map<String, Aspect> aspectMap = aspects.get(urn);
          if (aspectMap == null || !aspectMap.containsKey(aspectName)) {
            continue;
          }
          DataMap data = aspectMap.get(aspectName).data();
          Optional<Urn> parent = parentAspect.getParentExtractor().extractParent(data);
          parent.ifPresent(value -> parentsByUrn.put(urn, value));
        }
      } catch (RuntimeException e) {
        log.error("Failed to batch read {} for {} urns", aspectName, batch.size(), e);
        batch.forEach(urn -> failures.put(urn, e));
      }
    }
    return new ParentLookup(parentsByUrn, failures);
  }
}
