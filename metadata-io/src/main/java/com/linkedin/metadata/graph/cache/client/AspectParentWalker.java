package com.linkedin.metadata.graph.cache.client;

import com.linkedin.common.urn.Urn;
import com.linkedin.data.DataMap;
import com.linkedin.entity.Aspect;
import com.linkedin.metadata.aspect.AspectRetriever;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
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

  @Nonnull
  private static Map<Urn, Urn> batchGetParents(
      @Nonnull OperationContext opContext,
      @Nonnull HierarchyReadSpec spec,
      @Nonnull Set<Urn> urns) {
    if (urns.isEmpty()) {
      return Collections.emptyMap();
    }

    Map<String, Set<Urn>> urnsByAspect = new LinkedHashMap<>();
    for (Urn urn : urns) {
      ParentAspectSpec parentAspect = spec.parentAspectFor(urn);
      if (parentAspect == null) {
        continue;
      }
      urnsByAspect
          .computeIfAbsent(parentAspect.getAspectName(), ignored -> new LinkedHashSet<>())
          .add(urn);
    }

    Map<Urn, Urn> parentsByUrn = new LinkedHashMap<>();
    AspectRetriever aspectRetriever = opContext.getRetrieverContext().getAspectRetriever();
    for (Map.Entry<String, Set<Urn>> entry : urnsByAspect.entrySet()) {
      String aspectName = entry.getKey();
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
      } catch (Exception e) {
        log.error("Failed to batch read {} for {} urns", aspectName, batch.size(), e);
      }
    }
    return parentsByUrn;
  }
}
