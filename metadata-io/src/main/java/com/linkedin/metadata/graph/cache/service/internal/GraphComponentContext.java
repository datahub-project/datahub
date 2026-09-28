package com.linkedin.metadata.graph.cache.service.internal;

import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Resolved snapshot view plus traversal coverage for one cache component. */
public final class GraphComponentContext {
  private final EntityGraphView view;
  @Nullable private final TraversalCoverage coverage;
  @Nullable private final String cacheKey;

  public GraphComponentContext(
      @Nonnull EntityGraphView view,
      @Nullable TraversalCoverage coverage,
      @Nullable String cacheKey) {
    this.view = view;
    this.coverage = coverage;
    this.cacheKey = cacheKey;
  }

  @Nonnull
  public EntityGraphView view() {
    return view;
  }

  @Nullable
  public TraversalCoverage coverage() {
    return coverage;
  }

  @Nullable
  public String cacheKey() {
    return cacheKey;
  }

  @Nonnull
  public GraphComponentContext withCacheKey(@Nonnull String key) {
    return new GraphComponentContext(view, coverage, key);
  }

  @Nonnull
  public static GraphComponentContext fromSnapshot(
      @Nonnull EntityGraphSnapshot snapshot, @Nonnull EntityGraphView view) {
    return new GraphComponentContext(view, snapshot.getTraversalCoverage(), snapshot.getCacheKey());
  }

  @Nonnull
  public static Set<String> rootsFrom(@Nonnull Collection<String> roots) {
    return roots.stream().collect(LinkedHashSet::new, LinkedHashSet::add, LinkedHashSet::addAll);
  }

  @Nullable
  public static EntityGraphView viewFromEdges(
      @Nullable List<EntityGraphSnapshot.DirectedEdge> edges) {
    if (edges == null || edges.isEmpty()) {
      return null;
    }
    return new EntityGraphView(edges);
  }
}
