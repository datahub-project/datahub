package com.linkedin.metadata.graph.cache.snapshot;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView.ClosureReplacement;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/** Surgical edits to cached graph snapshots. */
public final class EntityGraphSnapshotEditor {

  private EntityGraphSnapshotEditor() {}

  @Value
  public static class VertexRemovalResult {
    boolean changed;

    /** When {@code dropKey} is true, remove the cache entry; otherwise publish {@code snapshot}. */
    boolean dropKey;

    @Nullable EntityGraphSnapshot snapshot;
  }

  @Nonnull
  public static VertexRemovalResult removeVertex(
      @Nonnull EntityGraphSnapshot snapshot, @Nonnull String entityUrn) {
    List<EntityGraphSnapshot.DirectedEdge> edges =
        snapshot.getEdges() != null ? snapshot.getEdges() : List.of();
    EntityGraphView view = new EntityGraphView(edges);
    var updatedView = view.withoutVertex(entityUrn);
    if (updatedView.isEmpty()) {
      return new VertexRemovalResult(false, false, snapshot);
    }
    EntityGraphView updated = updatedView.get();
    if (updated.getEdges().isEmpty()) {
      return new VertexRemovalResult(true, true, null);
    }
    return new VertexRemovalResult(
        true,
        false,
        EntityGraphSnapshotMaterializer.rebuildWithEdges(snapshot, updated.getEdges()));
  }

  @Value
  public static class FullWalkEdit {
    boolean containsAllSeeds;
    int exploredDepth;

    @Nullable EntityGraphSnapshot snapshot;
  }

  /**
   * Replaces the directional closure and stamps that direction as a trusted full walk for {@code
   * seeds}. The cache key comes from {@code existing} when editing. The other direction's coverage
   * is kept.
   */
  @Nonnull
  public static FullWalkEdit applyFullWalk(
      @Nullable EntityGraphSnapshot existing,
      @Nonnull String graphId,
      @Nonnull String cacheKey,
      @Nonnull String buildSource,
      long builtAtMillis,
      long generation,
      @Nonnull TraversalDirection direction,
      @Nonnull Set<String> seeds,
      @Nonnull List<DirectedEdge> walkedEdges,
      int configuredMaxDepth) {
    List<DirectedEdge> baseEdges =
        existing != null && existing.getEdges() != null ? existing.getEdges() : List.of();
    ClosureReplacement replacement =
        new EntityGraphView(baseEdges).replacingClosure(direction, seeds, walkedEdges);
    if (!replacement.isContainsAllSeeds()) {
      return new FullWalkEdit(false, replacement.getExploredDepth(), null);
    }
    TraversalCoverage prior = existing == null ? null : existing.getTraversalCoverage();
    DirectionCoverage previous = prior == null ? null : prior.getDirection(direction);
    DirectionCoverage.DirectionCoverageBuilder stamped =
        DirectionCoverage.builder()
            .direction(direction)
            .trustedSeeds(List.copyOf(seeds))
            .trustedEdgeLines(replacement.getTrustedEdgeLines());
    if (previous != null) {
      stamped
          .explored(previous.isExplored())
          .exploredDepth(previous.getExploredDepth())
          .configuredMaxDepth(previous.getConfiguredMaxDepth())
          .complete(previous.isComplete())
          .truncationReason(previous.getTruncationReason());
    } else {
      stamped
          .explored(true)
          .exploredDepth(replacement.getExploredDepth())
          .configuredMaxDepth(configuredMaxDepth)
          .complete(true);
    }
    DirectionCoverage stampedCoverage = stamped.build();
    TraversalCoverage coverage =
        prior == null
            ? TraversalCoverage.builder().direction(stampedCoverage).build()
            : prior.withDirection(stampedCoverage);
    return new FullWalkEdit(
        true,
        replacement.getExploredDepth(),
        EntityGraphSnapshotMaterializer.materializeFullWalk(
            graphId,
            cacheKey,
            buildSource,
            builtAtMillis,
            generation,
            replacement.getEdges(),
            coverage));
  }
}
