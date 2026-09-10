package com.linkedin.metadata.graph.cache.snapshot;

import java.util.List;
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
}
