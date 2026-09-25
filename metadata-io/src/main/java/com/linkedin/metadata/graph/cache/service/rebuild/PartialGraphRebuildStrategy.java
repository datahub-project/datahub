package com.linkedin.metadata.graph.cache.service.rebuild;

import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.service.strategy.PartialGraphScopeStrategy;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collection;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** PARTIAL-scope rebuild: directional BFS with optional merge-in-place at publish key. */
public class PartialGraphRebuildStrategy implements GraphScopeRebuildStrategy {
  private final EntityGraphDistributedStore distributedStore;
  private final EntityGraphSnapshotBuilder snapshotBuilder;
  private final OperationContext systemOperationContext;

  public PartialGraphRebuildStrategy(
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext) {
    this.distributedStore = distributedStore;
    this.snapshotBuilder = snapshotBuilder;
    this.systemOperationContext = systemOperationContext;
  }

  @Nonnull
  @Override
  public BuildResult executeRebuild(
      @Nonnull EntityGraphDefinition definition,
      @Nonnull GraphSnapshotSource source,
      @Nullable Collection<String> seeds,
      @Nullable String publishKeyHint,
      @Nonnull TraversalDirection direction) {
    EntityGraphSnapshot existing =
        publishKeyHint != null ? distributedStore.getSnapshot(publishKeyHint) : null;
    boolean mergeExisting =
        existing != null && publishKeyHint != null && publishKeyHint.equals(existing.getCacheKey());
    return snapshotBuilder.buildPartial(
        systemOperationContext,
        definition,
        source,
        seeds,
        direction,
        mergeExisting ? existing.getEdges() : null,
        mergeExisting ? existing.getTraversalCoverage() : null,
        mergeExisting ? existing.getCacheKey() : null);
  }

  @Nonnull
  public static String failureMarkerKey(
      @Nonnull String graphId, @Nonnull GraphSnapshotSource source, @Nonnull String root) {
    return PartialGraphScopeStrategy.failureMarkerKey(graphId, source, root);
  }

  @Nullable
  public static String rebuildClaimKey(
      @Nonnull String graphId,
      @Nonnull GraphSnapshotSource source,
      @Nullable String publishKeyHint,
      @Nullable Collection<String> seeds) {
    if (publishKeyHint != null) {
      return publishKeyHint;
    }
    if (seeds == null || seeds.isEmpty()) {
      return null;
    }
    return failureMarkerKey(graphId, source, seeds.iterator().next());
  }
}
