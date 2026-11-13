package com.linkedin.metadata.graph.cache.service.read;

import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.service.strategy.FullGraphScopeReadStrategy;
import com.linkedin.metadata.graph.cache.service.strategy.PartialGraphScopeReadStrategy;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;

/** Wires shared and scope-specific graph read backends to read strategies. */
public final class GraphCacheReaderFactory {

  private GraphCacheReaderFactory() {}

  @Nonnull
  public static GraphCacheReaderComponents create(
      @Nonnull EntityGraphDistributedStore distributedStore,
      @Nonnull EntityGraphLocalViewCache localViews,
      @Nonnull EntityGraphSnapshotBuilder snapshotBuilder,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull SnapshotFreshnessEvaluator freshnessEvaluator,
      @Nonnull GraphCacheRebuilder rebuilder) {
    GraphReadBackend sharedReadBackend =
        new GraphReadBackend(
            distributedStore,
            localViews,
            snapshotBuilder,
            systemOperationContext,
            freshnessEvaluator,
            rebuilder);
    PartialGraphReadBackend partialReadBackend =
        new PartialGraphReadBackend(
            sharedReadBackend,
            distributedStore,
            snapshotBuilder,
            systemOperationContext,
            freshnessEvaluator,
            rebuilder);
    return new GraphCacheReaderComponents(
        sharedReadBackend,
        partialReadBackend,
        new FullGraphScopeReadStrategy(sharedReadBackend),
        new PartialGraphScopeReadStrategy(partialReadBackend));
  }

  public record GraphCacheReaderComponents(
      GraphReadBackend sharedReadBackend,
      PartialGraphReadBackend partialReadBackend,
      FullGraphScopeReadStrategy fullReadStrategy,
      PartialGraphScopeReadStrategy partialReadStrategy) {}
}
