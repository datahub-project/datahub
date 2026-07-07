package com.linkedin.metadata.graph.cache.service.rebuild;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.concurrent.Executors;
import org.testng.annotations.Test;

public class GraphCacheRebuilderTest {

  @Test
  public void ensureFreshKeySkipsOnDemandRebuildForScheduledGraphs() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator freshnessEvaluator = mock(SnapshotFreshnessEvaluator.class);
    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            mock(EntityGraphSnapshotBuilder.class),
            mock(OperationContext.class),
            Executors.newSingleThreadExecutor(),
            freshnessEvaluator);

    EntityGraphDefinition scheduled =
        EntityGraphDefinition.builder()
            .graphId("membership")
            .enabled(true)
            .populationStrategy(PopulationStrategy.SCHEDULED)
            .populationIntervalSeconds(600)
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .build();

    rebuilder.ensureFreshKey(
        scheduled, GraphSnapshotSource.GRAPH, "membership@graph", null, TraversalDirection.REVERSE);

    verify(distributedStore, never()).getStatus(any());
    verify(distributedStore, never()).tryClaimRebuild(any(), anyLong());
    verify(freshnessEvaluator, never()).shouldSkipRebuild(any(), any(), any());
  }
}
