package com.linkedin.metadata.graph.cache.service.rebuild;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
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

  @Test
  public void stalePartialKeyEnqueuesPartialRebuildOffTheCaller() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    GraphScopeRebuildStrategy fullRebuildStrategy = mock(GraphScopeRebuildStrategy.class);
    List<Runnable> queued = new ArrayList<>();
    ExecutorService executor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              queued.add(invocation.getArgument(0));
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));

    EntityGraphDefinition definition = partialContainerDefinition();
    EntityGraphSnapshot existing =
        componentSnapshot("component-key", System.currentTimeMillis() - 120_000L);
    when(distributedStore.getStatus("component-key")).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot("component-key")).thenReturn(existing);
    when(distributedStore.hasServableSnapshot("component-key")).thenReturn(true);
    when(distributedStore.tryClaimRebuild(eq("component-key"), anyLong())).thenReturn(true, false);
    when(snapshotBuilder.buildPartial(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.ACTIVE)
                .snapshot(componentSnapshot("component-key", System.currentTimeMillis()))
                .build());

    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            snapshotBuilder,
            mock(OperationContext.class),
            executor,
            mock(SnapshotFreshnessEvaluator.class),
            fullRebuildStrategy,
            new PartialGraphRebuildStrategy(
                distributedStore, snapshotBuilder, mock(OperationContext.class)));

    rebuilder.ensureFreshKey(
        definition,
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);
    rebuilder.ensureFreshKey(
        definition,
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);

    assertEquals(queued.size(), 1);
    verify(snapshotBuilder, never())
        .buildPartial(any(), any(), any(), any(), any(), any(), any(), any());
    verify(fullRebuildStrategy, never()).executeRebuild(any(), any(), any(), any(), any());

    queued.get(0).run();

    verify(snapshotBuilder).buildPartial(any(), any(), any(), any(), any(), any(), any(), any());
    verify(fullRebuildStrategy, never()).executeRebuild(any(), any(), any(), any(), any());
  }

  @Test
  public void missingPartialKeyRebuildsSynchronouslyAndStaysPartial() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    GraphScopeRebuildStrategy fullRebuildStrategy = mock(GraphScopeRebuildStrategy.class);
    ExecutorService executor = mock(ExecutorService.class);
    when(distributedStore.getStatus("component-key")).thenReturn(CacheStatus.ABSENT);
    when(distributedStore.getSnapshot("component-key")).thenReturn(null);
    when(distributedStore.tryClaimRebuild(eq("component-key"), anyLong())).thenReturn(true);
    when(snapshotBuilder.buildPartial(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.ACTIVE)
                .snapshot(componentSnapshot("component-key", System.currentTimeMillis()))
                .build());

    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            snapshotBuilder,
            mock(OperationContext.class),
            executor,
            mock(SnapshotFreshnessEvaluator.class),
            fullRebuildStrategy,
            new PartialGraphRebuildStrategy(
                distributedStore, snapshotBuilder, mock(OperationContext.class)));

    rebuilder.ensureFreshKey(
        partialContainerDefinition(),
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);

    verify(executor, never()).execute(any(Runnable.class));
    verify(snapshotBuilder).buildPartial(any(), any(), any(), any(), any(), any(), any(), any());
    verify(fullRebuildStrategy, never()).executeRebuild(any(), any(), any(), any(), any());
  }

  @Test
  public void tombstonePartialKeyRebuildsSynchronously() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    ExecutorService executor = mock(ExecutorService.class);
    when(distributedStore.getStatus("component-key")).thenReturn(CacheStatus.COOLDOWN);
    when(distributedStore.getSnapshot("component-key"))
        .thenReturn(componentSnapshot("component-key", System.currentTimeMillis() - 120_000L));
    when(distributedStore.hasServableSnapshot("component-key")).thenReturn(false);
    when(distributedStore.tryClaimRebuild(eq("component-key"), anyLong())).thenReturn(true);
    when(snapshotBuilder.buildPartial(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.ACTIVE)
                .snapshot(componentSnapshot("component-key", System.currentTimeMillis()))
                .build());

    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            snapshotBuilder,
            mock(OperationContext.class),
            executor,
            mock(SnapshotFreshnessEvaluator.class),
            mock(GraphScopeRebuildStrategy.class),
            new PartialGraphRebuildStrategy(
                distributedStore, snapshotBuilder, mock(OperationContext.class)));

    rebuilder.ensureFreshKey(
        partialContainerDefinition(),
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);

    verify(executor, never()).execute(any(Runnable.class));
    verify(snapshotBuilder).buildPartial(any(), any(), any(), any(), any(), any(), any(), any());
  }

  @Test
  public void configExceptionOnStaleRefreshSuppressesAnotherRebuild() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    List<Runnable> queued = new ArrayList<>();
    ExecutorService executor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              queued.add(invocation.getArgument(0));
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));
    when(distributedStore.getStatus("component-key")).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot("component-key"))
        .thenReturn(componentSnapshot("component-key", System.currentTimeMillis() - 120_000L));
    when(distributedStore.hasServableSnapshot("component-key")).thenReturn(true);
    when(distributedStore.tryClaimRebuild(eq("component-key"), anyLong())).thenReturn(true);
    doThrow(new IllegalArgumentException("invalid configuration"))
        .when(snapshotBuilder)
        .buildPartial(any(), any(), any(), any(), any(), any(), any(), any());

    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            snapshotBuilder,
            mock(OperationContext.class),
            executor,
            mock(SnapshotFreshnessEvaluator.class),
            mock(GraphScopeRebuildStrategy.class),
            new PartialGraphRebuildStrategy(
                distributedStore, snapshotBuilder, mock(OperationContext.class)));

    rebuilder.ensureFreshKey(
        partialContainerDefinition(),
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);
    assertEquals(queued.size(), 1);
    queued.get(0).run();

    verify(distributedStore).suppressRebuildKeepingSnapshot("component-key", CacheStatus.INVALID);
    verify(distributedStore, never()).releaseRebuildClaim(any());
    verify(distributedStore, never()).markInvalid(any());
  }

  @Test
  public void partialRebuildWithoutSeedsDoesNotCallFullStrategy() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    GraphScopeRebuildStrategy fullRebuildStrategy = mock(GraphScopeRebuildStrategy.class);
    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            mock(EntityGraphSnapshotBuilder.class),
            mock(OperationContext.class),
            mock(ExecutorService.class),
            mock(SnapshotFreshnessEvaluator.class),
            fullRebuildStrategy,
            new PartialGraphRebuildStrategy(
                distributedStore,
                mock(EntityGraphSnapshotBuilder.class),
                mock(OperationContext.class)));

    rebuilder.executeRebuild(
        partialContainerDefinition(),
        GraphSnapshotSource.GRAPH,
        null,
        "component-key",
        TraversalDirection.FORWARD);

    verify(fullRebuildStrategy, never()).executeRebuild(any(), any(), any(), any(), any());
    verify(distributedStore).markInvalid(any());
  }

  @Test
  public void scheduledRebuildDoesNotRunForPartialGraphs() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            mock(EntityGraphSnapshotBuilder.class),
            mock(OperationContext.class),
            Executors.newSingleThreadExecutor(),
            mock(SnapshotFreshnessEvaluator.class));

    rebuilder.scheduledRebuild(partialContainerDefinition());

    verify(distributedStore, never()).tryClaimRebuild(any(), anyLong());
  }

  @Test
  public void failedStaleWhileRevalidateRebuildKeepsServableSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    List<Runnable> queued = new ArrayList<>();
    ExecutorService executor = mock(ExecutorService.class);
    doAnswer(
            invocation -> {
              queued.add(invocation.getArgument(0));
              return null;
            })
        .when(executor)
        .execute(any(Runnable.class));

    when(distributedStore.getStatus("component-key")).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot("component-key"))
        .thenReturn(componentSnapshot("component-key", System.currentTimeMillis() - 120_000L));
    when(distributedStore.hasServableSnapshot("component-key")).thenReturn(true);
    when(distributedStore.tryClaimRebuild(eq("component-key"), anyLong())).thenReturn(true);
    when(snapshotBuilder.buildPartial(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.COOLDOWN)
                .failureReason("graph_failed")
                .build());

    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            snapshotBuilder,
            mock(OperationContext.class),
            executor,
            mock(SnapshotFreshnessEvaluator.class),
            mock(GraphScopeRebuildStrategy.class),
            new PartialGraphRebuildStrategy(
                distributedStore, snapshotBuilder, mock(OperationContext.class)));

    rebuilder.ensureFreshKey(
        partialContainerDefinition(),
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);
    assertEquals(queued.size(), 1);
    queued.get(0).run();

    verify(distributedStore).suppressRebuildKeepingSnapshot("component-key", CacheStatus.COOLDOWN);
    verify(distributedStore, never()).markCooldown(any());
    verify(distributedStore, never()).markOverLimit(any());
    verify(distributedStore, never()).markInvalid(any());
  }

  @Test
  public void failedSyncRebuildStillTombstonesServableSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    EntityGraphSnapshotBuilder snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    when(distributedStore.getStatus("component-key")).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot("component-key"))
        .thenReturn(componentSnapshot("component-key", System.currentTimeMillis() - 120_000L));
    when(distributedStore.hasServableSnapshot("component-key")).thenReturn(true);
    when(distributedStore.tryClaimRebuild(eq("component-key"), anyLong())).thenReturn(true);
    when(snapshotBuilder.buildPartial(any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.COOLDOWN)
                .failureReason("graph_failed")
                .build());

    EntityGraphDefinition sync =
        EntityGraphDefinition.builder()
            .graphId("container")
            .enabled(true)
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(12).build())
            .populationStrategy(PopulationStrategy.LAZY)
            .rebuildExecution(RebuildExecution.SYNC)
            .populationIntervalSeconds(60)
            .buildSource(GraphSnapshotSource.GRAPH)
            .build();
    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            snapshotBuilder,
            mock(OperationContext.class),
            mock(ExecutorService.class),
            mock(SnapshotFreshnessEvaluator.class),
            mock(GraphScopeRebuildStrategy.class),
            new PartialGraphRebuildStrategy(
                distributedStore, snapshotBuilder, mock(OperationContext.class)));

    rebuilder.ensureFreshKey(
        sync,
        GraphSnapshotSource.GRAPH,
        "component-key",
        Set.of("urn:li:container:root"),
        TraversalDirection.FORWARD);

    verify(distributedStore).markCooldown("component-key");
    verify(distributedStore, never()).suppressRebuildKeepingSnapshot(any(), any());
  }

  @Test
  public void failedScheduledRebuildKeepsServableSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    GraphScopeRebuildStrategy fullRebuildStrategy = mock(GraphScopeRebuildStrategy.class);
    SnapshotFreshnessEvaluator freshnessEvaluator = mock(SnapshotFreshnessEvaluator.class);
    when(freshnessEvaluator.shouldSkipRebuild(any(), any())).thenReturn(false);
    when(distributedStore.tryClaimRebuild(eq("domain@search"), anyLong())).thenReturn(true);
    when(distributedStore.hasServableSnapshot("domain@search")).thenReturn(true);
    when(fullRebuildStrategy.executeRebuild(any(), any(), any(), any(), any()))
        .thenReturn(
            BuildResult.builder()
                .status(CacheStatus.COOLDOWN)
                .failureReason("search_failed")
                .build());

    EntityGraphDefinition scheduled =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .enabled(true)
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .populationStrategy(PopulationStrategy.SCHEDULED)
            .populationIntervalSeconds(60)
            .buildSource(GraphSnapshotSource.SEARCH)
            .build();
    GraphCacheRebuilder rebuilder =
        new GraphCacheRebuilder(
            distributedStore,
            mock(EntityGraphLocalViewCache.class),
            mock(EntityGraphSnapshotBuilder.class),
            mock(OperationContext.class),
            mock(ExecutorService.class),
            freshnessEvaluator,
            fullRebuildStrategy,
            mock(GraphScopeRebuildStrategy.class));

    rebuilder.scheduledRebuild(scheduled);

    verify(distributedStore).suppressRebuildKeepingSnapshot("domain@search", CacheStatus.COOLDOWN);
    verify(distributedStore, never()).markCooldown(any());
  }

  @Test
  public void warmRebuildThatLosesToANewerSnapshotReleasesItsOwnClaim() {
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
    String cacheKey = "domain@graph";
    EntityGraphDefinition definition =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .enabled(true)
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .populationStrategy(PopulationStrategy.LAZY)
            .rebuildExecution(RebuildExecution.STALE_WHILE_REVALIDATE)
            .populationIntervalSeconds(60)
            .buildSource(GraphSnapshotSource.GRAPH)
            .build();
    when(distributedStore.tryClaimRebuild(eq(cacheKey), anyLong())).thenReturn(true);
    when(distributedStore.publish(any(), any())).thenReturn(false);

    rebuilder.warmCacheIfMissingOrStale(
        definition, componentSnapshot(cacheKey, 1_000L), TraversalDirection.FORWARD, null);

    verify(distributedStore).releaseRebuildClaim(cacheKey);
  }

  private static EntityGraphDefinition partialContainerDefinition() {
    return EntityGraphDefinition.builder()
        .graphId("container")
        .enabled(true)
        .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(12).build())
        .populationStrategy(PopulationStrategy.LAZY)
        .rebuildExecution(RebuildExecution.STALE_WHILE_REVALIDATE)
        .populationIntervalSeconds(60)
        .buildSource(GraphSnapshotSource.GRAPH)
        .build();
  }

  private static EntityGraphSnapshot componentSnapshot(String cacheKey, long builtAtMillis) {
    return EntityGraphSnapshot.builder()
        .graphId("container")
        .cacheKey(cacheKey)
        .builtAtMillis(builtAtMillis)
        .generation(1L)
        .vertexCount(1)
        .edgeCount(0)
        .topologyFingerprint("fp")
        .traversalCoverage(TraversalCoverage.fullComplete())
        .edges(List.of())
        .build();
  }
}
