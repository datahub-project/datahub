package com.linkedin.metadata.graph.cache.service.read;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.service.strategy.FullGraphScopeReadStrategy;
import com.linkedin.metadata.graph.cache.service.strategy.PartialGraphScopeReadStrategy;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphLocalViewCache;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StaleSnapshotReadTest {

  private static final String ROOT = "urn:li:container:root";
  private static final String CHILD = "urn:li:container:child";
  private static final String PARTIAL_KEY = "container-component";
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.GRAPH;

  private EntityGraphDistributedStore store;
  private EntityGraphLocalViewCache localViews;
  private GraphReadBackend backend;
  private PartialGraphReadBackend partialBackend;

  @BeforeMethod
  public void setUp() {
    store = mock(EntityGraphDistributedStore.class);
    localViews = mock(EntityGraphLocalViewCache.class);
    OperationContext operationContext = mock(OperationContext.class);
    when(operationContext.getMetricUtils()).thenReturn(Optional.empty());
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(store);
    GraphCacheRebuilder rebuilder = mock(GraphCacheRebuilder.class);
    backend =
        new GraphReadBackend(
            store,
            localViews,
            mock(EntityGraphSnapshotBuilder.class),
            operationContext,
            evaluator,
            rebuilder);
    partialBackend =
        new PartialGraphReadBackend(
            backend,
            store,
            mock(EntityGraphSnapshotBuilder.class),
            operationContext,
            evaluator,
            rebuilder);
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(CHILD)
                    .destinationUrn(ROOT)
                    .relationshipType("IsPartOf")
                    .build()));
    when(localViews.get(anyString(), anyLong())).thenReturn(Optional.of(view));
    when(store.findCacheKeyForSeeds(anyString(), any(), any(), any()))
        .thenReturn(Optional.of(PARTIAL_KEY));
  }

  @Test
  public void partialExpandServesStaleActiveSnapshot() {
    stubPartialSnapshot(CacheStatus.ACTIVE, TraversalCoverage.fullComplete());

    GraphReadResult result =
        partialStrategy()
            .expandCached(
                partialDefinition(), SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 12);

    assertTrue(result.isHit());
    assertTrue(result.verticesOrEmpty().contains(ROOT));
  }

  @Test
  public void partialExpandServesSnapshotWhileBuilding() {
    stubPartialSnapshot(CacheStatus.BUILDING, TraversalCoverage.fullComplete());

    GraphReadResult result =
        partialStrategy()
            .expandCached(
                partialDefinition(), SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 12);

    assertTrue(result.isHit());
  }

  @Test
  public void partialWalkServesStaleActiveSnapshot() {
    stubPartialSnapshot(CacheStatus.ACTIVE, TraversalCoverage.fullComplete());

    AncestorWalkResult result =
        partialStrategy().walkCached(partialDefinition(), SOURCE, CHILD, 12);

    assertTrue(result.isHit());
  }

  @Test
  public void partialExpandMissesTombstone() {
    when(store.getStatus(PARTIAL_KEY)).thenReturn(CacheStatus.COOLDOWN);

    GraphReadResult result =
        partialStrategy()
            .expandCached(
                partialDefinition(), SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 12);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.TOMBSTONE);
  }

  @Test
  public void partialExpandMissesInsufficientCoverage() {
    stubPartialSnapshot(CacheStatus.ACTIVE, TraversalCoverage.incomplete());

    GraphReadResult result =
        partialStrategy()
            .expandCached(
                partialDefinition(), SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 12);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Test
  public void scheduledFullExpandServesStaleActiveSnapshot() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    stubSnapshot(cacheKey, CacheStatus.ACTIVE, TraversalCoverage.fullComplete(), "domain");

    GraphReadResult result =
        new FullGraphScopeReadStrategy(backend)
            .expandCached(
                fullDefinition(PopulationStrategy.SCHEDULED, RebuildExecution.SYNC),
                GraphSnapshotSource.SEARCH,
                TraversalDirection.FORWARD,
                Set.of(ROOT),
                100,
                0);

    assertTrue(result.isHit());
  }

  @Test
  public void scheduledFullWalkServesStaleActiveSnapshot() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    stubSnapshot(cacheKey, CacheStatus.ACTIVE, TraversalCoverage.fullComplete(), "domain");

    AncestorWalkResult result =
        new FullGraphScopeReadStrategy(backend)
            .walkCached(
                fullDefinition(PopulationStrategy.SCHEDULED, RebuildExecution.SYNC),
                GraphSnapshotSource.SEARCH,
                CHILD,
                0);

    assertTrue(result.isHit());
  }

  @Test
  public void backgroundFullExpandStillMissesStaleSnapshot() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    stubSnapshot(cacheKey, CacheStatus.ACTIVE, TraversalCoverage.fullComplete(), "domain");

    GraphReadResult result =
        new FullGraphScopeReadStrategy(backend)
            .expandCached(
                fullDefinition(PopulationStrategy.LAZY, RebuildExecution.BACKGROUND),
                GraphSnapshotSource.SEARCH,
                TraversalDirection.FORWARD,
                Set.of(ROOT),
                100,
                0);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.STALE_BLOCKED);
  }

  @Test
  public void scheduledFullExpandServesSnapshotWhileBuilding() {
    String cacheKey = EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);
    stubSnapshot(cacheKey, CacheStatus.BUILDING, TraversalCoverage.fullComplete(), "domain");

    GraphReadResult result =
        new FullGraphScopeReadStrategy(backend)
            .expandCached(
                fullDefinition(PopulationStrategy.SCHEDULED, RebuildExecution.SYNC),
                GraphSnapshotSource.SEARCH,
                TraversalDirection.FORWARD,
                Set.of(ROOT),
                100,
                0);

    assertTrue(result.isHit());
  }

  private PartialGraphScopeReadStrategy partialStrategy() {
    return new PartialGraphScopeReadStrategy(partialBackend);
  }

  private void stubPartialSnapshot(CacheStatus status, TraversalCoverage coverage) {
    stubSnapshot(PARTIAL_KEY, status, coverage, "container");
  }

  private void stubSnapshot(
      String cacheKey, CacheStatus status, TraversalCoverage coverage, String graphId) {
    when(store.getStatus(cacheKey)).thenReturn(status);
    when(store.hasServableSnapshot(cacheKey)).thenReturn(true);
    when(store.getSnapshot(cacheKey))
        .thenReturn(
            EntityGraphSnapshot.builder()
                .graphId(graphId)
                .cacheKey(cacheKey)
                .builtAtMillis(System.currentTimeMillis() - 120_000L)
                .generation(1L)
                .vertexCount(2)
                .edgeCount(1)
                .topologyFingerprint("fp")
                .traversalCoverage(coverage)
                .build());
  }

  private static EntityGraphDefinition partialDefinition() {
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

  private static EntityGraphDefinition fullDefinition(
      PopulationStrategy strategy, RebuildExecution rebuildExecution) {
    return EntityGraphDefinition.builder()
        .graphId("domain")
        .enabled(true)
        .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
        .populationStrategy(strategy)
        .rebuildExecution(rebuildExecution)
        .populationIntervalSeconds(60)
        .buildSource(GraphSnapshotSource.SEARCH)
        .build();
  }
}
