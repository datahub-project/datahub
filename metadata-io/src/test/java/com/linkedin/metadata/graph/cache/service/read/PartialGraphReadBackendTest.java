package com.linkedin.metadata.graph.cache.service.read;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.freshness.SnapshotFreshnessEvaluator;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.rebuild.GraphCacheRebuilder;
import com.linkedin.metadata.graph.cache.service.strategy.PartialGraphScopeStrategy;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotBuilder.BuildResult;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PartialGraphReadBackendTest {

  private static final String GRAPH_ID = "glossary";
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.GRAPH;
  private static final String ROOT = "urn:li:glossaryNode:root";
  private static final String CHILD = "urn:li:glossaryNode:child";
  private static final EntityGraphDefinition DEFINITION =
      EntityGraphDefinition.builder()
          .graphId(GRAPH_ID)
          .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
          .build();

  private GraphReadBackend shared;
  private EntityGraphDistributedStore distributedStore;
  private EntityGraphSnapshotBuilder snapshotBuilder;
  private OperationContext systemOperationContext;
  private SnapshotFreshnessEvaluator freshnessEvaluator;
  private GraphCacheRebuilder rebuilder;
  private PartialGraphReadBackend backend;

  @BeforeMethod
  public void setUp() {
    shared = mock(GraphReadBackend.class);
    distributedStore = mock(EntityGraphDistributedStore.class);
    snapshotBuilder = mock(EntityGraphSnapshotBuilder.class);
    systemOperationContext = mock(OperationContext.class);
    freshnessEvaluator = mock(SnapshotFreshnessEvaluator.class);
    rebuilder = mock(GraphCacheRebuilder.class);
    backend =
        new PartialGraphReadBackend(
            shared,
            distributedStore,
            snapshotBuilder,
            systemOperationContext,
            freshnessEvaluator,
            rebuilder);
  }

  @Test
  public void ensureFreshForRootUsesExistingKey() {
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("cache-key"));

    backend.ensureFreshForRoot(DEFINITION, SOURCE, ROOT, TraversalDirection.FORWARD);

    verify(rebuilder)
        .ensureFreshKey(DEFINITION, SOURCE, "cache-key", Set.of(ROOT), TraversalDirection.FORWARD);
    verify(distributedStore, never()).tryClaimRebuild(any(), any(Long.class));
  }

  @Test
  public void ensureFreshForRootSkipsWhenFailureMarkerInCooldown() {
    String markerKey = PartialGraphScopeStrategy.failureMarkerKey(GRAPH_ID, SOURCE, ROOT);
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT))).thenReturn(Optional.empty());
    when(freshnessEvaluator.shouldSkipRebuild(markerKey, DEFINITION)).thenReturn(true);

    backend.ensureFreshForRoot(DEFINITION, SOURCE, ROOT, TraversalDirection.FORWARD);

    verify(distributedStore, never()).tryClaimRebuild(any(), any(Long.class));
    verify(rebuilder, never()).executeRebuild(any(), any(), any(), any(), any());
  }

  @Test
  public void ensureFreshForRootClaimsAndRebuildsWhenMissing() {
    String markerKey = PartialGraphScopeStrategy.failureMarkerKey(GRAPH_ID, SOURCE, ROOT);
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT))).thenReturn(Optional.empty());
    when(freshnessEvaluator.shouldSkipRebuild(markerKey, DEFINITION)).thenReturn(false);
    when(distributedStore.tryClaimRebuild(eq(markerKey), any(Long.class))).thenReturn(true);

    backend.ensureFreshForRoot(DEFINITION, SOURCE, ROOT, TraversalDirection.FORWARD);

    verify(rebuilder)
        .executeRebuild(DEFINITION, SOURCE, Set.of(ROOT), null, TraversalDirection.FORWARD);
  }

  @Test
  public void allRootsCacheFreshRequiresEveryRootFresh() {
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key-root"));
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(CHILD)))
        .thenReturn(Optional.of("key-child"));
    when(freshnessEvaluator.isFresh(DEFINITION, "key-root", TraversalDirection.FORWARD))
        .thenReturn(true);
    when(freshnessEvaluator.isFresh(DEFINITION, "key-child", TraversalDirection.FORWARD))
        .thenReturn(false);

    assertFalse(
        backend.allRootsCacheFresh(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT, CHILD)));
  }

  @Test
  public void allRootsActiveRequiresActiveStatusForEachRoot() {
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key-root"));
    when(distributedStore.getStatus("key-root")).thenReturn(CacheStatus.ACTIVE);

    assertTrue(backend.allRootsActive(DEFINITION, SOURCE, Set.of(ROOT)));
    assertFalse(backend.allRootsActive(DEFINITION, SOURCE, Set.of(ROOT, CHILD)));
  }

  @Test
  public void resolveComponentContextsDedupesSharedCacheKeys() {
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(List.of()), TraversalCoverage.fullComplete(), "shared-key");
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("shared-key"));
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(CHILD)))
        .thenReturn(Optional.of("shared-key"));
    when(shared.resolveComponent(DEFINITION, "shared-key")).thenReturn(component);

    List<GraphComponentContext> contexts =
        backend.resolveComponentContexts(DEFINITION, SOURCE, Set.of(ROOT, CHILD));

    assertEquals(contexts.size(), 1);
    assertEquals(contexts.get(0).cacheKey(), "shared-key");
  }

  @Test
  public void resolveComponentContextsReturnsEmptyWhenAnyRootMisses() {
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key-root"));
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(CHILD))).thenReturn(Optional.empty());

    assertTrue(backend.resolveComponentContexts(DEFINITION, SOURCE, Set.of(ROOT, CHILD)).isEmpty());
  }

  @Test
  public void firstRootMissReasonReportsAbsentWhenKeyMissing() {
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT))).thenReturn(Optional.empty());

    assertEquals(
        backend.firstRootMissReason(DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT)),
        ReadMissReason.ABSENT);
  }

  @Test
  public void firstRootMissReasonReportsStaleFromFreshness() {
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key"));
    when(shared.missReasonFromFreshness(DEFINITION, "key", TraversalDirection.FORWARD))
        .thenReturn(ReadMissReason.STALE_BLOCKED);

    assertEquals(
        backend.firstRootMissReason(DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT)),
        ReadMissReason.STALE_BLOCKED);
  }

  @Test
  public void firstRootMissReasonReportsInsufficientCoverage() {
    TraversalCoverage incomplete =
        TraversalCoverage.builder()
            .direction(
                TraversalCoverage.DirectionCoverage.builder()
                    .direction(TraversalDirection.FORWARD)
                    .explored(true)
                    .complete(false)
                    .exploredDepth(1)
                    .configuredMaxDepth(15)
                    .build())
            .build();
    GraphComponentContext component =
        new GraphComponentContext(new EntityGraphView(List.of()), incomplete, "key");
    when(shared.findCacheKeyForSeeds(GRAPH_ID, SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key"));
    when(shared.missReasonFromFreshness(DEFINITION, "key", TraversalDirection.FORWARD))
        .thenReturn(null);
    when(shared.resolveComponent(DEFINITION, "key")).thenReturn(component);

    assertEquals(
        backend.firstRootMissReason(DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT)),
        ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Test
  public void buildPartialComponentAndWarmReturnsNullOnFailedBuild() {
    BuildResult failed =
        BuildResult.builder().status(CacheStatus.COOLDOWN).failureReason("graph_failed").build();
    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(DEFINITION),
            eq(SOURCE),
            eq(Set.of(ROOT)),
            eq(TraversalDirection.FORWARD),
            eq(null),
            eq(null),
            eq(null)))
        .thenReturn(failed);

    assertNull(
        backend.buildPartialComponentAndWarm(
            DEFINITION, SOURCE, Set.of(ROOT), TraversalDirection.FORWARD));
  }

  @Test
  public void buildPartialComponentAndWarmWarmsAndReturnsComponent() {
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId(GRAPH_ID)
            .cacheKey("key")
            .generation(1L)
            .buildSource("graph")
            .builtAtMillis(System.currentTimeMillis())
            .edges(edges)
            .vertexCount(2)
            .edgeCount(1)
            .topologyFingerprint("fp")
            .traversalCoverage(TraversalCoverage.fullComplete())
            .build();
    BuildResult success =
        BuildResult.builder().status(CacheStatus.ACTIVE).snapshot(snapshot).build();
    when(snapshotBuilder.buildPartial(
            eq(systemOperationContext),
            eq(DEFINITION),
            eq(SOURCE),
            eq(Set.of(ROOT)),
            eq(TraversalDirection.FORWARD),
            eq(null),
            eq(null),
            eq(null)))
        .thenReturn(success);

    GraphComponentContext component =
        backend.buildPartialComponentAndWarm(
            DEFINITION, SOURCE, Set.of(ROOT), TraversalDirection.FORWARD);

    assertTrue(component != null);
    assertEquals(component.cacheKey(), "key");
    verify(rebuilder)
        .warmCacheIfMissingOrStale(DEFINITION, snapshot, TraversalDirection.FORWARD, Set.of(ROOT));
  }
}
