package com.linkedin.metadata.graph.cache.service.strategy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.AncestorWalkResult;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.service.read.GraphReadBackend;
import com.linkedin.metadata.graph.cache.service.read.PartialGraphReadBackend;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.mockito.InOrder;
import org.testng.annotations.Test;

public class PartialGraphScopeReadStrategyTest {

  private static final EntityGraphDefinition DEFINITION =
      EntityGraphDefinition.builder()
          .graphId("glossary")
          .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
          .build();
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.GRAPH;
  private static final String ROOT = "urn:li:glossaryNode:root";
  private static final String CHILD = "urn:li:glossaryNode:child";

  @Test
  public void expandCachedMissesFailClosedWhenAnyRootNotActive() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.rootsFrom(Set.of(ROOT, CHILD))).thenReturn(Set.of(ROOT, CHILD));
    when(partialBackend.allRootsActive(DEFINITION, SOURCE, Set.of(ROOT, CHILD))).thenReturn(false);
    when(partialBackend.firstRootMissReason(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT, CHILD)))
        .thenReturn(ReadMissReason.STALE_BLOCKED);

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    GraphReadResult result =
        strategy.expandCached(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT, CHILD), 100, 15);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.STALE_BLOCKED);
    verify(partialBackend, never()).resolveComponentContexts(any(), any(), any());
  }

  @Test
  public void expandCachedMissesWhenComponentsEmpty() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.rootsFrom(Set.of(ROOT))).thenReturn(Set.of(ROOT));
    when(partialBackend.allRootsActive(DEFINITION, SOURCE, Set.of(ROOT))).thenReturn(true);
    when(partialBackend.resolveComponentContexts(DEFINITION, SOURCE, Set.of(ROOT)))
        .thenReturn(List.of());
    when(partialBackend.firstRootMissReason(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT)))
        .thenReturn(ReadMissReason.ABSENT);

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    GraphReadResult result =
        strategy.expandCached(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 15);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.ABSENT);
  }

  @Test
  public void expandCachedReturnsHitWhenRootsActiveAndComponentsResolve() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(List.of()), TraversalCoverage.fullComplete(), "key");
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.rootsFrom(Set.of(ROOT))).thenReturn(Set.of(ROOT));
    when(partialBackend.allRootsActive(DEFINITION, SOURCE, Set.of(ROOT))).thenReturn(true);
    when(partialBackend.resolveComponentContexts(DEFINITION, SOURCE, Set.of(ROOT)))
        .thenReturn(List.of(component));
    when(shared.expandFromComponents(
            eq(DEFINITION),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(ROOT)),
            eq(100),
            eq(15),
            eq(List.of(component))))
        .thenReturn(GraphReadResult.fromVertices(Set.of(ROOT, CHILD)));

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    GraphReadResult result =
        strategy.expandCached(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 15);

    assertTrue(result.isHit());
    assertEquals(result.verticesOrEmpty(), Set.of(ROOT, CHILD));
  }

  @Test
  public void walkCachedWarmsRootBeforeLookup() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build());
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.findCacheKeyForSeeds(DEFINITION.getGraphId(), SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key"));
    when(shared.walkMissFromFreshness(DEFINITION, "key", TraversalDirection.FORWARD))
        .thenReturn(null);
    when(shared.resolveComponent(DEFINITION, "key")).thenReturn(component);
    when(shared.coverageSatisfies(DEFINITION, component.coverage(), TraversalDirection.FORWARD))
        .thenReturn(true);

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    AncestorWalkResult result = strategy.walkCached(DEFINITION, SOURCE, ROOT, 10);

    assertTrue(result.isHit());
    assertEquals(result.ancestorsOrEmpty(), List.of());

    InOrder inOrder = inOrder(partialBackend, shared);
    inOrder
        .verify(partialBackend)
        .ensureFreshForRoot(DEFINITION, SOURCE, ROOT, TraversalDirection.FORWARD);
    inOrder.verify(shared).findCacheKeyForSeeds(DEFINITION.getGraphId(), SOURCE, Set.of(ROOT));
  }

  @Test
  public void expandEphemeralMissesAbsentWhenNoComponentsBuilt() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.rootsFrom(Set.of(ROOT))).thenReturn(Set.of(ROOT));
    when(partialBackend.allRootsCacheFresh(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT)))
        .thenReturn(false);
    when(partialBackend.buildPartialComponentAndWarm(
            DEFINITION, SOURCE, Set.of(ROOT), TraversalDirection.FORWARD))
        .thenReturn(null);

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    GraphReadResult result =
        strategy.expandEphemeral(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 15);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.ABSENT);
  }

  @Test
  public void walkCachedMissesWhenCacheKeyAbsent() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.findCacheKeyForSeeds(DEFINITION.getGraphId(), SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.empty());

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    AncestorWalkResult result = strategy.walkCached(DEFINITION, SOURCE, ROOT, 10);

    assertEquals(((AncestorWalkResult.Miss) result).reason(), ReadMissReason.ABSENT);
  }

  @Test
  public void walkEphemeralUsesFreshCacheWhenAvailable() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build());
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.findCacheKeyForSeeds(DEFINITION.getGraphId(), SOURCE, Set.of(CHILD)))
        .thenReturn(Optional.of("key"));
    when(shared.isFresh(DEFINITION, "key", TraversalDirection.FORWARD)).thenReturn(true);
    when(shared.resolveComponent(DEFINITION, "key")).thenReturn(component);
    when(shared.coverageSatisfies(DEFINITION, component.coverage(), TraversalDirection.FORWARD))
        .thenReturn(true);

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    AncestorWalkResult result = strategy.walkEphemeral(DEFINITION, SOURCE, CHILD, 10);

    assertTrue(result.isHit());
    assertEquals(result.ancestorsOrEmpty(), List.of(ROOT));
  }

  @Test
  public void walkCachedMissesOnFreshnessFailure() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.findCacheKeyForSeeds(DEFINITION.getGraphId(), SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key"));
    when(shared.walkMissFromFreshness(DEFINITION, "key", TraversalDirection.FORWARD))
        .thenReturn(AncestorWalkResult.miss(ReadMissReason.STALE_BLOCKED));

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    AncestorWalkResult result = strategy.walkCached(DEFINITION, SOURCE, ROOT, 10);

    assertEquals(((AncestorWalkResult.Miss) result).reason(), ReadMissReason.STALE_BLOCKED);
  }

  @Test
  public void walkCachedMissesOnInsufficientCoverage() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(List.of()), TraversalCoverage.fullComplete(), "key");
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.findCacheKeyForSeeds(DEFINITION.getGraphId(), SOURCE, Set.of(ROOT)))
        .thenReturn(Optional.of("key"));
    when(shared.walkMissFromFreshness(DEFINITION, "key", TraversalDirection.FORWARD))
        .thenReturn(null);
    when(shared.resolveComponent(DEFINITION, "key")).thenReturn(component);
    when(shared.coverageSatisfies(DEFINITION, component.coverage(), TraversalDirection.FORWARD))
        .thenReturn(false);

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    AncestorWalkResult result = strategy.walkCached(DEFINITION, SOURCE, ROOT, 10);

    assertEquals(((AncestorWalkResult.Miss) result).reason(), ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Test
  public void expandEphemeralUsesCacheWhenAllRootsFresh() {
    PartialGraphReadBackend partialBackend = mock(PartialGraphReadBackend.class);
    GraphReadBackend shared = mock(GraphReadBackend.class);
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(List.of()), TraversalCoverage.fullComplete(), "key");
    when(partialBackend.shared()).thenReturn(shared);
    when(shared.rootsFrom(Set.of(ROOT))).thenReturn(Set.of(ROOT));
    when(partialBackend.allRootsCacheFresh(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT)))
        .thenReturn(true);
    when(partialBackend.resolveComponentContexts(DEFINITION, SOURCE, Set.of(ROOT)))
        .thenReturn(List.of(component));
    when(shared.expandFromComponents(
            eq(DEFINITION),
            eq(TraversalDirection.FORWARD),
            eq(Set.of(ROOT)),
            eq(100),
            eq(15),
            eq(List.of(component))))
        .thenReturn(GraphReadResult.fromVertices(Set.of(ROOT, CHILD)));

    PartialGraphScopeReadStrategy strategy = new PartialGraphScopeReadStrategy(partialBackend);
    GraphReadResult result =
        strategy.expandEphemeral(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of(ROOT), 100, 15);

    assertTrue(result.isHit());
    verify(partialBackend, never()).buildPartialComponentAndWarm(any(), any(), any(), any());
  }

  @Test
  public void expandFromComponentsMissesOnInsufficientCoverage() {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
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

    GraphReadResult result =
        backend.expandFromComponents(
            DEFINITION, TraversalDirection.FORWARD, Set.of(ROOT), 100, 15, List.of(component));

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Test
  public void expandFromComponentsReturnsEmptyHitForReverseLeafWithNoNeighbors() {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(ROOT)
                .destinationUrn("urn:li:glossaryNode:parent")
                .relationshipType("IsPartOf")
                .build());
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");

    GraphReadResult result =
        backend.expandFromComponents(
            DEFINITION, TraversalDirection.REVERSE, Set.of(ROOT), 100, 15, List.of(component));

    assertTrue(result instanceof GraphReadResult.EmptyHit);
    assertTrue(result.verticesOrEmpty().isEmpty());
  }

  @Test
  public void expandFromComponentsMissesWhenPerCallLimitExceeded() {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn("urn:li:glossaryNode:grandchild")
                .destinationUrn(CHILD)
                .relationshipType("IsPartOf")
                .build());
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");

    GraphReadResult result =
        backend.expandFromComponents(
            DEFINITION, TraversalDirection.REVERSE, Set.of(ROOT), 2, 15, List.of(component));

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.TRUNCATED);
  }

  @Test
  public void expandFromComponentsReturnsHitForForwardAncestors() {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build());
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");

    GraphReadResult result =
        backend.expandFromComponents(
            DEFINITION, TraversalDirection.FORWARD, Set.of(CHILD), 100, 15, List.of(component));

    assertTrue(result.isHit());
    assertEquals(result.verticesOrEmpty(), Set.of(CHILD, ROOT));
  }

  @Test
  public void expandFromComponentsWalksFullSnapshotWhenUseDefinitionMaxDepth() {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
    String level1 = "urn:li:domain:l1";
    String level2 = "urn:li:domain:l2";
    String level3 = "urn:li:domain:l3";
    String level4 = "urn:li:domain:l4";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(level1)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(level2)
                .destinationUrn(level1)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(level3)
                .destinationUrn(level2)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(level4)
                .destinationUrn(level3)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphDefinition shallowBuildDefinition =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(0).build())
            .build();
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");

    GraphReadResult result =
        backend.expandFromComponents(
            shallowBuildDefinition,
            TraversalDirection.REVERSE,
            Set.of(ROOT),
            100,
            com.linkedin.metadata.graph.cache.EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            List.of(component));

    assertTrue(result.isHit());
    assertEquals(result.verticesOrEmpty(), Set.of(ROOT, level1, level2, level3, level4));
  }

  @Test
  public void expandFromComponentsPartialReadCapsAtConfiguredMaxDepth() {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
    String d1 = "urn:li:glossaryNode:d1";
    String d2 = "urn:li:glossaryNode:d2";
    String d3 = "urn:li:glossaryNode:d3";
    String d4 = "urn:li:glossaryNode:d4";
    List<DirectedEdge> edges =
        List.of(
            DirectedEdge.builder()
                .sourceUrn(d1)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(d2)
                .destinationUrn(d1)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(d3)
                .destinationUrn(d2)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(d4)
                .destinationUrn(d3)
                .relationshipType("IsPartOf")
                .build());
    EntityGraphDefinition partialDepth2 =
        EntityGraphDefinition.builder()
            .graphId("glossary")
            .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(2).build())
            .build();
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(edges), TraversalCoverage.fullComplete(), "key");

    GraphReadResult result =
        backend.expandFromComponents(
            partialDepth2,
            TraversalDirection.REVERSE,
            Set.of(ROOT),
            100,
            EntityGraphCache.USE_DEFINITION_MAX_DEPTH,
            List.of(component));

    assertTrue(result.isHit());
    assertEquals(result.verticesOrEmpty(), Set.of(ROOT, d1, d2));
  }
}
