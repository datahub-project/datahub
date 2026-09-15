package com.linkedin.metadata.graph.cache.service.strategy;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
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
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class FullGraphScopeReadStrategyTest {

  private static final EntityGraphDefinition DEFINITION =
      EntityGraphDefinition.builder()
          .graphId("domain")
          .enabled(true)
          .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
          .build();
  private static final GraphSnapshotSource SOURCE = GraphSnapshotSource.SEARCH;
  private static final String CACHE_KEY =
      EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH);

  @Test
  public void expandCachedMissesWhenFreshnessCheckFails() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    when(backend.missFromFreshness(eq(DEFINITION), eq(CACHE_KEY), eq(TraversalDirection.FORWARD)))
        .thenReturn(GraphReadResult.miss(ReadMissReason.STALE_BLOCKED));

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    GraphReadResult result =
        strategy.expandCached(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of("urn:li:domain:root"), 100, 15);

    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.STALE_BLOCKED);
    verify(backend)
        .ensureFreshKey(
            eq(DEFINITION), eq(SOURCE), eq(CACHE_KEY), eq(null), eq(TraversalDirection.FORWARD));
  }

  @Test
  public void expandCachedMissesWhenComponentAbsent() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    when(backend.missFromFreshness(eq(DEFINITION), eq(CACHE_KEY), any())).thenReturn(null);
    when(backend.resolveComponent(DEFINITION, CACHE_KEY)).thenReturn(null);

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    GraphReadResult result =
        strategy.expandCached(
            DEFINITION, SOURCE, TraversalDirection.REVERSE, Set.of("urn:li:domain:root"), 100, 15);

    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.ABSENT);
  }

  @Test
  public void expandEphemeralReturnsCacheHitWhenFresh() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    when(backend.tryExpandFromFreshCache(
            eq(DEFINITION),
            eq(CACHE_KEY),
            eq(TraversalDirection.FORWARD),
            eq(Set.of("urn:li:domain:root")),
            eq(100),
            eq(15)))
        .thenReturn(GraphReadResult.fromVertices(Set.of("urn:li:domain:root")));

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    GraphReadResult result =
        strategy.expandEphemeral(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of("urn:li:domain:root"), 100, 15);

    assertTrue(result.isHit());
  }

  @Test
  public void expandEphemeralFallsBackToLiveBuildOnCacheMiss() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    when(backend.tryExpandFromFreshCache(any(), any(), any(), any(), anyInt(), anyInt()))
        .thenReturn(null);
    when(backend.expandEphemeralFromLiveFullBuild(
            eq(DEFINITION),
            eq(SOURCE),
            eq(TraversalDirection.FORWARD),
            eq(Set.of("urn:li:domain:root")),
            eq(100),
            eq(15)))
        .thenReturn(GraphReadResult.fromVertices(Set.of("urn:li:domain:child")));

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    GraphReadResult result =
        strategy.expandEphemeral(
            DEFINITION, SOURCE, TraversalDirection.FORWARD, Set.of("urn:li:domain:root"), 100, 15);

    assertEquals(result.verticesOrEmpty(), Set.of("urn:li:domain:child"));
  }

  @Test
  public void walkCachedMissesOnInsufficientCoverage() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    GraphComponentContext component =
        new GraphComponentContext(
            new EntityGraphView(List.of()), TraversalCoverage.fullComplete(), CACHE_KEY);
    when(backend.walkMissFromFreshness(
            eq(DEFINITION), eq(CACHE_KEY), eq(TraversalDirection.FORWARD)))
        .thenReturn(null);
    when(backend.resolveComponent(DEFINITION, CACHE_KEY)).thenReturn(component);
    when(backend.coverageSatisfies(
            eq(DEFINITION), eq(component.coverage()), eq(TraversalDirection.FORWARD)))
        .thenReturn(false);

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    AncestorWalkResult result = strategy.walkCached(DEFINITION, SOURCE, "urn:li:domain:child", 10);

    assertTrue(result.isMiss());
    assertEquals(((AncestorWalkResult.Miss) result).reason(), ReadMissReason.INSUFFICIENT_COVERAGE);
  }

  @Test
  public void walkCachedResolvesUseDefinitionMaxDepth() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    EntityGraphView view = mock(EntityGraphView.class);
    GraphComponentContext component =
        new GraphComponentContext(view, TraversalCoverage.fullComplete(), CACHE_KEY);
    when(backend.walkMissFromFreshness(
            eq(DEFINITION), eq(CACHE_KEY), eq(TraversalDirection.FORWARD)))
        .thenReturn(null);
    when(backend.resolveComponent(DEFINITION, CACHE_KEY)).thenReturn(component);
    when(backend.coverageSatisfies(
            eq(DEFINITION), eq(component.coverage()), eq(TraversalDirection.FORWARD)))
        .thenReturn(true);
    when(view.orderedForwardAncestors(eq("urn:li:domain:child"), eq(Integer.MAX_VALUE)))
        .thenReturn(List.of("urn:li:domain:root"));

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    AncestorWalkResult result =
        strategy.walkCached(
            DEFINITION, SOURCE, "urn:li:domain:child", EntityGraphCache.USE_DEFINITION_MAX_DEPTH);

    assertTrue(result.isHit());
    assertEquals(result.ancestorsOrEmpty(), List.of("urn:li:domain:root"));
    verify(view).orderedForwardAncestors("urn:li:domain:child", Integer.MAX_VALUE);
  }

  @Test
  public void walkEphemeralReturnsFromFreshCacheWhenAvailable() {
    GraphReadBackend backend = mock(GraphReadBackend.class);
    when(backend.tryWalkOrderedForwardAncestorsFromFreshCache(
            eq(DEFINITION), eq(CACHE_KEY), eq("urn:li:domain:child"), eq(10)))
        .thenReturn(AncestorWalkResult.fromAncestors(List.of("urn:li:domain:root")));

    FullGraphScopeReadStrategy strategy = new FullGraphScopeReadStrategy(backend);
    AncestorWalkResult result =
        strategy.walkEphemeral(DEFINITION, SOURCE, "urn:li:domain:child", 10);

    assertTrue(result.isHit());
    assertEquals(result.ancestorsOrEmpty(), List.of("urn:li:domain:root"));
  }
}
