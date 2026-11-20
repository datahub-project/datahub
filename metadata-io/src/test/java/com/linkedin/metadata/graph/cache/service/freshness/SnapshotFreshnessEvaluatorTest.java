package com.linkedin.metadata.graph.cache.service.freshness;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.PopulationStrategy;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
import com.linkedin.metadata.graph.cache.store.EntityGraphOperationalStatus;
import org.testng.annotations.Test;

public class SnapshotFreshnessEvaluatorTest {

  private static final String CACHE_KEY = "domain@primary";

  @Test
  public void expandBlockedAllowsFreshBackgroundSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition = definition(ScopeMode.FULL, RebuildExecution.BACKGROUND, 300);
    EntityGraphSnapshot freshSnapshot = snapshot(System.currentTimeMillis());

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(freshSnapshot);

    assertFalse(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.REVERSE));
  }

  @Test
  public void expandBlockedBlocksStaleBackgroundSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition = definition(ScopeMode.FULL, RebuildExecution.BACKGROUND, 60);
    EntityGraphSnapshot staleSnapshot = snapshot(System.currentTimeMillis() - 120_000L);

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(staleSnapshot);

    assertTrue(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.REVERSE));
  }

  @Test
  public void expandBlockedBlocksWhenActiveSnapshotMissing() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition = definition(ScopeMode.FULL, RebuildExecution.BACKGROUND, 60);

    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY)).thenReturn(null);

    assertTrue(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.FORWARD));
  }

  @Test
  public void expandBlockedAllowsStaleScheduledFullSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition =
        EntityGraphDefinition.builder()
            .graphId("domain")
            .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
            .populationStrategy(PopulationStrategy.SCHEDULED)
            .rebuildExecution(RebuildExecution.SYNC)
            .populationIntervalSeconds(60)
            .build();
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.hasServableSnapshot(CACHE_KEY)).thenReturn(true);
    when(distributedStore.getSnapshot(CACHE_KEY))
        .thenReturn(snapshot(System.currentTimeMillis() - 120_000L));

    assertFalse(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.FORWARD));
  }

  @Test
  public void expandBlockedAllowsStalePartialSnapshotWhenConfigured() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition =
        definition(ScopeMode.PARTIAL, RebuildExecution.STALE_WHILE_REVALIDATE, 60);
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.hasServableSnapshot(CACHE_KEY)).thenReturn(true);
    when(distributedStore.getSnapshot(CACHE_KEY))
        .thenReturn(snapshot(System.currentTimeMillis() - 120_000L));

    assertFalse(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.FORWARD));
  }

  @Test
  public void expandBlockedBlocksBuildingTombstone() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition =
        definition(ScopeMode.PARTIAL, RebuildExecution.STALE_WHILE_REVALIDATE, 60);
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.BUILDING);
    when(distributedStore.hasServableSnapshot(CACHE_KEY)).thenReturn(false);
    when(distributedStore.getSnapshot(CACHE_KEY))
        .thenReturn(snapshot(System.currentTimeMillis() - 120_000L));

    assertTrue(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.FORWARD));
  }

  @Test
  public void expandBlockedStillBlocksStaleSyncPartialSnapshot() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition = definition(ScopeMode.PARTIAL, RebuildExecution.SYNC, 60);
    when(distributedStore.getStatus(CACHE_KEY)).thenReturn(CacheStatus.ACTIVE);
    when(distributedStore.getSnapshot(CACHE_KEY))
        .thenReturn(snapshot(System.currentTimeMillis() - 120_000L));

    assertTrue(evaluator.expandBlocked(definition, CACHE_KEY, TraversalDirection.FORWARD));
  }

  @Test
  public void shouldSkipRebuildWhileRefreshSuppressionIsActive() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition =
        definition(ScopeMode.PARTIAL, RebuildExecution.STALE_WHILE_REVALIDATE, 60);
    when(distributedStore.getRebuildSuppression(CACHE_KEY))
        .thenReturn(
            EntityGraphOperationalStatus.of(CacheStatus.COOLDOWN, System.currentTimeMillis()));

    assertTrue(evaluator.shouldSkipRebuild(CacheStatus.ACTIVE, CACHE_KEY, definition));
  }

  @Test
  public void shouldSkipRebuildAfterSuppressionExpires() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition =
        definition(ScopeMode.PARTIAL, RebuildExecution.STALE_WHILE_REVALIDATE, 60);
    when(distributedStore.getRebuildSuppression(CACHE_KEY))
        .thenReturn(
            EntityGraphOperationalStatus.of(
                CacheStatus.COOLDOWN, System.currentTimeMillis() - 120_000L));

    assertFalse(evaluator.shouldSkipRebuild(CacheStatus.ACTIVE, CACHE_KEY, definition));
  }

  @Test
  public void shouldSkipRebuildWhileOverLimitSuppressionRemains() {
    EntityGraphDistributedStore distributedStore = mock(EntityGraphDistributedStore.class);
    SnapshotFreshnessEvaluator evaluator = new SnapshotFreshnessEvaluator(distributedStore);
    EntityGraphDefinition definition =
        definition(ScopeMode.PARTIAL, RebuildExecution.STALE_WHILE_REVALIDATE, 60);
    when(distributedStore.getRebuildSuppression(CACHE_KEY))
        .thenReturn(EntityGraphOperationalStatus.of(CacheStatus.OVER_LIMIT));

    assertTrue(evaluator.shouldSkipRebuild(CacheStatus.ACTIVE, CACHE_KEY, definition));
  }

  private static EntityGraphDefinition definition(
      ScopeMode scopeMode, RebuildExecution rebuildExecution, int populationIntervalSeconds) {
    return EntityGraphDefinition.builder()
        .graphId("domain")
        .scope(EntityGraphScope.builder().mode(scopeMode).maxDepth(15).build())
        .populationIntervalSeconds(populationIntervalSeconds)
        .rebuildExecution(rebuildExecution)
        .build();
  }

  private static EntityGraphSnapshot snapshot(long builtAtMillis) {
    return EntityGraphSnapshot.builder()
        .graphId("domain")
        .cacheKey(CACHE_KEY)
        .builtAtMillis(builtAtMillis)
        .vertexCount(1)
        .edgeCount(0)
        .topologyFingerprint("fp")
        .build();
  }
}
