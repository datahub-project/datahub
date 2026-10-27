package com.linkedin.metadata.graph.cache.service.freshness;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.RebuildExecution;
import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.CacheStatus;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot;
import com.linkedin.metadata.graph.cache.store.EntityGraphDistributedStore;
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
