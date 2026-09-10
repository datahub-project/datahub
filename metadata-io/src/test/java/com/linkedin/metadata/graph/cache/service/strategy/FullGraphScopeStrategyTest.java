package com.linkedin.metadata.graph.cache.service.strategy;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphSnapshotSource;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.store.EntityGraphCacheKeys;
import org.testng.annotations.Test;

public class FullGraphScopeStrategyTest {

  private static final EntityGraphDefinition FULL =
      EntityGraphDefinition.builder()
          .graphId("domain")
          .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).build())
          .build();

  private static final EntityGraphDefinition PARTIAL =
      EntityGraphDefinition.builder()
          .graphId("glossary")
          .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
          .build();

  @Test
  public void matchesFullScopeOnly() {
    assertTrue(FullGraphScopeStrategy.matches(FULL));
    assertFalse(FullGraphScopeStrategy.matches(PARTIAL));
  }

  @Test
  public void cacheKeyUsesFullSnapshotPrefix() {
    assertEquals(
        FullGraphScopeStrategy.cacheKey(FULL, GraphSnapshotSource.SEARCH),
        EntityGraphCacheKeys.fullCacheKey("domain", GraphSnapshotSource.SEARCH));
  }
}
