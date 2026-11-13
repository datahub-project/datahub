package com.linkedin.metadata.graph.cache.service.read;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.EntityGraphCache;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import org.testng.annotations.Test;

public class GraphReadDepthResolverTest {

  private static final EntityGraphDefinition PARTIAL_DEPTH_3 =
      EntityGraphDefinition.builder()
          .graphId("glossary")
          .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(3).build())
          .build();

  private static final EntityGraphDefinition FULL =
      EntityGraphDefinition.builder()
          .graphId("domain")
          .scope(EntityGraphScope.builder().mode(ScopeMode.FULL).maxDepth(0).build())
          .build();

  @Test
  public void partialUsesConfiguredMaxDepthForDefinitionSentinel() {
    assertEquals(
        GraphReadDepthResolver.resolve(PARTIAL_DEPTH_3, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        3);
  }

  @Test
  public void partialClampsExplicitCallerDepthToConfiguredMaxDepth() {
    assertEquals(GraphReadDepthResolver.resolve(PARTIAL_DEPTH_3, 10), 3);
    assertEquals(GraphReadDepthResolver.resolve(PARTIAL_DEPTH_3, 2), 2);
  }

  @Test
  public void fullIgnoresConfiguredMaxDepthAndWalksMaterializedSnapshot() {
    assertEquals(
        GraphReadDepthResolver.resolve(FULL, EntityGraphCache.USE_DEFINITION_MAX_DEPTH),
        Integer.MAX_VALUE);
    assertEquals(GraphReadDepthResolver.resolve(FULL, 1), 1);
  }
}
