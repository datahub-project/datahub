package com.linkedin.metadata.graph.cache;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class GraphBuildFailureTest {

  @Test
  public void transientReasonsMapToCooldown() {
    assertEquals(GraphBuildFailure.statusForFailedBuild("graph_failed"), CacheStatus.COOLDOWN);
    assertEquals(GraphBuildFailure.statusForFailedBuild("search_failed"), CacheStatus.COOLDOWN);
    assertEquals(GraphBuildFailure.statusForFailedBuild("scroll_incomplete"), CacheStatus.COOLDOWN);
    assertEquals(GraphBuildFailure.statusForFailedBuild("empty_snapshot"), CacheStatus.COOLDOWN);
    assertEquals(GraphBuildFailure.statusForFailedBuild("empty_component"), CacheStatus.COOLDOWN);
    assertEquals(GraphBuildFailure.failureMetric("empty_snapshot"), "entity.graph.cache.cooldown");
    assertFalse(GraphBuildFailure.suppressesAutomaticRebuild(CacheStatus.COOLDOWN));
  }

  @Test
  public void structuralReasonsMapToOverLimit() {
    assertEquals(GraphBuildFailure.statusForFailedBuild("vertex_limit"), CacheStatus.OVER_LIMIT);
    assertEquals(GraphBuildFailure.statusForFailedBuild("edge_limit"), CacheStatus.OVER_LIMIT);
    assertEquals(GraphBuildFailure.failureMetric("vertex_limit"), "entity.graph.cache.over_limit");
    assertTrue(GraphBuildFailure.suppressesAutomaticRebuild(CacheStatus.OVER_LIMIT));
  }

  @Test
  public void configReasonsMapToInvalid() {
    assertEquals(
        GraphBuildFailure.statusForFailedBuild("primary_reverse_unsupported"), CacheStatus.INVALID);
    assertEquals(
        GraphBuildFailure.statusForFailedBuild("search_reverse_unsupported"), CacheStatus.INVALID);
    assertEquals(
        GraphBuildFailure.statusForFailedBuild("invalid_configuration"), CacheStatus.INVALID);
    assertEquals(
        GraphBuildFailure.failureMetric("partial_requires_seeds"), "entity.graph.cache.invalid");
    assertTrue(GraphBuildFailure.suppressesAutomaticRebuild(CacheStatus.INVALID));
  }
}
