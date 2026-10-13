package com.linkedin.metadata.graph.cache.service.read;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.entitygraph.EntityGraphCacheProperties.ScopeMode;
import com.linkedin.metadata.graph.cache.GraphReadResult;
import com.linkedin.metadata.graph.cache.ReadMissReason;
import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphDefinition;
import com.linkedin.metadata.graph.cache.config.EntityGraphModel.EntityGraphScope;
import com.linkedin.metadata.graph.cache.service.internal.GraphComponentContext;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphView;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

/**
 * A hierarchy seed that is absent from the snapshot must produce a cache MISS (so callers fall back
 * to the live graph), not an empty HIT that is trusted as authoritative. The membership read path
 * already guards this ({@code listRelatedFromView}); the hierarchy expand path must match it.
 * Otherwise a domain/container/glossary created or re-parented by a write that did not pass the
 * sync gate (async ingest, non-UI REST) reads back with empty parents/children and empty VBAC
 * ancestor expansion until the snapshot rebuilds.
 */
public class GraphReadBackendAbsentSeedTest {

  private static final EntityGraphDefinition DEFINITION =
      EntityGraphDefinition.builder()
          .graphId("container")
          .scope(EntityGraphScope.builder().mode(ScopeMode.PARTIAL).maxDepth(15).build())
          .build();

  private static final String CHILD = "urn:li:container:child";
  private static final String PRESENT = "urn:li:container:present";
  private static final String ABSENT = "urn:li:container:absent";

  private static EntityGraphView viewWithoutAbsentSeed() {
    return new EntityGraphView(
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(PRESENT)
                .relationshipType("IsPartOf")
                .build()));
  }

  private static GraphReadResult expand(String seed) {
    return expand(Set.of(seed));
  }

  private static GraphReadResult expand(Set<String> seeds) {
    GraphReadBackend backend = new GraphReadBackend(null, null, null, null, null, null);
    EntityGraphView view = viewWithoutAbsentSeed();
    GraphComponentContext component =
        new GraphComponentContext(view, TraversalCoverage.fullComplete(), "key");
    return backend.expandFromView(
        DEFINITION,
        TraversalDirection.REVERSE,
        seeds,
        Integer.MAX_VALUE,
        15,
        view,
        List.of(component));
  }

  @Test
  public void expandMissesWhenSeedAbsentFromSnapshot() {
    GraphReadResult result = expand(ABSENT);
    assertFalse(result.isHit(), "absent seed must not be served as an authoritative empty hit");
    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.ABSENT);
  }

  @Test
  public void expandMissesWhenAnyMultiRootSeedAbsentFromSnapshot() {
    // Mixed present+absent must not return a partial HIT that expands only PRESENT — callers
    // (VBAC / policy ancestor paths) would treat that as complete and skip live fallback.
    GraphReadResult result = expand(Set.of(PRESENT, ABSENT));
    assertFalse(result.isHit(), "mixed absent seed must not be served as a partial hit");
    assertTrue(result.isMiss());
    assertEquals(((GraphReadResult.Miss) result).reason(), ReadMissReason.ABSENT);
  }

  @Test
  public void expandReturnsEmptyHitWhenSeedPresentButHasNoChildren() {
    // CHILD is a vertex in the snapshot but has no incoming (reverse) neighbors: a legitimate leaf.
    // This must stay a HIT so the guard does not turn every childless node into a live fallback.
    GraphReadResult result = expand(CHILD);
    assertTrue(result.isHit(), "present leaf seed must remain an authoritative empty hit");
    assertTrue(result.verticesOrEmpty().isEmpty());
  }
}
