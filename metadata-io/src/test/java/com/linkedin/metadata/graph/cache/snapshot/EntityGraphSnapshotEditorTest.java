package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotEditor.VertexRemovalResult;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import java.util.List;
import org.testng.annotations.Test;

public class EntityGraphSnapshotEditorTest {

  @Test
  public void removeVertexUnchangedWhenAbsent() {
    EntityGraphSnapshot snapshot = sampleSnapshot();
    VertexRemovalResult result =
        EntityGraphSnapshotEditor.removeVertex(snapshot, "urn:li:domain:missing");
    assertFalse(result.isChanged());
    assertFalse(result.isDropKey());
  }

  @Test
  public void removeVertexUpdatesSnapshotWhenOtherEdgesRemain() {
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey("domain@search")
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:sibling")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(3)
            .edgeCount(2)
            .build();
    VertexRemovalResult result =
        EntityGraphSnapshotEditor.removeVertex(snapshot, "urn:li:domain:child");
    assertTrue(result.isChanged());
    assertFalse(result.isDropKey());
    assertNotNull(result.getSnapshot());
    assertEquals(result.getSnapshot().getVertexCount(), 2);
    assertEquals(result.getSnapshot().getEdgeCount(), 1);
  }

  @Test
  public void removeVertexPreservesBuiltAtMillisAndFullScopeCoverage() {
    long builtAtMillis = 1_700_000_000_000L;
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey("domain@search")
            .builtAtMillis(builtAtMillis)
            .traversalCoverage(
                TraversalCoverage.builder()
                    .direction(
                        DirectionCoverage.builder()
                            .direction(TraversalDirection.FORWARD)
                            .explored(true)
                            .exploredDepth(1)
                            .configuredMaxDepth(15)
                            .complete(false)
                            .build())
                    .build())
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:sibling")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(3)
            .edgeCount(2)
            .build();

    VertexRemovalResult result =
        EntityGraphSnapshotEditor.removeVertex(snapshot, "urn:li:domain:child");

    assertTrue(result.isChanged());
    assertNotNull(result.getSnapshot());
    assertEquals(result.getSnapshot().getBuiltAtMillis(), builtAtMillis);
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.FORWARD));
    assertTrue(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.REVERSE));
  }

  @Test
  public void removeVertexMarksPartialScopeCoverageIncomplete() {
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey("domain@search:component-fp")
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:child")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:sibling")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(3)
            .edgeCount(2)
            .build();

    VertexRemovalResult result =
        EntityGraphSnapshotEditor.removeVertex(snapshot, "urn:li:domain:child");

    assertTrue(result.isChanged());
    assertNotNull(result.getSnapshot());
    assertFalse(result.getSnapshot().getTraversalCoverage().canSatisfy(TraversalDirection.FORWARD));
  }

  @Test
  public void removeVertexEmptiesGraph() {
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("domain")
            .cacheKey("domain@search")
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:domain:only")
                        .destinationUrn("urn:li:domain:root")
                        .relationshipType("IsPartOf")
                        .build()))
            .vertexCount(2)
            .edgeCount(1)
            .build();
    VertexRemovalResult result =
        EntityGraphSnapshotEditor.removeVertex(snapshot, "urn:li:domain:only");
    assertTrue(result.isChanged());
    assertTrue(result.isDropKey());
    assertNull(result.getSnapshot());
  }

  @Test
  public void removeVertexDropsAllParallelMembershipEdgesOnUser() {
    EntityGraphSnapshot snapshot =
        EntityGraphSnapshot.builder()
            .graphId("membership")
            .cacheKey("membership@graph")
            .edges(
                List.of(
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:corpuser:alice")
                        .destinationUrn("urn:li:corpGroup:eng")
                        .relationshipType("IsMemberOfGroup")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:corpuser:alice")
                        .destinationUrn("urn:li:corpGroup:eng")
                        .relationshipType("IsMemberOfNativeGroup")
                        .build(),
                    DirectedEdge.builder()
                        .sourceUrn("urn:li:corpuser:bob")
                        .destinationUrn("urn:li:corpGroup:eng")
                        .relationshipType("IsMemberOfGroup")
                        .build()))
            .vertexCount(3)
            .edgeCount(3)
            .build();

    VertexRemovalResult result =
        EntityGraphSnapshotEditor.removeVertex(snapshot, "urn:li:corpuser:alice");

    assertTrue(result.isChanged());
    assertNotNull(result.getSnapshot());
    assertEquals(result.getSnapshot().getEdgeCount(), 1);
    assertEquals(result.getSnapshot().getEdges().get(0).getSourceUrn(), "urn:li:corpuser:bob");
  }

  private static EntityGraphSnapshot sampleSnapshot() {
    return EntityGraphSnapshot.builder()
        .graphId("domain")
        .cacheKey("domain@search")
        .edges(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn("urn:li:domain:child")
                    .destinationUrn("urn:li:domain:root")
                    .relationshipType("IsPartOf")
                    .build()))
        .vertexCount(2)
        .edgeCount(1)
        .build();
  }
}
