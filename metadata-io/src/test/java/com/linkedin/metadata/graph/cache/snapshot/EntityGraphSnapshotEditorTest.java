package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotEditor.FullWalkEdit;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshotEditor.VertexRemovalResult;
import com.linkedin.metadata.graph.cache.snapshot.TraversalCoverage.DirectionCoverage;
import java.util.List;
import java.util.Set;
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

  @Test
  public void fullWalkReplacesReverseClosureAndKeepsUpwardEdge() {
    String parent = "urn:li:glossaryNode:parent";
    String seed = "urn:li:glossaryNode:seed";
    String child = "urn:li:glossaryTerm:child";
    String grandchild = "urn:li:glossaryTerm:grandchild";
    String branch = "urn:li:glossaryNode:branch";
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey("glossary@graph:fp")
            .buildSource("graph")
            .builtAtMillis(50L)
            .generation(4L)
            .edges(
                List.of(
                    edge(seed, parent),
                    edge(branch, parent),
                    edge(child, seed),
                    edge(grandchild, seed)))
            .traversalCoverage(
                TraversalCoverage.builder()
                    .direction(
                        DirectionCoverage.builder()
                            .direction(TraversalDirection.FORWARD)
                            .explored(true)
                            .exploredDepth(1)
                            .configuredMaxDepth(25)
                            .complete(true)
                            .trustedSeeds(List.of(parent))
                            .build())
                    .build())
            .build();

    FullWalkEdit edit =
        EntityGraphSnapshotEditor.applyFullWalk(
            existing,
            "glossary",
            existing.getCacheKey(),
            "graph",
            existing.getBuiltAtMillis(),
            5L,
            TraversalDirection.REVERSE,
            Set.of(seed),
            List.of(edge(child, seed), edge(grandchild, child)),
            25);

    assertTrue(edit.isContainsAllSeeds());
    assertEquals(edit.getExploredDepth(), 2);
    EntityGraphSnapshot updated = edit.getSnapshot();
    assertNotNull(updated);
    assertEquals(updated.getCacheKey(), existing.getCacheKey());
    assertEquals(updated.getBuiltAtMillis(), 50L);
    assertEquals(updated.getGeneration(), 5L);
    assertTrue(hasEdge(updated, seed, parent));
    assertTrue(hasEdge(updated, branch, parent));
    assertTrue(hasEdge(updated, child, seed));
    assertTrue(hasEdge(updated, grandchild, child));
    assertFalse(hasEdge(updated, grandchild, seed));
    assertTrue(updated.getTraversalCoverage().isTrustedFullWalk(TraversalDirection.REVERSE));
    assertTrue(updated.getTraversalCoverage().isTrustedFullWalk(TraversalDirection.FORWARD));
    DirectionCoverage reverse =
        updated.getTraversalCoverage().getDirection(TraversalDirection.REVERSE);
    assertEquals(reverse.getExploredDepth(), 2);
    assertEquals(reverse.getConfiguredMaxDepth(), 25);
    assertEquals(reverse.getTrustedSeeds(), List.of(seed));
  }

  @Test
  public void fullWalkDepthIsTheFarthestSeedWhenSeedsAreNested() {
    String seed = "urn:li:glossaryNode:seed";
    String child = "urn:li:glossaryTerm:child";
    String grandchild = "urn:li:glossaryTerm:grandchild";
    FullWalkEdit edit =
        EntityGraphSnapshotEditor.applyFullWalk(
            null,
            "glossary",
            "glossary@graph:nested",
            "graph",
            1L,
            1L,
            TraversalDirection.REVERSE,
            Set.of(seed, child),
            List.of(edge(child, seed), edge(grandchild, child)),
            25);

    assertTrue(edit.isContainsAllSeeds());
    assertEquals(edit.getExploredDepth(), 2);
  }

  @Test
  public void fullWalkDepthIgnoresLeftoverEdgesBeyondTheWalk() {
    String oldParent = "urn:li:glossaryNode:oldParent";
    String newParent = "urn:li:glossaryNode:newParent";
    String child = "urn:li:glossaryTerm:child";
    String grandchild = "urn:li:glossaryTerm:grandchild";
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey("glossary@graph:reparent")
            .buildSource("graph")
            .builtAtMillis(1L)
            .generation(1L)
            .edges(List.of(edge(child, oldParent), edge(grandchild, child)))
            .traversalCoverage(
                TraversalCoverage.builder()
                    .direction(
                        DirectionCoverage.builder()
                            .direction(TraversalDirection.REVERSE)
                            .explored(true)
                            .exploredDepth(4)
                            .configuredMaxDepth(25)
                            .complete(true)
                            .build())
                    .build())
            .build();

    FullWalkEdit edit =
        EntityGraphSnapshotEditor.applyFullWalk(
            existing,
            "glossary",
            existing.getCacheKey(),
            "graph",
            2L,
            2L,
            TraversalDirection.REVERSE,
            Set.of(newParent),
            List.of(edge(child, newParent)),
            25);

    assertTrue(edit.isContainsAllSeeds());
    assertEquals(edit.getExploredDepth(), 1);
    EntityGraphSnapshot updated = edit.getSnapshot();
    assertNotNull(updated);
    assertTrue(hasEdge(updated, grandchild, child));
    assertTrue(hasEdge(updated, child, newParent));
    DirectionCoverage reverse =
        updated.getTraversalCoverage().getDirection(TraversalDirection.REVERSE);
    assertEquals(reverse.getExploredDepth(), 4);
    assertEquals(reverse.getTrustedSeeds(), List.of(newParent));
    assertEquals(reverse.getTrustedEdgeLines(), List.of(child + "->" + newParent + ":IsPartOf"));
  }

  @Test
  public void fullWalkKeepsTheDeeperExistingDirectionDepth() {
    String seed = "urn:li:glossaryNode:seed";
    String child = "urn:li:glossaryTerm:child";
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey("glossary@graph:depth")
            .buildSource("graph")
            .builtAtMillis(1L)
            .generation(1L)
            .edges(List.of(edge(child, seed)))
            .traversalCoverage(
                TraversalCoverage.builder()
                    .direction(
                        DirectionCoverage.builder()
                            .direction(TraversalDirection.REVERSE)
                            .explored(true)
                            .exploredDepth(4)
                            .configuredMaxDepth(25)
                            .complete(true)
                            .build())
                    .build())
            .build();

    FullWalkEdit edit =
        EntityGraphSnapshotEditor.applyFullWalk(
            existing,
            "glossary",
            existing.getCacheKey(),
            "graph",
            2L,
            2L,
            TraversalDirection.REVERSE,
            Set.of(child),
            List.of(),
            25);

    assertTrue(edit.isContainsAllSeeds());
    assertEquals(edit.getExploredDepth(), 0);
    assertEquals(
        edit.getSnapshot()
            .getTraversalCoverage()
            .getDirection(TraversalDirection.REVERSE)
            .getExploredDepth(),
        4);
  }

  @Test
  public void fullWalkCanonicalizesStoredEdgesBeforeReplacingThem() {
    String seed = "urn:li:glossaryNode:seed";
    String child = "urn:li:glossaryTerm:child";
    String quotedChild = "\"" + child + "\"";
    EntityGraphSnapshot existing =
        EntityGraphSnapshot.builder()
            .graphId("glossary")
            .cacheKey("glossary@graph:quoted")
            .buildSource("graph")
            .builtAtMillis(1L)
            .generation(1L)
            .edges(List.of(edge(quotedChild, seed)))
            .build();

    FullWalkEdit edit =
        EntityGraphSnapshotEditor.applyFullWalk(
            existing,
            "glossary",
            existing.getCacheKey(),
            "graph",
            2L,
            2L,
            TraversalDirection.REVERSE,
            Set.of(seed),
            List.of(edge(child, seed)),
            25);

    assertTrue(edit.isContainsAllSeeds());
    assertEquals(edit.getSnapshot().getEdges().size(), 1);
    assertEquals(edit.getSnapshot().getEdges().get(0).getSourceUrn(), child);
  }

  @Test
  public void fullWalkCreateWithoutEdgesDoesNotInventAVertex() {
    FullWalkEdit edit =
        EntityGraphSnapshotEditor.applyFullWalk(
            null,
            "glossary",
            "glossary@graph:empty",
            "graph",
            1L,
            1L,
            TraversalDirection.REVERSE,
            Set.of("urn:li:glossaryNode:seed"),
            List.of(),
            25);

    assertFalse(edit.isContainsAllSeeds());
    assertNull(edit.getSnapshot());
  }

  private static DirectedEdge edge(String source, String destination) {
    return DirectedEdge.builder()
        .sourceUrn(source)
        .destinationUrn(destination)
        .relationshipType("IsPartOf")
        .build();
  }

  private static boolean hasEdge(EntityGraphSnapshot snapshot, String source, String destination) {
    return snapshot.getEdges().stream()
        .anyMatch(
            edge ->
                source.equals(edge.getSourceUrn()) && destination.equals(edge.getDestinationUrn()));
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
