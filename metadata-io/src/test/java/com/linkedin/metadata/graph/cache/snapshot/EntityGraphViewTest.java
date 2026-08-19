package com.linkedin.metadata.graph.cache.snapshot;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.graph.cache.TraversalDirection;
import com.linkedin.metadata.graph.cache.snapshot.EntityGraphSnapshot.DirectedEdge;
import java.util.List;
import java.util.Set;
import org.testng.annotations.Test;

public class EntityGraphViewTest {

  private static final String ROOT = "urn:li:domain:root";
  private static final String CHILD = "urn:li:domain:child";
  private static final String GRANDCHILD = "urn:li:domain:grandchild";

  @Test
  public void testMaxDepthZeroReturnsSeedsOnly() {
    EntityGraphView view = linearView();
    Set<String> expanded = view.expand(TraversalDirection.REVERSE, Set.of(ROOT), 100, 0);
    assertEquals(expanded, Set.of(ROOT));
  }

  @Test
  public void testMaxDepthOneReturnsDirectNeighbors() {
    EntityGraphView view = linearView();
    Set<String> expanded = view.expand(TraversalDirection.REVERSE, Set.of(ROOT), 100, 1);
    assertEquals(expanded, Set.of(ROOT, CHILD));
  }

  @Test
  public void testMaxDepthTwoReturnsTwoHops() {
    EntityGraphView view = linearView();
    Set<String> expanded = view.expand(TraversalDirection.REVERSE, Set.of(ROOT), 100, 2);
    assertEquals(expanded, Set.of(ROOT, CHILD, GRANDCHILD));
  }

  @Test
  public void testForwardWalkAlongStoredEdges() {
    EntityGraphView view = linearView();
    Set<String> expanded = view.expand(TraversalDirection.FORWARD, Set.of(GRANDCHILD), 100, 2);
    assertTrue(expanded.contains(GRANDCHILD));
    assertTrue(expanded.contains(CHILD));
    assertTrue(expanded.contains(ROOT));
  }

  private static final String ISLAND_A = "urn:li:domain:island-a";
  private static final String ISLAND_B = "urn:li:domain:island-b";

  @Test
  public void testSeedsInSameWeakComponent() {
    EntityGraphView view = linearView();
    assertTrue(view.seedsInSameWeakComponent(Set.of(ROOT, CHILD)));
    assertTrue(!view.seedsInSameWeakComponent(Set.of(ROOT, ISLAND_A)));
  }

  @Test
  public void testInducedComponentEdges() {
    EntityGraphView view = disconnectedView();
    assertEquals(view.inducedComponentEdges(Set.of(ROOT)).size(), 2);
    assertEquals(view.inducedComponentEdges(Set.of(ISLAND_A)).size(), 1);
  }

  @Test
  public void testComponentFingerprintStable() {
    EntityGraphView view = linearView();
    String fp1 = view.componentFingerprint(Set.of(ROOT));
    String fp2 = view.componentFingerprint(Set.of(CHILD));
    assertEquals(fp1, fp2);
    assertTrue(!fp1.equals(disconnectedView().componentFingerprint(Set.of(ISLAND_A))));
  }

  private static EntityGraphView disconnectedView() {
    return new EntityGraphView(
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(GRANDCHILD)
                .destinationUrn(CHILD)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(ISLAND_B)
                .destinationUrn(ISLAND_A)
                .relationshipType("IsPartOf")
                .build()));
  }

  @Test
  public void orderedForwardAncestorsReturnsParentChain() {
    EntityGraphView view = linearView();
    assertEquals(view.orderedForwardAncestors(GRANDCHILD, 10), List.of(CHILD, ROOT));
    assertEquals(view.orderedForwardAncestors(GRANDCHILD, 1), List.of(CHILD));
  }

  @Test
  public void containsVertexReflectsGraphMembership() {
    EntityGraphView view = linearView();
    assertTrue(view.containsVertex(CHILD));
    assertFalse(view.containsVertex("urn:li:domain:missing"));
  }

  @Test
  public void expandWithResultDetectsMaxDepthTruncation() {
    EntityGraphView view = linearView();
    EntityGraphView.ExpandResult result =
        view.expandWithResult(TraversalDirection.REVERSE, Set.of(ROOT), 100, 1);
    assertEquals(result.getVertices(), Set.of(ROOT, CHILD));
    assertTrue(result.isTruncatedByMaxDepth());
    assertFalse(result.isTruncatedByLimit());
  }

  @Test
  public void expandWithResultDetectsLimitTruncation() {
    EntityGraphView view = linearView();
    EntityGraphView.ExpandResult result =
        view.expandWithResult(TraversalDirection.REVERSE, Set.of(ROOT), 2, 2);
    assertEquals(result.getVertices().size(), 2);
    assertTrue(result.isTruncatedByLimit());
  }

  @Test
  public void orderedForwardAncestorsReturnsAllParentsAtSameDepth() {
    String child = "urn:li:domain:child";
    String parentA = "urn:li:domain:parent-a";
    String parentB = "urn:li:domain:parent-b";
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(child)
                    .destinationUrn(parentA)
                    .relationshipType("IsPartOf")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(child)
                    .destinationUrn(parentB)
                    .relationshipType("IsPartOf")
                    .build()));
    assertEquals(view.orderedForwardAncestors(child, 1), List.of(parentA, parentB));
  }

  @Test
  public void orderedForwardAncestorsTerminatesOnCycle() {
    String a = "urn:li:domain:a";
    String b = "urn:li:domain:b";
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(a)
                    .destinationUrn(b)
                    .relationshipType("IsPartOf")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(b)
                    .destinationUrn(a)
                    .relationshipType("IsPartOf")
                    .build()));
    assertEquals(view.orderedForwardAncestors(a, 10), List.of(b));
  }

  private static final String USER = "urn:li:corpuser:alice";
  private static final String GROUP = "urn:li:corpGroup:eng";
  private static final String ROLE = "urn:li:dataHubRole:admin";

  @Test
  public void neighborsWithResultPreservesParallelEdgeTypes() {
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfNativeGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(ROLE)
                    .relationshipType("IsMemberOfRole")
                    .build()));

    EntityGraphView.NeighborResult groups =
        view.neighborsWithResult(
            TraversalDirection.FORWARD,
            USER,
            Set.of("IsMemberOfGroup", "IsMemberOfNativeGroup"),
            1,
            0,
            10);
    assertEquals(groups.getTotal(), 2);
    assertEquals(groups.getNeighbors().size(), 2);

    EntityGraphView.NeighborResult roles =
        view.neighborsWithResult(
            TraversalDirection.FORWARD, USER, Set.of("IsMemberOfRole"), 1, 0, 10);
    assertEquals(roles.getTotal(), 1);
    assertEquals(roles.getNeighbors().get(0).getDestinationUrn(), ROLE);
  }

  @Test
  public void neighborsWithResultReverseListsMembers() {
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn("urn:li:corpuser:bob")
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build()));

    EntityGraphView.NeighborResult members =
        view.neighborsWithResult(
            TraversalDirection.REVERSE, GROUP, Set.of("IsMemberOfGroup"), 1, 0, 10);
    assertEquals(members.getTotal(), 2);
    assertTrue(members.getNeighbors().stream().anyMatch(edge -> edge.getSourceUrn().equals(USER)));
  }

  @Test
  public void neighborsWithResultPaginates() {
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn("urn:li:corpGroup:a")
                    .relationshipType("IsMemberOfGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn("urn:li:corpGroup:b")
                    .relationshipType("IsMemberOfGroup")
                    .build()));

    EntityGraphView.NeighborResult page =
        view.neighborsWithResult(
            TraversalDirection.FORWARD, USER, Set.of("IsMemberOfGroup"), 1, 1, 1);
    assertEquals(page.getTotal(), 2);
    assertEquals(page.getNeighbors().size(), 1);
  }

  @Test
  public void edgeCountEqualsListSizeWithParallelTypes() {
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfNativeGroup")
                    .build()));
    assertEquals(view.edgeCount(), 2);
    assertEquals(view.getEdges().size(), 2);
  }

  @Test
  public void inducedComponentEdgesExcludesCrossComponentEdges() {
    String otherRoot = "urn:li:domain:other-root";
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(CHILD)
                    .destinationUrn(ROOT)
                    .relationshipType("IsPartOf")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfNativeGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(ISLAND_B)
                    .destinationUrn(otherRoot)
                    .relationshipType("IsPartOf")
                    .build()));

    List<DirectedEdge> rootComponent = view.inducedComponentEdges(Set.of(ROOT));
    assertEquals(rootComponent.size(), 1);
    assertEquals(rootComponent.get(0).getSourceUrn(), CHILD);

    List<DirectedEdge> membershipComponent = view.inducedComponentEdges(Set.of(USER));
    assertEquals(membershipComponent.size(), 2);
  }

  @Test
  public void withoutVertexRemovesAllIncidentTypedEdges() {
    EntityGraphView view =
        new EntityGraphView(
            List.of(
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn(USER)
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfNativeGroup")
                    .build(),
                DirectedEdge.builder()
                    .sourceUrn("urn:li:corpuser:bob")
                    .destinationUrn(GROUP)
                    .relationshipType("IsMemberOfGroup")
                    .build()));

    EntityGraphView updated = view.withoutVertex(USER).orElseThrow();
    assertEquals(updated.getEdges().size(), 1);
    assertEquals(updated.getEdges().get(0).getSourceUrn(), "urn:li:corpuser:bob");
    assertTrue(view.withoutVertex("urn:li:domain:missing").isEmpty());
  }

  @Test
  public void concurrentLazyGraphBuildIsSafe() throws Exception {
    EntityGraphView view = linearView();
    int threadCount = 8;
    java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
    java.util.concurrent.CountDownLatch doneLatch =
        new java.util.concurrent.CountDownLatch(threadCount);
    java.util.concurrent.ExecutorService executor =
        java.util.concurrent.Executors.newFixedThreadPool(threadCount);
    try {
      for (int i = 0; i < threadCount; i++) {
        executor.submit(
            () -> {
              try {
                startLatch.await();
                assertTrue(view.containsVertex(ROOT));
                assertTrue(
                    view.expand(TraversalDirection.REVERSE, Set.of(ROOT), 100, 2).size() >= 1);
                assertTrue(view.seedsInSameWeakComponent(Set.of(ROOT, CHILD)));
              } catch (Exception e) {
                throw new RuntimeException(e);
              } finally {
                doneLatch.countDown();
              }
            });
      }
      startLatch.countDown();
      assertTrue(doneLatch.await(30, java.util.concurrent.TimeUnit.SECONDS));
    } finally {
      executor.shutdownNow();
    }
  }

  private static EntityGraphView linearView() {
    // child -> parent edges (IsPartOf)
    return new EntityGraphView(
        List.of(
            DirectedEdge.builder()
                .sourceUrn(CHILD)
                .destinationUrn(ROOT)
                .relationshipType("IsPartOf")
                .build(),
            DirectedEdge.builder()
                .sourceUrn(GRANDCHILD)
                .destinationUrn(CHILD)
                .relationshipType("IsPartOf")
                .build()));
  }
}
