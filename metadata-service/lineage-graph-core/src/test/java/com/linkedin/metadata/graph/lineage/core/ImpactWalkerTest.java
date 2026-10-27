package com.linkedin.metadata.graph.lineage.core;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.testng.annotations.Test;

public class ImpactWalkerTest {

  private static final String DOWNSTREAM_OF = "DownstreamOf";
  // Impact (downstream) for datasets: follow DownstreamOf edges pointing at the current node.
  private static final List<EdgeFilter.Rule> DOWNSTREAM =
      List.of(
          new EdgeFilter.Rule("dataset", DOWNSTREAM_OF, EdgeFilter.Direction.INCOMING, "dataset"),
          new EdgeFilter.Rule("dataJob", "Produces", EdgeFilter.Direction.OUTGOING, "dataset"));

  private static String ds(String name) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)";
  }

  private static String job(String name) {
    return "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD)," + name + ")";
  }

  /** Adds "child is DownstreamOf parent" edges. */
  private static GraphBuilder lineage(String... parentChildPairs) {
    GraphBuilder b = new GraphBuilder();
    for (int i = 0; i < parentChildPairs.length; i += 2) {
      b.addEdge(
          ds(parentChildPairs[i + 1]), DOWNSTREAM_OF, ds(parentChildPairs[i]), null, null, null);
    }
    return b;
  }

  /** A walker over one in-memory snapshot. */
  private record Walker(GraphSnapshot snapshot, ImpactWalker walker) {
    WalkResult walk(WalkRequest request) {
      return walker.walk(snapshot, request);
    }
  }

  private static Walker walker(BaseGraph g) {
    return new Walker(GraphSnapshot.of(g), new ImpactWalker());
  }

  private static WalkRequest request(String start, int maxHops, int maxRelations, boolean partial) {
    return new WalkRequest(start, DOWNSTREAM, maxHops, maxRelations, partial, null, null, 0);
  }

  private static Map<String, Integer> degrees(WalkResult r) {
    Map<String, Integer> m = new HashMap<>();
    for (int i = 0; i < r.size(); i++) {
      m.put(r.urn(i), r.degree(i));
    }
    return m;
  }

  private static List<String> path(WalkResult r, String urn) {
    for (int i = 0; i < r.size(); i++) {
      if (r.urn(i).equals(urn)) {
        List<int[]> paths = r.paths(i);
        assertEquals(paths.size(), 1, "paths of " + urn);
        List<String> out = new ArrayList<>();
        for (int n : paths.get(0)) {
          out.add(r.graph().urn(n));
        }
        return out;
      }
    }
    throw new AssertionError(urn + " not in result");
  }

  @Test
  public void testDegreesPathsAndExplored() {
    // a -> b -> c, a -> d -> c (c reached twice at hop 2; first parent wins)
    BaseGraph g = lineage("a", "b", "b", "c", "a", "d", "d", "c").build();
    WalkResult r = walker(g).walk(request(ds("a"), 10, 0, false));

    assertEquals(r.status(), WalkResult.Status.COMPLETE);
    assertEquals(degrees(r), Map.of(ds("b"), 1, ds("d"), 1, ds("c"), 2));
    assertEquals(path(r, ds("b")), List.of(ds("a"), ds("b")));
    assertEquals(path(r, ds("c")), List.of(ds("a"), ds("b"), ds("c")));
    for (int i = 0; i < r.size(); i++) {
      // c starts hop 3 (which finds nothing), so it is explored like the others.
      assertTrue(r.explored(i), r.urn(i));
      assertEquals(r.relationshipType(i), DOWNSTREAM_OF);
    }

    // With maxHops = 2 there is no hop 3, so the hop-2 node is never marked explored.
    WalkResult capped = walker(g).walk(request(ds("a"), 2, 0, false));
    for (int i = 0; i < capped.size(); i++) {
      assertEquals(capped.explored(i), !capped.urn(i).equals(ds("c")), capped.urn(i));
    }
  }

  @Test
  public void testMaxHopsAndCyclesTerminate() {
    // a -> b -> c -> a (cycle back to the root)
    BaseGraph g = lineage("a", "b", "b", "c", "c", "a").build();
    Walker w = walker(g);

    assertEquals(degrees(w.walk(request(ds("a"), 10, 0, false))), Map.of(ds("b"), 1, ds("c"), 2));
    assertEquals(degrees(w.walk(request(ds("a"), 1, 0, false))), Map.of(ds("b"), 1));
  }

  @Test
  public void testDuplicateAndSelfEdgesAreDropped() {
    BaseGraph g = lineage("a", "b", "a", "b", "a", "a").build();
    assertEquals(g.edgeCount(), 1);
    assertEquals(degrees(walker(g).walk(request(ds("a"), 10, 0, false))).size(), 1);
  }

  @Test
  public void testViaEntityBecomesRelationshipAndFrontier() {
    // b is DownstreamOf a via job j; j Produces e. Starting at a, j is a hop-1 relationship whose
    // path is cut at j, and j is expanded at hop 2 even though it is never "visited".
    GraphBuilder b = new GraphBuilder();
    b.addEdge(ds("b"), DOWNSTREAM_OF, ds("a"), job("j"), null, null);
    b.addEdge(job("j"), "Produces", ds("e"), null, null, null);
    WalkResult r = walker(b.build()).walk(request(ds("a"), 10, 0, false));

    assertEquals(degrees(r), Map.of(ds("b"), 1, job("j"), 1, ds("e"), 2));
    assertEquals(path(r, ds("b")), List.of(ds("a"), job("j"), ds("b")));
    assertEquals(path(r, job("j")), List.of(ds("a"), job("j")));
    // j had no stored path when e was found, so e's path starts at j.
    assertEquals(path(r, ds("e")), List.of(job("j"), ds("e")));
  }

  @Test
  public void testPathCycleThroughViaStopsBelowIt() {
    // b via j, then c via j again: c's path [a, j, b, j, c] repeats j. c still gets a relationship
    // (addEdgeToPaths only checks the parent's path), but d below c is visited without one.
    GraphBuilder b = new GraphBuilder();
    b.addEdge(ds("b"), DOWNSTREAM_OF, ds("a"), job("j"), null, null);
    b.addEdge(ds("c"), DOWNSTREAM_OF, ds("b"), job("j"), null, null);
    b.addEdge(ds("d"), DOWNSTREAM_OF, ds("c"), null, null, null);
    b.addEdge(ds("e"), DOWNSTREAM_OF, ds("d"), null, null, null);
    WalkResult r = walker(b.build()).walk(request(ds("a"), 10, 0, false));

    assertEquals(degrees(r), Map.of(ds("b"), 1, job("j"), 1, ds("c"), 2));
    assertEquals(path(r, ds("c")), List.of(ds("a"), job("j"), ds("b"), job("j"), ds("c")));
  }

  @Test
  public void testMaxRelations() {
    BaseGraph g = lineage("a", "b", "b", "c", "c", "d").build();
    Walker w = walker(g);

    WalkResult partial = w.walk(request(ds("a"), 10, 2, true));
    assertEquals(partial.status(), WalkResult.Status.MAX_RELATIONS);
    assertTrue(partial.partial());
    assertEquals(partial.size(), 2);

    WalkResult strict = w.walk(request(ds("a"), 10, 2, false));
    assertEquals(strict.status(), WalkResult.Status.MAX_RELATIONS);
    assertFalse(strict.partial());
  }

  @Test
  public void testUnknownStartAndScratchReuse() {
    BaseGraph g = lineage("a", "b").build();
    Walker w = walker(g);
    assertEquals(
        w.walk(request(ds("zzz"), 10, 0, false)).status(), WalkResult.Status.UNKNOWN_START);
    // Pooled scratch must be fully reset between walks.
    for (int i = 0; i < 3; i++) {
      assertEquals(degrees(w.walk(request(ds("a"), 10, 0, false))), Map.of(ds("b"), 1));
    }
  }

  @Test
  public void testTimeWindowIsIgnored() {
    GraphBuilder b = new GraphBuilder();
    b.addEdge(ds("old"), DOWNSTREAM_OF, ds("a"), null, 50L, 60L);
    b.addEdge(ds("new"), DOWNSTREAM_OF, ds("a"), null, 150L, 150L);
    WalkResult r =
        walker(b.build()).walk(new WalkRequest(ds("a"), DOWNSTREAM, 10, 0, false, 100L, 200L, 0));
    assertEquals(degrees(r).keySet(), java.util.Set.of(ds("old"), ds("new")));
  }

  @Test
  public void testScratchIsReplacedAfterReload() {
    // A walker reused across snapshots of different sizes must not reuse stale scratch space.
    ImpactWalker w = new ImpactWalker(1);
    GraphSnapshot small = GraphSnapshot.of(lineage("a", "b").build());
    GraphSnapshot large = GraphSnapshot.of(lineage("a", "b", "b", "c", "c", "d", "d", "e").build());
    try (WalkResult r = w.walk(small, request(ds("a"), 10, 0, false))) {
      assertEquals(r.size(), 1);
    }
    try (WalkResult r = w.walk(large, request(ds("a"), 10, 0, false))) {
      assertEquals(r.size(), 4);
    }
    try (WalkResult r = w.walk(small, request(ds("a"), 10, 0, false))) {
      assertEquals(degrees(r), Map.of(ds("b"), 1));
    }
  }

  @Test
  public void testFilterUsesEntityTypes() {
    // A dataJob consuming a dataset is not followed by the dataset-only rule set.
    GraphBuilder b = new GraphBuilder();
    b.addEdge(job("j"), "Consumes", ds("a"), null, null, null);
    b.addEdge(ds("b"), DOWNSTREAM_OF, ds("a"), null, null, null);
    WalkResult r = walker(b.build()).walk(request(ds("a"), 10, 0, false));
    assertEquals(degrees(r), Map.of(ds("b"), 1));
  }
}
