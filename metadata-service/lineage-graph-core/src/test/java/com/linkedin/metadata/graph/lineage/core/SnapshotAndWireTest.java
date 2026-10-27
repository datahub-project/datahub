package com.linkedin.metadata.graph.lineage.core;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.testng.annotations.Test;

public class SnapshotAndWireTest {

  private static final String DOWNSTREAM_OF = "DownstreamOf";
  private static final List<EdgeFilter.Rule> RULES =
      List.of(
          new EdgeFilter.Rule("dataset", DOWNSTREAM_OF, EdgeFilter.Direction.INCOMING, "dataset"),
          new EdgeFilter.Rule("dataJob", "Produces", EdgeFilter.Direction.OUTGOING, "dataset"));

  private static String ds(String name) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)";
  }

  private static String job(String name) {
    return "urn:li:dataJob:(urn:li:dataFlow:(airflow,flow,PROD)," + name + ")";
  }

  /** A random DAG with vias, timestamps and duplicate edges. */
  private static BaseGraph randomGraph(long seed) {
    Random rnd = new Random(seed);
    GraphBuilder b = new GraphBuilder();
    for (int i = 1; i < 400; i++) {
      int parents = 1 + rnd.nextInt(3);
      for (int p = 0; p < parents; p++) {
        int parent = rnd.nextInt(i);
        boolean withVia = rnd.nextInt(5) == 0;
        b.addEdge(
            ds("t" + i),
            DOWNSTREAM_OF,
            ds("t" + parent),
            withVia ? job("j" + rnd.nextInt(10)) : null,
            rnd.nextBoolean() ? (long) rnd.nextInt(1000) : null,
            rnd.nextBoolean() ? (long) rnd.nextInt(1000) : null);
      }
    }
    for (int j = 0; j < 10; j++) {
      b.addEdge(job("j" + j), "Produces", ds("out" + j), null, null, null);
    }
    return b.build();
  }

  private static WalkResult walk(GraphSnapshot g, WalkRequest req) {
    return new ImpactWalker().walk(g, req);
  }

  private static List<List<String>> paths(WalkResult r, int i) {
    List<List<String>> out = new ArrayList<>();
    for (int[] p : r.paths(i)) {
      List<String> l = new ArrayList<>();
      for (int n : p) {
        l.add(r.graph().urn(n));
      }
      out.add(l);
    }
    return out;
  }

  @Test
  public void testWireRoundTripMatchesWalkResult() throws Exception {
    for (long seed = 1; seed <= 5; seed++) {
      GraphSnapshot snapshot = new GraphSnapshot(randomGraph(seed), 1_700_000_000_000L, 0, null);
      try (WalkResult r =
          walk(snapshot, new WalkRequest(ds("t0"), RULES, 1000, 0, false, null, null, 0))) {
        assertTrue(r.size() > 50, "seed " + seed);
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        WireCodec.encode(r, 123, 7, bytes);
        ImpactResponse resp = WireCodec.decode(bytes.toByteArray());

        assertEquals(resp.status(), WalkResult.Status.COMPLETE);
        assertEquals(resp.hops(), r.hops());
        assertEquals(resp.walkMicros(), 123);
        assertEquals(resp.lagMillis(), 7);
        assertEquals(resp.watermark(), 1_700_000_000_000L);
        assertEquals(resp.relationships().size(), r.size());
        for (int i = 0; i < r.size(); i++) {
          ImpactResponse.Relationship rel = resp.relationships().get(i);
          assertEquals(rel.urn(), r.urn(i));
          assertEquals(rel.type(), r.relationshipType(i));
          assertEquals(rel.degree(), r.degree(i));
          assertEquals(rel.degrees(), r.degrees(i));
          assertEquals(rel.explored(), r.explored(i));
          assertEquals(rel.paths(), paths(r, i), rel.urn());
          // Edge details are looked up per page in GMS, never sent by the service.
          assertFalse(rel.manual());
          assertNull(rel.createdOn());
          assertNull(rel.updatedOn());
          assertNull(rel.createdActor());
          assertNull(rel.updatedActor());
        }
      }
    }
  }

  @Test
  public void testSearchWireRoundTrip() throws Exception {
    try (WalkResult r =
        walk(
            GraphSnapshot.of(randomGraph(3)),
            new WalkRequest(ds("t0"), RULES, 1000, 0, false, null, null, 0))) {
      // The full answer carries the walk as /v1/impact would.
      ByteArrayOutputStream full = new ByteArrayOutputStream();
      WireCodec.encodeSearchFull(r, 123, 45, 7, 6, full);
      ImpactSearchResponse fullResp = WireCodec.decodeSearch(full.toByteArray());
      assertEquals(fullResp.mode(), ImpactSearchResponse.Mode.FULL);
      assertEquals(fullResp.prefiltered(), 6);
      assertEquals(fullResp.searchMicros(), 45);
      assertEquals(fullResp.full().relationships().size(), r.size());
      assertEquals(fullResp.full().relationships().get(3).paths(), paths(r, 3));

      // A page: rows (with a urn the graph does not hold), paths, and facets in their order.
      ImpactSearchResponse page =
          new ImpactSearchResponse(
              ImpactSearchResponse.Mode.LIGHTNING,
              null,
              WalkResult.Status.MAX_RELATIONS,
              true,
              r.hops(),
              r.edgesScanned(),
              123,
              45,
              9,
              7,
              r.size(),
              40,
              31,
              List.of(
                  new ImpactSearchResponse.Facet(
                      List.of("urn:li:dataPlatform:b", "urn:li:dataPlatform:a"),
                      new long[] {30, 1},
                      true),
                  new ImpactSearchResponse.Facet(List.of("dataset"), new long[] {31}, false),
                  new ImpactSearchResponse.Facet(List.of(), new long[0], false)),
              List.of(
                  new ImpactSearchResponse.Row(r.urn(1), r.degree(1), paths(r, 1)),
                  new ImpactSearchResponse.Row("urn:li:dataset:(x,converted,PROD)", 2, List.of())));
      ByteArrayOutputStream bytes = new ByteArrayOutputStream();
      WireCodec.encodeSearch(page, bytes);
      ImpactSearchResponse decoded = WireCodec.decodeSearch(bytes.toByteArray());
      assertEquals(decoded.mode(), ImpactSearchResponse.Mode.LIGHTNING);
      assertEquals(decoded.status(), WalkResult.Status.MAX_RELATIONS);
      assertTrue(decoded.partial());
      assertEquals(decoded.walked(), r.size());
      assertEquals(decoded.prefiltered(), 40);
      assertEquals(decoded.total(), 31);
      assertEquals(decoded.rows(), page.rows());
      assertEquals(decoded.facets().size(), 3);
      for (int f = 0; f < 3; f++) {
        assertEquals(decoded.facets().get(f).values(), page.facets().get(f).values());
        assertEquals(decoded.facets().get(f).counts(), page.facets().get(f).counts());
        assertEquals(
            decoded.facets().get(f).countedAfterLastNew(),
            page.facets().get(f).countedAfterLastNew());
      }
    }
  }

  @Test
  public void testSnapshotPublishLoadAndPrune() throws Exception {
    BaseGraph graph = randomGraph(9);
    WalkRequest req = new WalkRequest(ds("t0"), RULES, 1000, 0, false, null, null, 0);
    Map<String, Integer> before = degrees(walk(GraphSnapshot.of(graph), req));

    Path root = Files.createTempDirectory("lg-snap");
    try {
      assertTrue(SnapshotIO.latest(root).isEmpty());
      for (long builtAt = 1000; builtAt <= 4000; builtAt += 1000) {
        SnapshotIO.publish(
            graph, new SnapshotIO.Meta(builtAt, builtAt - 10, Map.of("scannedDocs", "7")), root);
      }
      // LATEST names the newest; one previous snapshot is kept for rollback.
      Path latest = SnapshotIO.latest(root).orElseThrow();
      assertEquals(latest.getFileName().toString(), "4000");
      assertTrue(Files.isDirectory(root.resolve("3000")));
      assertFalse(Files.exists(root.resolve("2000")));
      assertFalse(Files.exists(root.resolve("1000")));

      Arena arena = Arena.ofShared();
      SnapshotIO.Loaded loaded = SnapshotIO.load(latest, arena);
      assertEquals(loaded.meta().builtAtMillis(), 4000);
      assertEquals(loaded.meta().pointInTimeMillis(), 3990);
      assertEquals(loaded.meta().stats().get("scannedDocs"), "7");
      BaseGraph g = loaded.graph();
      assertEquals(g.nodeCount(), graph.nodeCount());
      assertEquals(g.edgeCount(), graph.edgeCount());
      assertEquals(g.memory(), graph.memory());
      for (int n = 0; n < g.nodeCount(); n++) {
        assertEquals(g.meta(n), graph.meta(n));
        assertEquals(g.parent(n), graph.parent(n));
      }
      GraphSnapshot snapshot = new GraphSnapshot(g, 4000, 3990, arena);
      assertEquals(degrees(walk(snapshot, req)), before);
      snapshot.retire(null);
      assertFalse(arena.scope().isAlive());
    } finally {
      try (Stream<Path> s = Files.walk(root)) {
        s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }

  @Test
  public void testRetiredSnapshotClosesAfterLastWalk() throws Exception {
    Arena arena = Arena.ofShared();
    BaseGraph graph = randomGraph(4);
    Path root = Files.createTempDirectory("lg-snap");
    try {
      Path dir = SnapshotIO.publish(graph, new SnapshotIO.Meta(1, 1, Map.of()), root);
      GraphSnapshot old = new GraphSnapshot(SnapshotIO.load(dir, arena).graph(), 1, 1, arena);
      WalkRequest req = new WalkRequest(ds("t0"), RULES, 1000, 0, false, null, null, 0);

      // A walk holds the old snapshot while a newer one is published.
      assertTrue(old.tryAcquire());
      WalkResult running = walk(old, req);
      int[] closed = {0};
      old.retire(() -> closed[0]++);
      assertTrue(old.retired());
      assertTrue(arena.scope().isAlive(), "still held by the running walk");
      assertFalse(old.tryAcquire(), "new walks must take the new snapshot");
      assertTrue(running.size() > 50);
      assertEquals(running.urn(0), running.graph().urn(running.node(0)));
      running.close();
      old.release();

      assertFalse(arena.scope().isAlive());
      assertEquals(closed[0], 1);
      // The mapping is gone: reading through the old snapshot fails instead of reading freed
      // memory.
      assertThrows(IllegalStateException.class, () -> old.base().meta(0));
    } finally {
      try (Stream<Path> s = Files.walk(root)) {
        s.sorted(Comparator.reverseOrder()).forEach(p -> p.toFile().delete());
      }
    }
  }

  private static Map<String, Integer> degrees(WalkResult r) {
    try (r) {
      Map<String, Integer> m = new HashMap<>();
      for (int i = 0; i < r.size(); i++) {
        m.put(r.urn(i), r.degree(i));
      }
      return m;
    }
  }
}
