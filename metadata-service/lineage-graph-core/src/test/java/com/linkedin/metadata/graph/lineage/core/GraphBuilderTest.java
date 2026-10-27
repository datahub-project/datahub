package com.linkedin.metadata.graph.lineage.core;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.TreeSet;
import org.testng.annotations.Test;

public class GraphBuilderTest {

  @Test
  public void testDictionaryRoundTripsAcrossBlocks() {
    GraphBuilder b = new GraphBuilder();
    TreeSet<String> urns = new TreeSet<>();
    Random rnd = new Random(7);
    for (int i = 0; i < 1000; i++) {
      String u =
          "urn:li:dataset:(urn:li:dataPlatform:snowflake,db.s"
              + rnd.nextInt(50)
              + ".t"
              + i
              + ",PROD)";
      urns.add(u);
      b.urn(u);
    }
    // Longer than the decoder's initial buffer, and multi-byte UTF-8.
    String longUrn = "urn:li:dataset:(urn:li:dataPlatform:hive," + "é".repeat(300) + ",PROD)";
    urns.add(longUrn);
    b.urn(longUrn);
    BaseGraph g = b.build();

    UrnDictionary d = g.urns();
    assertEquals(d.size(), urns.size());
    // Ids follow byte order, which matches String order for these ASCII-prefixed URNs.
    List<String> sorted = new ArrayList<>(urns);
    UrnDictionary.Cursor c = d.cursor();
    for (int id = 0; id < d.size(); id++) {
      assertEquals(d.get(id), sorted.get(id));
      assertEquals(c.get(id), sorted.get(id));
      assertEquals(d.find(sorted.get(id)), id);
    }
    assertEquals(d.find("urn:li:dataset:(urn:li:dataPlatform:snowflake,missing,PROD)"), -1);
    assertEquals(d.find("a"), -1);
  }

  @Test
  public void testNodeFactsAreStored() {
    String x = "urn:li:dataset:(urn:li:dataPlatform:snowflake,x,PROD)";
    String field = "urn:li:schemaField:(" + x + ",col)";
    String parentOnly = "urn:li:dataset:(urn:li:dataPlatform:hive,p,DEV)";
    String fieldOfParentOnly = "urn:li:schemaField:(" + parentOnly + ",col)";
    NodeFacts facts =
        urn -> {
          NodeFacts.Facts f = NodeFacts.URN_PARTS.of(urn);
          if (urn.startsWith("urn:li:schemaField:(")) {
            String parent = urn.substring("urn:li:schemaField:(".length(), urn.lastIndexOf(','));
            return new NodeFacts.Facts(f.entityType(), f.platform(), "PROD", parent, false);
          }
          return new NodeFacts.Facts(
              f.entityType(), f.platform(), urn.contains("DEV") ? "DEV" : null, null, false);
        };
    GraphBuilder b = new GraphBuilder(facts);
    b.addEdge(field, "DownstreamOf", fieldOfParentOnly, null, null, null);
    b.urn(x);
    b.urn("urn:li:corpuser:datahub");
    BaseGraph g = b.build();

    int f = g.urns().find(field);
    int user = g.urns().find("urn:li:corpuser:datahub");
    // A schema field's parent is a node even when no edge mentions it.
    int p = g.urns().find(parentOnly);
    assertEquals(g.nodeCount(), 5);
    assertEquals(g.parent(f), g.urns().find(x));
    assertEquals(g.parent(g.urns().find(fieldOfParentOnly)), p);
    assertEquals(g.parent(p), BaseGraph.NO_ID);
    assertEquals(g.entityTypes().name(g.entityType(f)), "schemaField");
    assertEquals(g.platforms().name(g.platform(f)), "urn:li:dataPlatform:snowflake");
    assertEquals(g.environments().name(g.environment(f)), "PROD");
    assertEquals(g.environments().name(g.environment(p)), "DEV");
    assertEquals(g.entityTypes().name(g.entityType(user)), "corpuser");
    assertEquals(g.platform(user), -1);
    assertEquals(g.environment(user), -1);
    assertFalse(g.unreadable(user));
  }

  @Test
  public void testUnreadableNodesAreMarked() {
    GraphBuilder b =
        new GraphBuilder(
            urn ->
                new NodeFacts.Facts(
                    NodeFacts.URN_PARTS.of(urn).entityType(),
                    null,
                    null,
                    null,
                    urn.contains("bad")));
    b.addEdge(ds("bad"), "DownstreamOf", ds("good"), null, null, null);
    BaseGraph g = b.build();
    assertTrue(g.unreadable(g.urns().find(ds("bad"))));
    assertFalse(g.unreadable(g.urns().find(ds("good"))));
    assertEquals(g.environment(g.urns().find(ds("bad"))), -1);
    assertEquals(b.unreadableNodes(), 1);
  }

  private static String ds(String name) {
    return "urn:li:dataset:(urn:li:dataPlatform:hive," + name + ",PROD)";
  }

  private static List<String> forwardRow(BaseGraph g, String urn) {
    int u = g.urns().find(urn);
    List<String> out = new ArrayList<>();
    for (int e = g.fwdStart(u); e < g.fwdEnd(u); e++) {
      int via = g.fwdVia(e);
      out.add(g.urns().get(g.fwdNbr(e)) + (via < 0 ? "" : " via " + g.urns().get(via)));
    }
    return out;
  }

  private static List<String> reverseRow(BaseGraph g, String urn) {
    int u = g.urns().find(urn);
    List<String> out = new ArrayList<>();
    for (int k = g.revStart(u); k < g.revEnd(u); k++) {
      int via = g.revVia(k);
      out.add(g.urns().get(g.revNbr(k)) + (via < 0 ? "" : " via " + g.urns().get(via)));
    }
    return out;
  }

  @Test
  public void testRowsAreInVisualizationPriorityOrder() {
    // Order: via ascending with no via last; updatedOn descending, missing first; createdOn
    // descending, missing first; then the neighbour.
    GraphBuilder b = new GraphBuilder();
    b.addEdge(ds("a"), "DownstreamOf", ds("n1"), null, 5L, 10L);
    b.addEdge(ds("a"), "DownstreamOf", ds("n2"), null, 5L, 20L);
    b.addEdge(ds("a"), "DownstreamOf", ds("n3"), null, 5L, null);
    b.addEdge(ds("a"), "DownstreamOf", ds("n4"), null, 7L, 20L);
    b.addEdge(ds("a"), "DownstreamOf", ds("n5"), ds("v2"), 1L, 1L);
    b.addEdge(ds("a"), "DownstreamOf", ds("n6"), ds("v1"), 1L, 1L);
    b.addEdge(ds("a"), "DownstreamOf", ds("n0"), null, 5L, 10L);
    BaseGraph g = b.build();
    assertEquals(
        forwardRow(g, ds("a")),
        List.of(
            ds("n6") + " via " + ds("v1"),
            ds("n5") + " via " + ds("v2"),
            ds("n3"),
            ds("n4"),
            ds("n2"),
            ds("n0"),
            ds("n1")));

    // Reverse rows use the same order, with the source as the neighbour.
    GraphBuilder r = new GraphBuilder();
    r.addEdge(ds("s1"), "DownstreamOf", ds("t"), null, null, 10L);
    r.addEdge(ds("s2"), "DownstreamOf", ds("t"), null, null, 30L);
    r.addEdge(ds("s3"), "DownstreamOf", ds("t"), ds("v"), null, 1L);
    BaseGraph rg = r.build();
    assertEquals(
        reverseRow(rg, ds("t")), List.of(ds("s3") + " via " + ds("v"), ds("s2"), ds("s1")));
  }

  @Test
  public void testDedupeKeepsDistinctVias() {
    GraphBuilder b = new GraphBuilder();
    // Two owners of the same edge: one edge.
    b.addEdge(ds("a"), "DownstreamOf", ds("b"), null, 1L, 1L);
    b.addEdge(ds("a"), "DownstreamOf", ds("b"), null, 2L, 2L);
    // The same edge through two vias: two edges.
    b.addEdge(ds("a"), "DownstreamOf", ds("c"), ds("v1"), null, null);
    b.addEdge(ds("a"), "DownstreamOf", ds("c"), ds("v2"), null, null);
    b.addEdge(ds("a"), "DownstreamOf", ds("c"), ds("v2"), null, null);
    // Another relationship type is another edge.
    b.addEdge(ds("a"), "TrainedBy", ds("b"), null, null, null);
    BaseGraph g = b.build();
    assertEquals(g.edgeCount(), 4);
    assertEquals(b.duplicates(), 2);
    assertEquals(
        forwardRow(g, ds("a")),
        List.of(ds("c") + " via " + ds("v1"), ds("c") + " via " + ds("v2"), ds("b"), ds("b")));
    int a = g.urns().find(ds("a"));
    assertEquals(g.revEnd(g.urns().find(ds("c"))) - g.revStart(g.urns().find(ds("c"))), 2);
    assertEquals(g.fwdEnd(a) - g.fwdStart(a), 4);
  }

  @Test
  public void testCsvLoader() throws IOException {
    Path csv = Files.createTempFile("edges", ".csv");
    try {
      Files.writeString(
          csv,
          "downstream_urn,upstream_urn\n"
              + "\"urn:li:dataset:(urn:li:dataPlatform:snowflake,b,PROD)\","
              + "\"urn:li:dataset:(urn:li:dataPlatform:snowflake,a,PROD)\"\n"
              + "\"urn:li:dataset:(urn:li:dataPlatform:snowflake,c,PROD)\","
              + "\"urn:li:dataset:(urn:li:dataPlatform:snowflake,a,PROD)\"\n");
      // Small buffers put every field and separator on a refill boundary at some point.
      for (int bufferSize : new int[] {1 << 20, 3, 7, 16, 61, 64}) {
        GraphBuilder b = new GraphBuilder();
        CsvEdgeLoader.load(csv, b, bufferSize);
        BaseGraph g = b.build();
        assertEquals(g.nodeCount(), 3, "buffer " + bufferSize);
        assertEquals(g.edgeCount(), 2, "buffer " + bufferSize);
        int a = g.urns().find("urn:li:dataset:(urn:li:dataPlatform:snowflake,a,PROD)");
        assertEquals(g.revEnd(a) - g.revStart(a), 2, "buffer " + bufferSize);
      }
    } finally {
      Files.delete(csv);
    }
  }
}
