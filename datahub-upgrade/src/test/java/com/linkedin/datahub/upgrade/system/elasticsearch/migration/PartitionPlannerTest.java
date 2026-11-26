package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class PartitionPlannerTest {
  private StubCluster source;

  @BeforeMethod
  public void setUp() throws Exception {
    source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
  }

  private static SourceIndexSet concrete(String name) {
    return new SourceIndexSet(name, IndexLayout.CONCRETE, List.of(name), name);
  }

  @Test
  public void testSmallTermsBecomeOnePartitionEach() throws Exception {
    source.on(
        "POST",
        "/graph_service_v1/_search",
        StubCluster.Reply.ok(
            "{\"aggregations\":{\"terms\":{\"sum_other_doc_count\":0,\"buckets\":["
                + "{\"key\":\"DownstreamOf\",\"doc_count\":5},{\"key\":\"OwnedBy\",\"doc_count\":3}]}}}"));
    source.on("POST", "/graph_service_v1/_count", call -> StubCluster.Reply.ok("{\"count\":0}"));

    List<CopyPartition> partitions =
        new PartitionPlanner(source.http(), 1_000)
            .plan(
                SearchComponent.GRAPH,
                List.of(concrete("graph_service_v1")),
                n -> "tgt_" + n,
                null);

    assertEquals(partitions.size(), 2);
    assertEquals(partitions.get(0).targetIndex(), "tgt_graph_service_v1");
    assertEquals(
        partitions.get(0).filter().path("term").path("relationshipType").asText(), "DownstreamOf");
    assertEquals(partitions.get(0).estimatedDocs(), 5);
    assertEquals(partitions.get(1).label(), "OwnedBy");
    // The plan reads the indices as they stand; nothing refreshes the source on the way.
    assertTrue(source.calls.stream().noneMatch(call -> call.path().endsWith("/_refresh")));
  }

  @Test
  public void testLargeTermIsSplitByTimeRangeWithAMissingBucket() throws Exception {
    source.on(
        "POST",
        "/graph_service_v1/_search",
        call -> {
          if (call.body().contains("\"terms\"")) {
            return StubCluster.Reply.ok(
                "{\"aggregations\":{\"terms\":{\"sum_other_doc_count\":0,\"buckets\":["
                    + "{\"key\":\"DownstreamOf\",\"doc_count\":30}]}}}");
          }
          return StubCluster.Reply.ok(
              "{\"aggregations\":{\"min\":{\"value\":0.0},\"max\":{\"value\":99.0}}}");
        });
    source.on(
        "POST",
        "/graph_service_v1/_count",
        call -> {
          if (call.body().contains("must_not") && !call.body().contains("range")) {
            // the term's documents without createdOn, then the top-level missing relationshipType
            return StubCluster.Reply.ok(
                call.body().contains("createdOn") ? "{\"count\":3}" : "{\"count\":0}");
          }
          if (call.body().contains("range")) {
            return StubCluster.Reply.ok("{\"count\":9}");
          }
          return StubCluster.Reply.ok("{\"count\":0}");
        });

    List<CopyPartition> partitions =
        new PartitionPlanner(source.http(), 10)
            .plan(SourceIndexSetSupport.GRAPH, List.of(concrete("graph_service_v1")), n -> n, null);

    // 27 documents with createdOn over [0, 99] at 10 per partition -> 3 ranges, plus the 3 without
    // it
    long ranges = partitions.stream().filter(p -> p.label().contains("createdOn[")).count();
    assertEquals(ranges, 3);
    assertTrue(partitions.stream().anyMatch(p -> p.label().endsWith("missing createdOn")));
    assertEquals(partitions.size(), 4);
    for (int i = 0; i < partitions.size(); i++) {
      assertEquals(partitions.get(i).id(), i);
    }
  }

  @Test
  public void testRolloverLayoutCopiesHistoricalGenerationsApartAndLiveOnesTogether()
      throws Exception {
    for (String backing : List.of("usage-000001", "usage-000002", "usage-000003")) {
      source.on("POST", "/" + backing + "/_count", StubCluster.Reply.ok("{\"count\":7}"));
    }
    SourceIndexSet set =
        new SourceIndexSet(
            "usage",
            IndexLayout.ROLLOVER_ALIAS,
            List.of("usage-000001", "usage-000002", "usage-000003"),
            "usage-000002");

    List<CopyPartition> partitions =
        new PartitionPlanner(source.http(), 1_000)
            .plan(SearchComponent.USAGE, List.of(set), n -> "staging-" + n, "staging-live");

    assertEquals(partitions.size(), 3);
    assertEquals(partitions.get(0).targetIndex(), "staging-usage-000001");
    assertEquals(partitions.get(1).targetIndex(), "staging-live");
    assertEquals(partitions.get(2).targetIndex(), "staging-live");
    assertNull(partitions.get(0).filter());
  }

  @Test
  public void testAConcreteUsageIndexIsTheWriteIndexAndCopiesIntoTheLiveStagingIndex()
      throws Exception {
    source.on("POST", "/usage/_count", StubCluster.Reply.ok("{\"count\":7}"));

    List<CopyPartition> partitions =
        new PartitionPlanner(source.http(), 1_000)
            .plan(SearchComponent.USAGE, List.of(concrete("usage")), n -> "staging-" + n, "live");

    // The mirror writes the same events there, so op_type=create deduplicates them exactly.
    assertEquals(partitions.size(), 1);
    assertEquals(partitions.get(0).sourceIndex(), "usage");
    assertEquals(partitions.get(0).targetIndex(), "live");
  }

  /** Alias so the planner call reads naturally in the range test. */
  private static final class SourceIndexSetSupport {
    static final SearchComponent GRAPH = SearchComponent.GRAPH;
  }
}
