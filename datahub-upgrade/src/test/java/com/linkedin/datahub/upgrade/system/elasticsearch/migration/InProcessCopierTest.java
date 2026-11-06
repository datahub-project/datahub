package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.ArrayList;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class InProcessCopierTest {
  private StubCluster source;
  private StubCluster target;
  private List<String> cursors;

  @BeforeMethod
  public void setUp() throws Exception {
    source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    source.on("DELETE", "/_search/scroll", StubCluster.Reply.ok("{\"succeeded\":true}"));
    cursors = new ArrayList<>();
  }

  private InProcessCopier copier(int pageSize) {
    return new InProcessCopier(
        source.http(), target.http(), pageSize, "5m", 1, InProcessCopier.Pacer.NONE);
  }

  private static CopyPartition partition(long estimatedDocs) {
    return new CopyPartition(0, "src", "tgt", "src", null, estimatedDocs, null);
  }

  private static String hit(String id) {
    return "{\"_id\":\"" + id + "\",\"_source\":{\"v\":\"" + id + "\"}}";
  }

  private static String page(String scrollId, boolean withTotal, String... hits) {
    return "{\"_scroll_id\":\""
        + scrollId
        + "\",\"hits\":{"
        + (withTotal ? "\"total\":{\"value\":3}," : "")
        + "\"hits\":["
        + String.join(",", hits)
        + "]}}";
  }

  @Test
  public void testScrollsThroughTheSourceAndCountsEveryBulkItem() throws Exception {
    source.on("POST", "/src/_search", StubCluster.Reply.ok(page("s1", true, hit("a"), hit("b"))));
    source.on("POST", "/_search/scroll", StubCluster.Reply.ok(page("s2", false, hit("c"))));
    target.on(
        "POST",
        "/_bulk",
        call ->
            call.body().contains("\"_id\":\"a\"")
                ? StubCluster.Reply.ok(
                    "{\"items\":[{\"create\":{\"status\":201}},{\"create\":{\"status\":409}}]}")
                : StubCluster.Reply.ok("{\"items\":[{\"create\":{\"status\":201}}]}"));

    CrossClusterCopier.Result result =
        copier(2)
            .copy(partition(3), null, checkpoint -> cursors.add(checkpoint.cursor()), () -> false);

    assertTrue(result.isComplete());
    assertEquals(result.total(), 3);
    assertEquals(result.read(), 3);
    assertEquals(result.created(), 2);
    assertEquals(result.alreadyPresent(), 1);
    StubCluster.Call open = source.calls("POST", "/src/_search").get(0);
    assertTrue(open.query().contains("scroll=5m"));
    assertTrue(open.body().contains("\"sort\":[\"_doc\"]"));
    assertTrue(open.body().contains("\"track_total_hits\":true"));
    assertTrue(
        source.calls("POST", "/_search/scroll").get(0).body().contains("\"scroll_id\":\"s1\""));
    assertTrue(
        target
            .calls("POST", "/_bulk")
            .get(0)
            .body()
            .startsWith("{\"create\":{\"_index\":\"tgt\",\"_id\":\"a\"}}\n{\"v\":\"a\"}\n"));
    // One checkpoint after the full first page, none after the short last page; scroll cleared.
    assertEquals(cursors, List.of("s1"));
    assertEquals(source.calls("DELETE", "/_search/scroll").size(), 1);
  }

  @Test
  public void testPausesOnRequestAndResumesTheScrollWithItsCounts() throws Exception {
    source.on("POST", "/src/_search", StubCluster.Reply.ok(page("s1", true, hit("a"), hit("b"))));
    source.on("POST", "/_search/scroll", StubCluster.Reply.ok(page("s1", true, hit("c"))));
    target.on(
        "POST",
        "/_bulk",
        call ->
            StubCluster.Reply.ok(
                call.body().contains("\"_id\":\"c\"")
                    ? "{\"items\":[{\"create\":{\"status\":201}}]}"
                    : "{\"items\":[{\"create\":{\"status\":201}},{\"create\":{\"status\":409}}]}"));
    List<CrossClusterCopier.Checkpoint> checkpoints = new ArrayList<>();

    CrossClusterCopier.Result paused =
        copier(2).copy(partition(3), null, checkpoints::add, () -> true);
    assertTrue(paused.paused());
    assertFalse(paused.isComplete());
    assertEquals(paused.read(), 2);
    assertEquals(checkpoints, List.of(new CrossClusterCopier.Checkpoint("s1", 2, 1, 1, 0)));
    // A paused partition keeps its scroll open for the next run.
    assertEquals(source.calls("DELETE", "/_search/scroll").size(), 0);

    CrossClusterCopier.Result resumed =
        copier(2).copy(partition(3), checkpoints.get(0), checkpoints::add, () -> false);
    assertTrue(resumed.isComplete());
    assertEquals(resumed.read(), 3, "the resumed attempt carries what the paused one did");
    assertEquals(resumed.created(), 2);
    assertEquals(resumed.alreadyPresent(), 1);
    assertEquals(
        source.calls("POST", "/src/_search").size(), 1, "the resume did not reopen the scroll");
  }

  @Test
  public void testAnExpiredScrollRestartsThePartitionFromNothing() throws Exception {
    source.on("POST", "/_search/scroll", new StubCluster.Reply(404, "{\"error\":\"no context\"}"));
    source.on("POST", "/src/_search", StubCluster.Reply.ok(page("s9", true, hit("a"))));
    target.on(
        "POST", "/_bulk", StubCluster.Reply.ok("{\"items\":[{\"create\":{\"status\":409}}]}"));

    CrossClusterCopier.Result result =
        copier(10)
            .copy(
                partition(1),
                new CrossClusterCopier.Checkpoint("stale", 7, 7, 0, 0),
                checkpoint -> {},
                () -> false);

    assertTrue(result.isComplete());
    assertEquals(result.read(), 1, "the stale attempt's counts are not carried into a restart");
    assertEquals(result.alreadyPresent(), 1);
    assertEquals(source.calls("POST", "/src/_search").size(), 1);
  }

  @Test
  public void testACursorTheSourceCannotParseRestartsThePartition() throws Exception {
    // A remote reindex task id persisted as the cursor before the mode changed to in-process.
    source.on(
        "POST",
        "/_search/scroll",
        new StubCluster.Reply(
            400,
            "{\"error\":{\"type\":\"illegal_argument_exception\",\"reason\":\"Cannot parse scroll"
                + " id\"},\"status\":400}"));
    source.on("POST", "/src/_search", StubCluster.Reply.ok(page("s9", true, hit("a"))));
    target.on(
        "POST", "/_bulk", StubCluster.Reply.ok("{\"items\":[{\"create\":{\"status\":201}}]}"));

    CrossClusterCopier.Result result =
        copier(10)
            .copy(
                partition(1),
                new CrossClusterCopier.Checkpoint("node:4711", 0, 0, 0, 0),
                checkpoint -> {},
                () -> false);

    assertTrue(result.isComplete());
    assertEquals(result.created(), 1);
    assertEquals(source.calls("POST", "/src/_search").size(), 1);
  }

  @Test
  public void testAGuardedPartitionReplacesOlderDocumentsAndLeavesNewerOnes() throws Exception {
    source.on(
        "POST",
        "/src/_search",
        StubCluster.Reply.ok(
            page(
                "s1",
                true,
                "{\"_id\":\"a\",\"_source\":{\"updatedOn\":5,\"v\":\"a\"}}",
                "{\"_id\":\"b\",\"_source\":{\"updatedOn\":6,\"v\":\"b\"}}",
                "{\"_id\":\"c\",\"_source\":{\"updatedOn\":7,\"v\":\"c\"}}",
                "{\"_id\":\"d\",\"_source\":{\"createdOn\":9,\"v\":\"d\"}}")));
    // a: the target's copy was older and is replaced; b: the target's is current and stays;
    // c: the target lacked it and the upsert creates it; d: a graph edge in the window by
    // createdOn alone, so the guard is null and the script noops when the content matches.
    target.on(
        "POST",
        "/_bulk",
        StubCluster.Reply.ok(
            "{\"items\":[{\"update\":{\"status\":200,\"result\":\"updated\"}},"
                + "{\"update\":{\"status\":200,\"result\":\"noop\"}},"
                + "{\"update\":{\"status\":201,\"result\":\"created\"}},"
                + "{\"update\":{\"status\":200,\"result\":\"noop\"}}]}"));

    CrossClusterCopier.Result result =
        copier(10)
            .copy(
                new CopyPartition(0, "src", "tgt", "window", null, -1, "updatedOn"),
                null,
                checkpoint -> {},
                () -> false);

    assertTrue(result.isComplete());
    // A replace counts as written, like a create: the target lacked the source's version of both.
    assertEquals(result.created(), 2);
    assertEquals(result.alreadyPresent(), 2);
    String body = target.calls("POST", "/_bulk").get(0).body();
    assertTrue(
        body.startsWith(
            "{\"update\":{\"_index\":\"tgt\",\"_id\":\"a\",\"retry_on_conflict\":3}}\n"
                + "{\"scripted_upsert\":true,\"script\":{\"source\":\""),
        body);
    assertTrue(
        body.contains(
            "\"params\":{\"field\":\"updatedOn\",\"value\":5,\"doc\":{\"updatedOn\":5,\"v\":\"a\"}}},\"upsert\":{}}\n"),
        body);
    assertTrue(
        body.contains(
            "\"params\":{\"field\":\"updatedOn\",\"value\":null,\"doc\":{\"createdOn\":9,\"v\":\"d\"}}},\"upsert\":{}}\n"),
        body);
    assertTrue(body.contains("boolean sameStamp = (mine == null && params.value == null)"), body);
    assertTrue(body.contains(InProcessCopier.GUARDED_REPLACE_SCRIPT.replace("\"", "\\\"")), body);
  }

  @Test
  public void testDocumentsReachTheTargetAsUtf8() throws Exception {
    // One character inside Latin-1 and one beyond it: a body sent in any other encoding arrives as
    // invalid bytes for the first and as '?' for the second.
    String query = "requête n°1 日本語";
    source.on(
        "POST",
        "/src/_search",
        StubCluster.Reply.ok(
            page("s1", true, "{\"_id\":\"a\",\"_source\":{\"query\":\"" + query + "\"}}")));
    target.on(
        "POST", "/_bulk", StubCluster.Reply.ok("{\"items\":[{\"create\":{\"status\":201}}]}"));

    copier(10).copy(partition(1), null, checkpoint -> {}, () -> false);

    String body = target.calls("POST", "/_bulk").get(0).body();
    assertTrue(body.contains("{\"query\":\"" + query + "\"}"), body);
  }

  @Test
  public void testRejectedItemsMakeThePartitionIncomplete() throws Exception {
    source.on("POST", "/src/_search", StubCluster.Reply.ok(page("s1", true, hit("a"))));
    target.on(
        "POST",
        "/_bulk",
        StubCluster.Reply.ok(
            "{\"errors\":true,\"items\":[{\"create\":{\"status\":400,\"error\":{\"type\":\"mapper_parsing_exception\"}}}]}"));

    CrossClusterCopier.Result result =
        copier(10).copy(partition(1), null, checkpoint -> {}, () -> false);

    assertFalse(result.isComplete());
    assertEquals(result.failures(), 1);
  }
}
