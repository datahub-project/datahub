package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.Map;
import org.testng.annotations.Test;

public class MirroredWriteLogReplayerTest {

  @Test
  public void testReconcilesEveryDocumentALoggedOperationCouldHaveTouched() throws Exception {
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    StubCluster source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    target.on("HEAD", "/log", StubCluster.Reply.ok(""));
    target.on("POST", "/log/_refresh", StubCluster.Reply.ok("{}"));
    target.on(
        "POST",
        "/log/_search",
        StubCluster.Reply.ok(
            "{\"hits\":{\"hits\":["
                + "{\"_source\":{\"ts\":10,\"kind\":\"DELETE_BY_ID\",\"index\":\"g\",\"id\":\"e1\"},\"sort\":[10,0]},"
                + "{\"_source\":{\"ts\":11,\"kind\":\"DELETE_BY_QUERY\",\"index\":\"g\",\"query\":\"{\\\"term\\\":{\\\"a\\\":1}}\"},\"sort\":[11,0]},"
                + "{\"_source\":{\"ts\":12,\"kind\":\"UPDATE_BY_QUERY\",\"index\":\"g\",\"query\":\"{\\\"match_all\\\":{}}\",\"script\":\"{\\\"source\\\":\\\"x\\\"}\"},\"sort\":[12,0]},"
                // A graph edge id: Base64, so it travels in a request body and never in a path.
                + "{\"_source\":{\"ts\":13,\"kind\":\"DELETE_BY_ID\",\"index\":\"g\",\"id\":\"adGOhD+hoSCk69TnZfQ/Bw==\"},\"sort\":[13,0]}"
                + "]}}"));
    // Both by-query entries match the same two documents on the target.
    target.on(
        "POST",
        "/g/_search",
        StubCluster.Reply.ok(
            "{\"_scroll_id\":\"sc\",\"hits\":{\"hits\":[{\"_id\":\"e2\"},{\"_id\":\"e3\"}]}}"));
    target.on("DELETE", "/_search/scroll", StubCluster.Reply.ok("{\"succeeded\":true}"));
    // What the target holds now, with the version each document is held at; e3 is absent.
    target.on(
        "POST",
        "/g/_mget",
        StubCluster.Reply.ok(
            "{\"docs\":["
                + "{\"_id\":\"e1\",\"found\":true,\"_seq_no\":5,\"_primary_term\":1},"
                + "{\"_id\":\"e2\",\"found\":true,\"_seq_no\":6,\"_primary_term\":1},"
                + "{\"_id\":\"e3\",\"found\":false},"
                + "{\"_id\":\"adGOhD+hoSCk69TnZfQ/Bw==\",\"found\":true,\"_seq_no\":7,\"_primary_term\":1}"
                + "]}"));
    // The source still holds e1 (deleted and written again) and e3; e2 and the edge are gone.
    source.on(
        "POST",
        "/gsrc/_mget",
        StubCluster.Reply.ok(
            "{\"docs\":["
                + "{\"_id\":\"e1\",\"found\":true,\"_source\":{\"v\":1}},"
                + "{\"_id\":\"e2\",\"found\":false},"
                + "{\"_id\":\"e3\",\"found\":true,\"_source\":{\"v\":3}},"
                + "{\"_id\":\"adGOhD+hoSCk69TnZfQ/Bw==\",\"found\":false}"
                + "]}"));
    // e2 was written again by the mirror between the lookup and the bulk: its delete conflicts.
    target.on(
        "POST",
        "/_bulk",
        StubCluster.Reply.ok(
            "{\"errors\":true,\"items\":["
                + "{\"index\":{\"_id\":\"e1\",\"status\":200}},"
                + "{\"delete\":{\"_id\":\"e2\",\"status\":409,\"error\":{\"type\":\"version_conflict_engine_exception\"}}},"
                + "{\"create\":{\"_id\":\"e3\",\"status\":201}},"
                + "{\"delete\":{\"_id\":\"adGOhD+hoSCk69TnZfQ/Bw==\",\"result\":\"deleted\",\"status\":200}}"
                + "]}"));

    long replayed =
        new MirroredWriteLogReplayer(target.http(), source.http(), Map.of("g", "gsrc"))
            .replay("log", 5);

    assertEquals(replayed, 4);
    String logSearch = target.calls("POST", "/log/_search").get(0).body();
    assertTrue(logSearch.contains("\"gte\":5"));
    // Paged in time order with a tiebreak, so search_after holds on any shard count.
    assertTrue(logSearch.contains("\"sort\":[{\"ts\":\"asc\"},\"_doc\"]"), logSearch);
    // The query entries are run on the target only to find what they touch.
    assertEquals(target.calls("POST", "/g/_search").size(), 2);
    assertTrue(target.calls("POST", "/g/_search").get(0).body().contains("\"term\":{\"a\":1}"));
    assertTrue(target.calls("POST", "/g/_delete_by_query").isEmpty());
    assertTrue(target.calls("POST", "/g/_update_by_query").isEmpty());
    // Every touched id is looked up once on each side, in one request each; the target lookup
    // only needs the versions.
    String lookup = source.calls("POST", "/gsrc/_mget").get(0).body();
    assertEquals(source.calls("POST", "/gsrc/_mget").size(), 1);
    assertTrue(lookup.contains("\"e1\""));
    assertTrue(lookup.contains("\"e2\""));
    assertTrue(lookup.contains("\"e3\""));
    assertTrue(lookup.contains("\"adGOhD+hoSCk69TnZfQ/Bw==\""));
    assertFalse(lookup.contains("\"e2\",\"e2\""), "each id once");
    assertEquals(target.calls("POST", "/g/_mget").size(), 1);
    assertEquals(target.calls("POST", "/g/_mget").get(0).query(), "_source=false");
    // What the source holds is written again from it, what it lacks is deleted, each conditioned
    // on the version the target was seen at; a document the target lacks is created.
    String bulk = target.calls("POST", "/_bulk").get(0).body();
    assertTrue(
        bulk.contains(
            "{\"index\":{\"_index\":\"g\",\"_id\":\"e1\",\"if_seq_no\":5,\"if_primary_term\":1}}\n{\"v\":1}\n"),
        bulk);
    assertTrue(
        bulk.contains(
            "{\"delete\":{\"_index\":\"g\",\"_id\":\"e2\",\"if_seq_no\":6,\"if_primary_term\":1}}\n"),
        bulk);
    assertTrue(bulk.contains("{\"create\":{\"_index\":\"g\",\"_id\":\"e3\"}}\n{\"v\":3}\n"), bulk);
    assertTrue(
        bulk.contains(
            "{\"delete\":{\"_index\":\"g\",\"_id\":\"adGOhD+hoSCk69TnZfQ/Bw==\",\"if_seq_no\":7,\"if_primary_term\":1}}\n"),
        bulk);
  }

  @Test
  public void testNoLogMeansNothingToReplay() throws Exception {
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    StubCluster source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    target.on("HEAD", "/log", new StubCluster.Reply(404, ""));

    assertEquals(
        new MirroredWriteLogReplayer(target.http(), source.http(), Map.of()).replay("log", 0), 0);
  }
}
