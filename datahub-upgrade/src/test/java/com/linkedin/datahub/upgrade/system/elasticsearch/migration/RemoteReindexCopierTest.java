package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.RemoteCopySettings;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RemoteReindexCopierTest {
  private StubCluster target;
  private RemoteReindexCopier copier;

  @BeforeMethod
  public void setUp() throws Exception {
    target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    RemoteReindexCopier.Source source =
        RemoteReindexCopier.Source.of(
            "https://source:443",
            "svc",
            "secret",
            RemoteCopySettings.builder().external(true).build());
    copier = new RemoteReindexCopier(target.http(), source, "clustermigration|graph", 500, -1);
    // A fresh submit first looks for a task a crashed run may have left behind.
    target.on("GET", "/_tasks", StubCluster.Reply.ok("{\"nodes\":{}}"));
  }

  @Test
  public void testSubmitsPollsAndReportsTheTaskIdentity() throws Exception {
    AtomicInteger polls = new AtomicInteger();
    target.on("POST", "/_reindex", StubCluster.Reply.ok("{\"task\":\"n1:7\"}"));
    target.on(
        "GET",
        "/_tasks/n1:7",
        call ->
            polls.incrementAndGet() == 1
                ? StubCluster.Reply.ok(
                    "{\"completed\":false,\"task\":{\"status\":{\"total\":10,\"created\":4,\"version_conflicts\":0}}}")
                : StubCluster.Reply.ok(
                    "{\"completed\":true,\"response\":{\"total\":10,\"created\":7,\"version_conflicts\":3,\"noops\":0,\"failures\":[]}}"));
    List<String> cursors = new ArrayList<>();

    CrossClusterCopier.Result result =
        copier.copy(
            new CopyPartition(
                3, "src", "tgt", "src", target.mapper.createObjectNode().put("x", 1), 10, null),
            null,
            checkpoint -> cursors.add(checkpoint.cursor()),
            () -> false);

    assertTrue(result.isComplete());
    assertEquals(result.created(), 7);
    assertEquals(result.alreadyPresent(), 3);
    assertEquals(cursors, List.of("n1:7"));
    StubCluster.Call submit = target.calls("POST", "/_reindex").get(0);
    assertEquals(submit.opaqueId(), "clustermigration|graph|3");
    assertTrue(submit.query().contains("wait_for_completion=false"));
    assertTrue(
        submit
            .body()
            .contains(
                "\"remote\":{\"host\":\"https://source:443\",\"username\":\"svc\",\"password\":\"secret\",\"external\":true"));
    assertTrue(submit.body().contains("\"op_type\":\"create\""));
    assertTrue(submit.body().contains("\"query\":{\"x\":1}"));
  }

  @Test
  public void testReattachesToARunningTaskWhenTheCursorIsStale() throws Exception {
    target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    copier =
        new RemoteReindexCopier(
            target.http(),
            RemoteReindexCopier.Source.of(
                "https://source:443", null, null, RemoteCopySettings.builder().build()),
            "clustermigration|graph",
            500,
            -1);
    target.on("GET", "/_tasks/n1:1", new StubCluster.Reply(404, "{}"));
    target.on(
        "GET",
        "/_tasks",
        StubCluster.Reply.ok(
            "{\"nodes\":{\"n1\":{\"tasks\":{\"n1:2\":{\"headers\":{\"X-Opaque-Id\":\"clustermigration|graph|0\"}}}}}}"));
    target.on(
        "GET",
        "/_tasks/n1:2",
        StubCluster.Reply.ok(
            "{\"completed\":true,\"response\":{\"total\":2,\"created\":2,\"version_conflicts\":0,\"noops\":0,\"failures\":[]}}"));

    CrossClusterCopier.Result result =
        copier.copy(
            new CopyPartition(0, "src", "tgt", "src", null, 2, null),
            new CrossClusterCopier.Checkpoint("n1:1", 0, 0, 0, 0),
            checkpoint -> {},
            () -> false);

    assertTrue(result.isComplete());
    assertEquals(target.calls("POST", "/_reindex").size(), 0);
  }

  @Test
  public void testTaskFailuresMakeThePartitionIncomplete() throws Exception {
    target.on("POST", "/_reindex", StubCluster.Reply.ok("{\"task\":\"n1:9\"}"));
    target.on(
        "GET",
        "/_tasks/n1:9",
        StubCluster.Reply.ok(
            "{\"completed\":true,\"response\":{\"total\":5,\"created\":4,\"version_conflicts\":0,\"noops\":0,\"failures\":[{\"cause\":\"boom\"}]}}"));

    CrossClusterCopier.Result result =
        copier.copy(
            new CopyPartition(0, "src", "tgt", "src", null, 5, null),
            null,
            checkpoint -> {},
            () -> false);

    assertEquals(result.isComplete(), false);
    assertEquals(result.failures(), 2);
  }
}
