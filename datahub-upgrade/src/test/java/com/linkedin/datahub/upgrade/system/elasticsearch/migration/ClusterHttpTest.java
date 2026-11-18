package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import org.testng.annotations.Test;

public class ClusterHttpTest {

  @Test
  public void testOnlyAClearAnswerCountsAsExistsOrNot() throws Exception {
    StubCluster cluster = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    cluster.on("HEAD", "/present", StubCluster.Reply.ok(""));
    cluster.on("HEAD", "/absent", new StubCluster.Reply(404, ""));
    cluster.on("HEAD", "/refused", new StubCluster.Reply(403, "{\"error\":\"forbidden\"}"));
    cluster.on("HEAD", "/down", new StubCluster.Reply(503, ""));

    assertTrue(cluster.http().indexExists("present"));
    assertFalse(cluster.http().indexExists("absent"));
    // A refusal or an outage must not read as "the index is gone": a prepare would re-plan a
    // verified migration on that answer.
    assertThrows(IOException.class, () -> cluster.http().indexExists("refused"));
    assertThrows(IOException.class, () -> cluster.http().indexExists("down"));
  }

  @Test
  public void testADeleteThatFailsIsNotSwallowed() throws Exception {
    StubCluster cluster = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    cluster.on("DELETE", "/gone", new StubCluster.Reply(404, "{}"));
    cluster.on("DELETE", "/kept", new StubCluster.Reply(403, "{\"error\":\"forbidden\"}"));

    cluster.http().deleteIndex("gone");
    assertThrows(IOException.class, () -> cluster.http().deleteIndex("kept"));
  }
}
