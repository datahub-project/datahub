package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.io.IOException;
import org.testng.annotations.Test;

public class StagingIndexAdopterTest {

  @Test
  public void testFinishedStagingIndexJoinsTheAliasAsANonWriteMemberWithoutIsm() throws Exception {
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    target.on("POST", "/_aliases", StubCluster.Reply.ok("{}"));
    target.on("POST", "/_plugins/_ism/remove/stg", StubCluster.Reply.ok("{}"));

    new StagingIndexAdopter(target.http()).adopt(IndexLayout.ROLLOVER_ALIAS, "usage", "stg", true);

    String aliases = target.calls("POST", "/_aliases").get(0).body();
    assertTrue(
        aliases.contains(
            "\"add\":{\"index\":\"stg\",\"alias\":\"usage\",\"is_write_index\":false}"));
    assertEquals(target.calls("POST", "/_plugins/_ism/remove/stg").size(), 1);
    // Never write-blocked: a rollback mirrors into a live staging index by name again.
    assertEquals(target.calls("PUT", "/stg/_settings").size(), 0);
  }

  @Test
  public void testARefusedPolicyRemovalFailsTheAdoption() throws Exception {
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    target.on("POST", "/_aliases", StubCluster.Reply.ok("{}"));
    target.on(
        "POST",
        "/_plugins/_ism/remove/stg",
        new StubCluster.Reply(403, "{\"error\":\"no permissions\"}"));

    assertThrows(
        IOException.class,
        () ->
            new StagingIndexAdopter(target.http())
                .adopt(IndexLayout.ROLLOVER_ALIAS, "usage", "stg", true));
  }

  @Test
  public void testSettlingAMemberAlreadyInPlaceCompletesAnInterruptedAdoption() throws Exception {
    // The join succeeded on an earlier run that died before the rest; a retry must not add the
    // member again, but must still finish it.
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    target.on("PUT", "/stg/_settings", StubCluster.Reply.ok("{}"));

    new StagingIndexAdopter(target.http()).settle(IndexLayout.DATA_STREAM, "stg", true);

    assertEquals(target.calls("POST", "/_data_stream/_modify").size(), 0);
    assertTrue(
        target
            .calls("PUT", "/stg/_settings")
            .get(0)
            .body()
            .contains("\"index.lifecycle.indexing_complete\":true"));
  }

  @Test
  public void testLiveStagingIndexJoinsTheDataStreamWithoutBeingClosedOff() throws Exception {
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    target.on("POST", "/_data_stream/_modify", StubCluster.Reply.ok("{}"));

    new StagingIndexAdopter(target.http())
        .adopt(IndexLayout.DATA_STREAM, "usage", "stg-live", false);

    String body = target.calls("POST", "/_data_stream/_modify").get(0).body();
    assertTrue(
        body.contains("\"add_backing_index\":{\"data_stream\":\"usage\",\"index\":\"stg-live\"}"));
    assertEquals(target.calls("PUT", "/stg-live/_settings").size(), 0);
  }
}
