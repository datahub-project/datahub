package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.FamilyMigrationSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.List;
import org.testng.annotations.Test;

public class UsageStagingAdoptionTest {

  @Test
  public void testEveryStagingIndexOnTheTargetJoinsTheAliasOnceAndTheLiveOneStaysOpen()
      throws Exception {
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    // -000001 was adopted by an earlier, interrupted run; -000002 was restored by the operator and
    // never planned; the live staging index takes mirrored writes until the flip.
    target.on(
        "GET",
        "/_alias/tgt_datahub_usage_event",
        StubCluster.Reply.ok(
            "{\"tgt_datahub_usage_event-000001\":{\"aliases\":{\"tgt_datahub_usage_event\":{\"is_write_index\":true}}},"
                + "\"tgt_datahub_usage_migration-000001\":{\"aliases\":{\"tgt_datahub_usage_event\":{\"is_write_index\":false}}}}"));
    target.on(
        "GET",
        "/_resolve/index/tgt_datahub_usage_migration-*",
        StubCluster.Reply.ok(
            "{\"indices\":[{\"name\":\"tgt_datahub_usage_migration-000001\"},"
                + "{\"name\":\"tgt_datahub_usage_migration-000002\"},"
                + "{\"name\":\"tgt_datahub_usage_migration-live\"}]}"));
    target.on("POST", "/_aliases", StubCluster.Reply.ok("{}"));
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(
            IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration()),
            SearchComponent.USAGE,
            "tgt");
    SearchClusterRegistry.FamilyMigration migration =
        new SearchClusterRegistry.FamilyMigration(
            SearchComponent.USAGE,
            "primary",
            "secondary",
            FamilyMigrationSettings.builder().build(),
            names,
            null);

    new UsageStagingAdoption(target.opContext())
        .adopt(target.http(), migration, "tgt_datahub_usage_migration-live");

    List<String> added =
        target.calls("POST", "/_aliases").stream().map(StubCluster.Call::body).toList();
    assertEquals(added.size(), 2, added.toString());
    assertTrue(added.get(0).contains("\"index\":\"tgt_datahub_usage_migration-000002\""));
    assertTrue(added.get(1).contains("\"index\":\"tgt_datahub_usage_migration-live\""));
    assertFalse(
        added.toString().contains("migration-000001"), "a member already in place is not re-added");
    // ... but is settled again, in case the earlier run died between the join and the rest.
    assertEquals(
        target.calls("POST", "/_plugins/_ism/remove/tgt_datahub_usage_migration-000001").size(), 1);
    // Nothing is write-blocked and nothing is finished on an alias.
    assertTrue(target.calls.stream().noneMatch(call -> call.path().endsWith("/_settings")));
  }
}
