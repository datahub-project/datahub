package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;

import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.FamilyMigrationSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.List;
import org.testng.annotations.Test;

public class UsagePostFlipSweepTest {

  @Test
  public void testTheSweepCoversEveryGenerationAfterTheLastWriteIndexEvenOnceItIsGone()
      throws Exception {
    StubCluster oldCluster = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    // -000003 was the write index when the mirror ended; a lifecycle policy has since removed it,
    // and -000004 and -000005 hold the events of the bake. The staging member is the new cluster's
    // history and is never swept.
    oldCluster.on(
        "GET",
        "/_alias/tgt_datahub_usage_event",
        StubCluster.Reply.ok(
            "{\"tgt_datahub_usage_event-000005\":{\"aliases\":{\"tgt_datahub_usage_event\":{\"is_write_index\":true}}},"
                + "\"tgt_datahub_usage_event-000004\":{\"aliases\":{\"tgt_datahub_usage_event\":{}}},"
                + "\"tgt_datahub_usage_event-000002\":{\"aliases\":{\"tgt_datahub_usage_event\":{}}},"
                + "\"tgt_datahub_usage_migration-000001\":{\"aliases\":{\"tgt_datahub_usage_event\":{\"is_write_index\":false}}}}"));
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
    UsagePostFlipSweep sweep =
        new UsagePostFlipSweep(
            oldCluster.opContext(),
            mock(EntityService.class),
            mock(IndexConvention.class),
            "ClusterMigrationCopy_usage");

    List<String> toSweep =
        sweep.indicesToSweep(oldCluster.http(), migration, "tgt_datahub_usage_event-000003");

    assertEquals(
        toSweep, List.of("tgt_datahub_usage_event-000004", "tgt_datahub_usage_event-000005"));
  }
}
