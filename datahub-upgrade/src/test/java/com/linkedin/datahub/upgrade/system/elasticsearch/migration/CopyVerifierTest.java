package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;

import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeReport;
import com.linkedin.gms.factory.search.SearchClusterRegistry;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.FamilyMigrationSettings;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.graph.elastic.ElasticSearchGraphService;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ClusterMigrationState;
import com.linkedin.metadata.search.elasticsearch.update.MigrationIndexNames;
import com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService;
import com.linkedin.metadata.timeseries.elastic.ElasticSearchTimeseriesAspectService;
import com.linkedin.metadata.utils.elasticsearch.IndexConventionImpl;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import org.testng.annotations.Test;

public class CopyVerifierTest {

  @Test
  public void testADocumentStillMissingAfterTheRechecksCountsAndIsRecorded() throws Exception {
    StubCluster source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    source.on("POST", "/graph_service_v1/_refresh", StubCluster.Reply.ok("{}"));
    source.on("POST", "/graph_service_v1/_count", StubCluster.Reply.ok("{\"count\":2}"));
    source.on(
        "POST",
        "/graph_service_v1/_search",
        StubCluster.Reply.ok("{\"hits\":{\"hits\":[{\"_id\":\"a\"},{\"_id\":\"b\"}]}}"));
    target.on("POST", "/graph_service_v1/_refresh", StubCluster.Reply.ok("{}"));
    target.on("POST", "/graph_service_v1/_count", StubCluster.Reply.ok("{\"count\":1}"));
    // The target never gets "b", however often it is looked for.
    target.on(
        "POST",
        "/graph_service_v1/_search",
        StubCluster.Reply.ok("{\"hits\":{\"hits\":[{\"_id\":\"a\"}]}}"));
    OperationContext opContext = source.opContext();
    MigrationFamily graph =
        MigrationFamily.of(
            SearchComponent.GRAPH,
            mock(ElasticSearchGraphService.class),
            mock(ElasticSearchSystemMetadataService.class),
            mock(ElasticSearchTimeseriesAspectService.class));
    MigrationIndexNames names =
        MigrationIndexNames.forFamily(
            IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration()),
            SearchComponent.GRAPH,
            null);
    SearchClusterRegistry.FamilyMigration migration =
        new SearchClusterRegistry.FamilyMigration(
            SearchComponent.GRAPH,
            "primary",
            "secondary",
            FamilyMigrationSettings.builder().build(),
            names,
            null);
    ClusterMigrationHarness.InMemoryUpgradeRows rows =
        new ClusterMigrationHarness.InMemoryUpgradeRows();
    ClusterMigrationRow row = ClusterMigrationRow.load(opContext, rows.entityService(), "m");
    UpgradeContext context = mock(UpgradeContext.class);
    when(context.report()).thenReturn(new DefaultUpgradeReport());
    SourceIndexSet set =
        new SourceIndexSet(
            "graph_service_v1",
            IndexLayout.CONCRETE,
            List.of("graph_service_v1"),
            "graph_service_v1");

    int missing =
        new CopyVerifier(opContext, source.http(), target.http(), graph, "step", 0L)
            .verify(context, row, migration, List.of(set));

    assertEquals(missing, 1);
    assertEquals(
        row.get(ClusterMigrationState.VERIFY_PREFIX + "graph_service_v1.sampledMissing"), "1");
    // Looked for again before being counted, after a refresh each time.
    assertEquals(target.calls("POST", "/graph_service_v1/_search").size(), CopyVerifier.RECHECKS);
    assertEquals(target.calls("POST", "/graph_service_v1/_refresh").size(), CopyVerifier.RECHECKS);
  }

  @Test
  public void testADocumentTheSourceDeletedSinceTheSampleIsNotMissing() throws Exception {
    // Graph edges are replaced on every ingestion run: "b" was among the newest when sampled and
    // was deleted a moment later, which the mirror carried to the target.
    StubCluster source = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    StubCluster target = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_3);
    source.on("POST", "/graph_service_v1/_refresh", StubCluster.Reply.ok("{}"));
    source.on("POST", "/graph_service_v1/_count", StubCluster.Reply.ok("{\"count\":1}"));
    source.on(
        "POST",
        "/graph_service_v1/_search",
        call ->
            call.body().contains("\"ids\"")
                ? StubCluster.Reply.ok("{\"hits\":{\"hits\":[{\"_id\":\"a\"}]}}")
                : StubCluster.Reply.ok("{\"hits\":{\"hits\":[{\"_id\":\"a\"},{\"_id\":\"b\"}]}}"));
    target.on("POST", "/graph_service_v1/_refresh", StubCluster.Reply.ok("{}"));
    target.on("POST", "/graph_service_v1/_count", StubCluster.Reply.ok("{\"count\":1}"));
    target.on(
        "POST",
        "/graph_service_v1/_search",
        StubCluster.Reply.ok("{\"hits\":{\"hits\":[{\"_id\":\"a\"}]}}"));
    OperationContext opContext = source.opContext();
    MigrationFamily graph =
        MigrationFamily.of(
            SearchComponent.GRAPH,
            mock(ElasticSearchGraphService.class),
            mock(ElasticSearchSystemMetadataService.class),
            mock(ElasticSearchTimeseriesAspectService.class));
    SearchClusterRegistry.FamilyMigration migration =
        new SearchClusterRegistry.FamilyMigration(
            SearchComponent.GRAPH,
            "primary",
            "secondary",
            FamilyMigrationSettings.builder().build(),
            MigrationIndexNames.forFamily(
                IndexConventionImpl.noPrefix("MD5", new EntityIndexConfiguration()),
                SearchComponent.GRAPH,
                null),
            null);
    ClusterMigrationRow row =
        ClusterMigrationRow.load(
            opContext, new ClusterMigrationHarness.InMemoryUpgradeRows().entityService(), "m");
    UpgradeContext context = mock(UpgradeContext.class);
    when(context.report()).thenReturn(new DefaultUpgradeReport());
    SourceIndexSet set =
        new SourceIndexSet(
            "graph_service_v1",
            IndexLayout.CONCRETE,
            List.of("graph_service_v1"),
            "graph_service_v1");

    int missing =
        new CopyVerifier(opContext, source.http(), target.http(), graph, "step", 0L)
            .verify(context, row, migration, List.of(set));

    assertEquals(missing, 0);
    assertEquals(
        row.get(ClusterMigrationState.VERIFY_PREFIX + "graph_service_v1.sampledMissing"), "0");
  }
}
