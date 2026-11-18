package com.linkedin.datahub.upgrade.system.elasticsearch.migration;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.util.List;
import org.testng.annotations.Test;

public class IndexLayoutResolverTest {

  @Test
  public void testUsageDataStreamResolvesToItsGenerations() throws Exception {
    StubCluster cluster = new StubCluster(SearchClientShim.SearchEngineType.ELASTICSEARCH_8);
    cluster.on("GET", "/_alias/usage", new StubCluster.Reply(404, "{}"));
    cluster.on(
        "GET",
        "/_data_stream/usage",
        StubCluster.Reply.ok(
            "{\"data_streams\":[{\"name\":\"usage\",\"indices\":[{\"index_name\":\".ds-usage-000001\"},{\"index_name\":\".ds-usage-000002\"}]}]}"));

    SourceIndexSet set =
        IndexLayoutResolver.resolve(cluster.http(), SearchComponent.USAGE, "usage");

    assertEquals(set.layout(), IndexLayout.DATA_STREAM);
    assertEquals(set.backingIndices(), List.of(".ds-usage-000001", ".ds-usage-000002"));
    assertEquals(set.writeIndex(), ".ds-usage-000002");
  }

  @Test
  public void testUsageRolloverAliasResolvesToItsMembersWithTheWriteIndex() throws Exception {
    StubCluster cluster = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    cluster.on(
        "GET",
        "/_alias/usage",
        StubCluster.Reply.ok(
            "{\"usage-000002\":{\"aliases\":{\"usage\":{\"is_write_index\":true}}},"
                + "\"usage-000001\":{\"aliases\":{\"usage\":{\"is_write_index\":false}}}}"));

    SourceIndexSet set =
        IndexLayoutResolver.resolve(cluster.http(), SearchComponent.USAGE, "usage");

    assertEquals(set.layout(), IndexLayout.ROLLOVER_ALIAS);
    assertEquals(set.backingIndices(), List.of("usage-000001", "usage-000002"));
    assertEquals(set.writeIndex(), "usage-000002");
    // A user scoped to the deployment's indices holds no data stream permission on OpenSearch.
    assertTrue(cluster.calls("GET", "/_data_stream/usage").isEmpty());
  }

  @Test
  public void testAnAliasWithoutAWriteIndexFallsBackToItsNewestOwnGeneration() throws Exception {
    StubCluster cluster = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    // An adopted staging member sorts after the alias's own generations but never takes writes.
    String alias = "tgt_datahub_usage_event";
    cluster.on(
        "GET",
        "/_alias/" + alias,
        StubCluster.Reply.ok(
            "{\"tgt_datahub_usage_event-000002\":{\"aliases\":{\"tgt_datahub_usage_event\":{}}},"
                + "\"tgt_datahub_usage_event-000001\":{\"aliases\":{\"tgt_datahub_usage_event\":{}}},"
                + "\"tgt_datahub_usage_migration-live\":{\"aliases\":{\"tgt_datahub_usage_event\":{\"is_write_index\":false}}}}"));

    SourceIndexSet set = IndexLayoutResolver.resolve(cluster.http(), SearchComponent.USAGE, alias);

    assertEquals(
        set.backingIndices(),
        List.of(
            "tgt_datahub_usage_event-000001",
            "tgt_datahub_usage_event-000002",
            "tgt_datahub_usage_migration-live"));
    assertEquals(set.writeIndex(), "tgt_datahub_usage_event-000002");
  }

  @Test
  public void testLegacyConcreteUsageIndexAndMissingIndex() throws Exception {
    StubCluster cluster = new StubCluster(SearchClientShim.SearchEngineType.OPENSEARCH_2);
    cluster.on("GET", "/_alias/usage", new StubCluster.Reply(404, "{}"));
    cluster.on("HEAD", "/usage", StubCluster.Reply.ok(""));

    SourceIndexSet set =
        IndexLayoutResolver.resolve(cluster.http(), SearchComponent.USAGE, "usage");
    assertEquals(set.layout(), IndexLayout.CONCRETE);
    assertEquals(set.writeIndex(), "usage");

    assertNull(
        IndexLayoutResolver.resolve(cluster.http(), SearchComponent.GRAPH, "graph_service_v1"));
  }
}
