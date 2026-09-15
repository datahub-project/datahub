package com.linkedin.metadata.config.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

/** Covers cluster resolution, legacy env synthesis and the componentCluster routing table. */
public class ElasticSearchClustersTest {

  private static ElasticSearchConfiguration.ElasticSearchConfigurationBuilder base() {
    return ElasticSearchConfiguration.builder()
        .entityIndex(
            EntityIndexConfiguration.builder()
                .v2(EntityIndexVersionConfiguration.builder().enabled(true).build())
                .build());
  }

  private static Map<String, SearchClusterSettings> clusters(
      String name, SearchClusterSettings settings) {
    Map<String, SearchClusterSettings> map = new LinkedHashMap<>();
    map.put(name, settings);
    return map;
  }

  @Test
  public void testLegacyHostPortSynthesizesPrimaryUri() {
    ElasticSearchConfiguration config = base().host("search").port(9200).build();

    assertEquals(config.getPrimaryCluster().getUri(), "http://search:9200");
    assertEquals(config.getHost(), "search");
    assertEquals(config.getPort(), 9200);
    assertFalse(config.isUseSSL());
  }

  @Test
  public void testLegacyUseSslSynthesizesHttps() {
    ElasticSearchConfiguration config = base().host("search").port(9200).useSSL(true).build();

    assertEquals(config.getPrimaryCluster().getUri(), "https://search:9200");
    assertTrue(config.isUseSSL());
  }

  @Test
  public void testLegacyPathPrefixBecomesUriPath() {
    ElasticSearchConfiguration config =
        base().host("search").port(9200).pathPrefix("opensearch").build();

    assertEquals(config.getPrimaryCluster().getUri(), "http://search:9200/opensearch");
    assertEquals(config.getPathPrefix(), "/opensearch");
  }

  @Test
  public void testExplicitUriWinsOverLegacyHost() {
    ElasticSearchConfiguration config =
        base()
            .host("ignored")
            .pathPrefix("ignored")
            .clusters(
                clusters(
                    "primary",
                    SearchClusterSettings.builder()
                        .uri("https://es.example:443/opensearch")
                        .build()))
            .build();

    // Leftover legacy vars are ignored rather than failing startup, since Compose images often
    // still export ELASTICSEARCH_HOST alongside a newer URI.
    assertEquals(config.getHost(), "es.example");
    assertEquals(config.getPathPrefix(), "/opensearch");
    assertTrue(config.isUseSSL());
  }

  @Test
  public void testMissingLegacySettingsDefaultToLocalhost() {
    ElasticSearchConfiguration config = base().build();
    assertEquals(config.getPrimaryCluster().getUri(), "http://localhost:9200");
  }

  @Test
  public void testComponentsDefaultToPrimary() {
    ElasticSearchConfiguration config = base().host("search").port(9200).build();

    for (SearchComponent component : SearchComponent.values()) {
      assertEquals(config.getComponentCluster().clusterFor(component), "primary");
    }
  }

  @Test
  public void testComponentRoutedToSecondary() {
    Map<String, SearchClusterSettings> map = new LinkedHashMap<>();
    map.put("primary", SearchClusterSettings.builder().uri("http://search:9200").build());
    map.put("secondary", SearchClusterSettings.builder().uri("http://search2:9200").build());

    ElasticSearchConfiguration config =
        base()
            .clusters(map)
            .componentCluster(ComponentClusterConfiguration.builder().searchV3("secondary").build())
            .build();

    assertEquals(config.getCluster(SearchComponent.SEARCH_V3).getUri(), "http://search2:9200");
    assertEquals(config.getCluster(SearchComponent.SEARCH_V2).getUri(), "http://search:9200");
  }

  @Test
  public void testRoutingToUnconfiguredClusterFailsStartup() {
    Map<String, SearchClusterSettings> map = new LinkedHashMap<>();
    map.put("primary", SearchClusterSettings.builder().uri("http://search:9200").build());
    map.put("secondary", SearchClusterSettings.builder().uri("").build());

    ElasticSearchConfiguration config =
        base()
            .clusters(map)
            .componentCluster(ComponentClusterConfiguration.builder().semantic("secondary").build())
            .build();

    assertThrows(IllegalStateException.class, config::normalizeClusters);
  }

  @Test
  public void testBlankSecondaryIsIgnoredWhenNothingRoutesToIt() {
    Map<String, SearchClusterSettings> map = new LinkedHashMap<>();
    map.put("primary", SearchClusterSettings.builder().uri("http://search:9200").build());
    map.put("secondary", SearchClusterSettings.builder().uri("").build());

    ElasticSearchConfiguration config = base().clusters(map).build();

    config.normalizeClusters();
    assertFalse(config.getCluster("secondary").isConfigured());
  }

  @Test
  public void testShardCountFallsBackToOwnDataNodeCount() {
    SearchClusterSettings secondary =
        SearchClusterSettings.builder().uri("http://search2:9200").dataNodeCount(5).build();
    SearchClusterSettings primary =
        SearchClusterSettings.builder().uri("http://search:9200").dataNodeCount(2).build();

    // A secondary with no explicit shard count uses its own node count, never primary's.
    assertEquals(secondary.effectiveNumShards(), 5);
    assertEquals(primary.effectiveNumShards(), 2);
  }

  @Test
  public void testExplicitShardCountOverridesDataNodeCount() {
    SearchClusterSettings cluster =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .dataNodeCount(5)
            .index(SearchClusterIndexSettings.builder().numShards(11).build())
            .build();

    assertEquals(cluster.effectiveNumShards(), 11);
  }

  @Test
  public void testReplicaCountDefaultsToOne() {
    SearchClusterSettings cluster =
        SearchClusterSettings.builder().uri("http://search:9200").build();
    assertEquals(cluster.effectiveNumReplicas(), 1);
  }

  @Test
  public void testUnsetOverlayInheritsSharedIndexDefaults() {
    IndexConfiguration defaults =
        IndexConfiguration.builder().prefix("prod").numRetries(7).maxArrayLength(1000).build();
    SearchClusterSettings cluster =
        SearchClusterSettings.builder().uri("http://search2:9200").dataNodeCount(3).build();

    IndexConfiguration effective = cluster.effectiveIndex(defaults);
    assertEquals(effective.getPrefix(), "prod");
    assertEquals(effective.getNumRetries(), 7);
    assertEquals(effective.getNumShards(), 3);
  }

  @Test
  public void testClusterOverlayWinsOverSharedIndexDefaults() {
    IndexConfiguration defaults = IndexConfiguration.builder().prefix("prod").numRetries(7).build();
    SearchClusterSettings cluster =
        SearchClusterSettings.builder()
            .uri("http://search2:9200")
            .index(SearchClusterIndexSettings.builder().numRetries(2).build())
            .build();

    assertEquals(cluster.effectiveIndex(defaults).getNumRetries(), 2);
    assertEquals(cluster.effectiveIndex(defaults).getPrefix(), "prod");
  }

  @Test
  public void testV2TokenizerFoldedIntoEffectiveIndex() {
    ElasticSearchConfiguration config =
        base()
            .host("search")
            .port(9200)
            .index(IndexConfiguration.builder().prefix("prod").build())
            .entityIndex(
                EntityIndexConfiguration.builder()
                    .v2(EntityIndexVersionConfiguration.builder().mainTokenizer("keyword").build())
                    .build())
            .build();

    config.normalizeClusters();
    assertEquals(config.getIndex().getMainTokenizer(), "keyword");
  }

  @Test
  public void testIdHashAlgoReadsFromV2() {
    ElasticSearchConfiguration config =
        base()
            .entityIndex(
                EntityIndexConfiguration.builder()
                    .v2(EntityIndexVersionConfiguration.builder().idHashAlgo("SHA-256").build())
                    .build())
            .build();

    assertEquals(config.getIdHashAlgo(), "SHA-256");
  }

  @Test
  public void testSecondaryShimDefaultsToAutoDetect() {
    // An unset shim block must not inherit primary's pinned engineType.
    assertTrue(ShimSettings.builder().build().isAutoDetectEnabled());
    assertTrue(
        ShimSettings.builder().engineType("OPENSEARCH_2").build().isAutoDetectEnabled(),
        "autoDetectEngine defaults on, and takes precedence over engineType");
    assertFalse(
        ShimSettings.builder()
            .autoDetectEngine(false)
            .engineType("OPENSEARCH_2")
            .build()
            .isAutoDetectEnabled());
  }
}
