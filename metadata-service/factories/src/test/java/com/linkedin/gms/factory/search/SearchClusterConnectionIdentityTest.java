package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.EntityIndexConfiguration;
import com.linkedin.metadata.config.search.EntityIndexVersionConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchClusterIndexSettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
import com.linkedin.metadata.config.search.ShimSettings;
import com.linkedin.metadata.config.search.SslContextSettings;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.annotations.Test;

/**
 * Two cluster entries that resolve to the same endpoint and credentials should share one HTTP
 * client; anything that changes the connection must produce a distinct identity.
 */
public class SearchClusterConnectionIdentityTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  @Test
  public void testSameEndpointAndCredentialsAlias() {
    SearchClusterSettings a =
        SearchClusterSettings.builder().uri("http://search:9200").username("es").build();
    SearchClusterSettings b =
        SearchClusterSettings.builder().uri("http://search:9200").username("es").build();

    assertEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testDifferentHostIsADistinctConnection() {
    SearchClusterSettings a = SearchClusterSettings.builder().uri("http://search:9200").build();
    SearchClusterSettings b = SearchClusterSettings.builder().uri("http://search2:9200").build();

    assertNotEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testDifferentUserIsADistinctConnection() {
    SearchClusterSettings a =
        SearchClusterSettings.builder().uri("http://search:9200").username("es").build();
    SearchClusterSettings b =
        SearchClusterSettings.builder().uri("http://search:9200").username("other").build();

    assertNotEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testPathPrefixIsPartOfIdentity() {
    SearchClusterSettings a = SearchClusterSettings.builder().uri("http://search:9200").build();
    SearchClusterSettings b =
        SearchClusterSettings.builder().uri("http://search:9200/opensearch").build();

    assertNotEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testDifferentPasswordIsADistinctConnection() {
    SearchClusterSettings a =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .username("es")
            .password("one")
            .build();
    SearchClusterSettings b =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .username("es")
            .password("two")
            .build();

    assertNotEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testDifferentTlsMaterialIsADistinctConnection() {
    SearchClusterSettings a =
        SearchClusterSettings.builder()
            .uri("https://search:9200")
            .sslContext(SslContextSettings.builder().trustStoreFile("/a.jks").build())
            .build();
    SearchClusterSettings b =
        SearchClusterSettings.builder()
            .uri("https://search:9200")
            .sslContext(SslContextSettings.builder().trustStoreFile("/b.jks").build())
            .build();

    assertNotEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testDifferentEngineTypeIsADistinctConnection() {
    SearchClusterSettings a =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .shim(ShimSettings.builder().engineType("OPENSEARCH_2").autoDetectEngine(false).build())
            .build();
    SearchClusterSettings b =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .shim(ShimSettings.builder().engineType("OPENSEARCH_3").autoDetectEngine(false).build())
            .build();

    assertNotEquals(
        SearchClientShimFactory.connectionIdentity("primary", a),
        SearchClientShimFactory.connectionIdentity("secondary", b));
  }

  @Test
  public void testPasswordIsHashedNotEmbeddedInIdentity() {
    SearchClusterSettings cluster =
        SearchClusterSettings.builder()
            .uri("http://search:9200")
            .username("es")
            .password("super-secret")
            .build();

    String identity = SearchClientShimFactory.connectionIdentity("primary", cluster);
    assertFalse(identity.contains("super-secret"));
  }

  @Test
  public void testUnsetOverlayInheritsSharedDefaults() {
    ElasticSearchConfiguration esConfig = configWithSecondaryOverlay(null);

    BulkProcessorConfiguration effective =
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "secondary")
            .getBulkProcessor();

    assertEquals(effective.getRequestsLimit(), 1000);
    assertEquals(effective.getNumRetries(), 3);
  }

  @Test
  public void testOverlayReplacesOnlyTheFieldsItSets() {
    ElasticSearchConfiguration esConfig = configWithSecondaryOverlay(Map.of("requestsLimit", 250));

    BulkProcessorConfiguration secondary =
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "secondary")
            .getBulkProcessor();
    BulkProcessorConfiguration primary =
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "primary")
            .getBulkProcessor();

    assertEquals(secondary.getRequestsLimit(), 250);
    assertEquals(secondary.getNumRetries(), 3, "unset overlay fields keep the shared default");
    assertEquals(
        primary.getRequestsLimit(), 1000, "one cluster's overlay must not leak to another");
  }

  @Test
  public void testSharedDefaultsObjectIsNotMutatedByAnOverlay() {
    ElasticSearchConfiguration esConfig = configWithSecondaryOverlay(Map.of("requestsLimit", 250));
    BulkProcessorConfiguration shared = esConfig.getBulkProcessor();

    SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "secondary");

    assertEquals(shared.getRequestsLimit(), 1000);
    assertSame(esConfig.getBulkProcessor(), shared);
  }

  @Test
  public void testClusterMappingFilesOverlayEntityIndexWithoutMutatingSharedDefaults() {
    EntityIndexConfiguration sharedEntityIndex =
        EntityIndexConfiguration.builder()
            .v3(
                EntityIndexVersionConfiguration.builder()
                    .enabled(true)
                    .analyzerConfig("shared_analyzer.yaml")
                    .mappingConfig("shared_mapping.yaml")
                    .build())
            .build();
    ElasticSearchConfiguration esConfig =
        ElasticSearchConfiguration.builder()
            .clusters(
                Map.of(
                    "primary",
                    SearchClusterSettings.builder().uri("http://search:9200").build(),
                    "secondary",
                    SearchClusterSettings.builder()
                        .uri("http://search2:9200")
                        .index(
                            SearchClusterIndexSettings.builder()
                                .analyzerConfig("os3_analyzer.yaml")
                                .mappingConfig("os3_mapping.yaml")
                                .build())
                        .build()))
            .index(IndexConfiguration.builder().prefix("prod").build())
            .entityIndex(sharedEntityIndex)
            .bulkProcessor(BulkProcessorConfiguration.builder().requestsLimit(1000).build())
            .build();

    ElasticSearchConfiguration secondary =
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "secondary");
    ElasticSearchConfiguration primary =
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "primary");

    assertEquals(secondary.getEntityIndex().getV3().getAnalyzerConfig(), "os3_analyzer.yaml");
    assertEquals(secondary.getEntityIndex().getV3().getMappingConfig(), "os3_mapping.yaml");
    assertEquals(secondary.getEntityIndex().getV2().getAnalyzerConfig(), "os3_analyzer.yaml");
    assertEquals(primary.getEntityIndex().getV3().getAnalyzerConfig(), "shared_analyzer.yaml");
    assertEquals(sharedEntityIndex.getV3().getAnalyzerConfig(), "shared_analyzer.yaml");
  }

  @Test
  public void testShardCountComesFromEachClustersOwnNodeCount() {
    ElasticSearchConfiguration esConfig = configWithSecondaryOverlay(null);

    assertEquals(
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "primary")
            .getIndex()
            .getNumShards(),
        2);
    assertEquals(
        SearchClusterRegistryFactory.effectiveConfig(MAPPER, esConfig, "secondary")
            .getIndex()
            .getNumShards(),
        7);
  }

  private static ElasticSearchConfiguration configWithSecondaryOverlay(
      Map<String, Object> bulkOverlay) {
    Map<String, SearchClusterSettings> clusters = new LinkedHashMap<>();
    clusters.put(
        "primary",
        SearchClusterSettings.builder().uri("http://search:9200").dataNodeCount(2).build());
    clusters.put(
        "secondary",
        SearchClusterSettings.builder()
            .uri("http://search2:9200")
            .dataNodeCount(7)
            .index(SearchClusterIndexSettings.builder().build())
            .bulkProcessor(bulkOverlay)
            .build());

    return ElasticSearchConfiguration.builder()
        .clusters(clusters)
        .index(IndexConfiguration.builder().prefix("prod").build())
        .bulkProcessor(
            BulkProcessorConfiguration.builder().requestsLimit(1000).numRetries(3).build())
        .build();
  }
}
