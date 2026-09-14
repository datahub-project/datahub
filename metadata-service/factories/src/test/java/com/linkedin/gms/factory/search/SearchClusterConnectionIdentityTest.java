package com.linkedin.gms.factory.search;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertSame;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.search.BulkProcessorConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.config.search.SearchClusterIndexSettings;
import com.linkedin.metadata.config.search.SearchClusterSettings;
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
