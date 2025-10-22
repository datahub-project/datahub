package com.linkedin.metadata.search.opensearch;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import java.io.IOException;
import java.util.Map;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testcontainers.containers.GenericContainer;
import org.testng.annotations.Test;

@Import({
  OpenSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class SearchClientShimOpenSearchIntegrationTest extends AbstractTestNGSpringContextTests {
  private static final String TEST_INDEX = "test-shim-opensearch-index";

  @Autowired private GenericContainer<?> openSearchContainer;
  @Autowired private SearchClientShim<?> searchClientShim;

  @Test
  public void testShimCreation() {

    assertNotNull(searchClientShim);
    assertEquals(searchClientShim.getEngineType(), SearchEngineType.OPENSEARCH_2);

    // Test native client access
    Object nativeClient = searchClientShim.getNativeClient();
    assertNotNull(nativeClient);
    assertTrue(nativeClient instanceof org.opensearch.client.RestHighLevelClient);
  }

  @Test
  public void testClusterInfo() throws IOException {

    Map<String, String> clusterInfo = searchClientShim.getClusterInfo();
    assertNotNull(clusterInfo);

    assertTrue(clusterInfo.containsKey("version"));
    assertTrue(clusterInfo.containsKey("engine_type"));
    assertEquals(clusterInfo.get("engine_type"), "opensearch");

    // Verify version starts with 2
    String version = clusterInfo.get("version");
    assertTrue(version.startsWith("2."), "Expected version to start with 2, got: " + version);
  }

  @Test
  public void testEngineVersion() throws IOException {

    String version = searchClientShim.getEngineVersion();
    assertNotNull(version);
    assertNotEquals(version, "unknown");
    assertTrue(version.startsWith("2."), "Expected version to start with 2, got: " + version);
  }

  @Test
  public void testFeatureSupport() {

    // Test features that OpenSearch 2.x should support
    assertTrue(searchClientShim.supportsFeature("scroll"));
    assertTrue(searchClientShim.supportsFeature("bulk"));
    assertTrue(searchClientShim.supportsFeature("mapping_types"));
    assertTrue(searchClientShim.supportsFeature("point_in_time"));
    assertTrue(searchClientShim.supportsFeature("async_search"));

    // Test features that OpenSearch doesn't support
    assertFalse(searchClientShim.supportsFeature("cross_cluster_replication"));
  }

  @Test
  public void testIndexOperations() throws IOException {

    // Test index creation
    CreateIndexRequest createRequest = new CreateIndexRequest(TEST_INDEX);
    CreateIndexResponse createResponse =
        searchClientShim.createIndex(createRequest, RequestOptions.DEFAULT);
    assertNotNull(createResponse);
    assertTrue(createResponse.isAcknowledged());

    // Test index exists
    GetIndexRequest getRequest = new GetIndexRequest(TEST_INDEX);
    boolean exists = searchClientShim.indexExists(getRequest, RequestOptions.DEFAULT);
    assertTrue(exists);

    // Test refreshIndex
    RefreshRequest refreshRequest = new RefreshRequest(TEST_INDEX);
    RefreshResponse refreshResponse =
        searchClientShim.refreshIndex(refreshRequest, RequestOptions.DEFAULT);
    assertNotNull(refreshResponse);
  }

  @Test(dependsOnMethods = "testIndexOperations")
  public void testSearchOperations() throws IOException {

    // Test basic search
    SearchRequest searchRequest = new SearchRequest(TEST_INDEX);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(QueryBuilders.matchAllQuery());
    searchRequest.source(searchSourceBuilder);

    SearchResponse searchResponse = searchClientShim.search(searchRequest, RequestOptions.DEFAULT);
    assertNotNull(searchResponse);
    assertNotNull(searchResponse.getHits());
  }

  @Test
  public void testAutoDetection() throws IOException {

    // Create configuration without specifying engine type
    SearchClientShim.ShimConfiguration autoConfig =
        new ShimConfigurationBuilder()
            .withHost("localhost")
            .withPort(openSearchContainer.getMappedPort(9200))
            .withSSL(false)
            .withThreadCount(1)
            .withConnectionRequestTimeout(5000)
            .build();

    try (SearchClientShim<?> autoShim =
        SearchClientShimUtil.createShimWithAutoDetection(autoConfig, new ObjectMapper())) {

      assertNotNull(autoShim);
      assertEquals(autoShim.getEngineType(), SearchEngineType.OPENSEARCH_2);

      // Verify it can perform basic operations
      Map<String, String> clusterInfo = autoShim.getClusterInfo();
      assertNotNull(clusterInfo);
      assertEquals(clusterInfo.get("engine_type"), "opensearch");
    }
  }

  @Test
  public void testOpenSearchSpecificFeatures() throws IOException {

    // Verify OpenSearch engine type identification
    assertTrue(searchClientShim.getEngineType().isOpenSearch());
    assertFalse(searchClientShim.getEngineType().isElasticsearch());

    // Test that OpenSearch uses high-level client
    assertTrue(searchClientShim.getEngineType().supportsEs7HighLevelClient());
    assertFalse(searchClientShim.getEngineType().requiresEs8JavaClient());

    // Verify cluster information contains OpenSearch-specific details
    Map<String, String> clusterInfo = searchClientShim.getClusterInfo();
    assertTrue(clusterInfo.containsKey("build_flavor") || clusterInfo.containsKey("build_type"));
  }
}
