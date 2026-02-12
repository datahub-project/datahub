package com.linkedin.metadata.search.elasticsearch.client.shim;

import static org.testng.Assert.*;

import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import org.testng.annotations.Test;

/** Tests for the SearchClientShim functionality */
public class SearchClientShimTest {

  @Test
  public void testShimConfigurationBuilder() {
    // Test configuration builder
    SearchClientShim.ShimConfiguration config =
        new ShimConfigurationBuilder()
            .withEngineType(SearchEngineType.ELASTICSEARCH_7)
            .withHost("localhost")
            .withPort(9200)
            .withCredentials("user", "pass")
            .withSSL(true)
            .withPathPrefix("/es")
            .withAwsIamAuth(false, null)
            .withThreadCount(2)
            .withConnectionRequestTimeout(5000)
            .withSocketTimeout(30000)
            .build();

    assertEquals(config.getEngineType(), SearchEngineType.ELASTICSEARCH_7);
    assertEquals(config.getHost(), "localhost");
    assertEquals(config.getPort(), Integer.valueOf(9200));
    assertEquals(config.getUsername(), "user");
    assertEquals(config.getPassword(), "pass");
    assertTrue(config.isUseSSL());
    assertEquals(config.getPathPrefix(), "/es");
    assertFalse(config.isUseAwsIamAuth());
    assertEquals(config.getThreadCount(), Integer.valueOf(2));
    assertEquals(config.getConnectionRequestTimeout(), Integer.valueOf(5000));
    assertEquals(config.getSocketTimeout(), Integer.valueOf(30000));
  }

  @Test
  public void testSearchEngineTypeHelpers() {
    // Test engine type helper methods
    assertTrue(SearchEngineType.ELASTICSEARCH_7.isElasticsearch());
    assertTrue(SearchEngineType.ELASTICSEARCH_8.isElasticsearch());
    assertTrue(SearchEngineType.ELASTICSEARCH_9.isElasticsearch());
    assertFalse(SearchEngineType.OPENSEARCH_2.isElasticsearch());
    assertFalse(SearchEngineType.OPENSEARCH_2.isElasticsearch());

    assertFalse(SearchEngineType.ELASTICSEARCH_7.isOpenSearch());
    assertFalse(SearchEngineType.ELASTICSEARCH_8.isOpenSearch());
    assertFalse(SearchEngineType.ELASTICSEARCH_9.isOpenSearch());
    assertTrue(SearchEngineType.OPENSEARCH_2.isOpenSearch());

    // Test client compatibility
    assertTrue(SearchEngineType.ELASTICSEARCH_7.supportsEs7HighLevelClient());
    assertTrue(SearchEngineType.OPENSEARCH_2.supportsEs7HighLevelClient());
    assertFalse(SearchEngineType.ELASTICSEARCH_8.supportsEs7HighLevelClient());
    assertFalse(SearchEngineType.ELASTICSEARCH_9.supportsEs7HighLevelClient());

    assertFalse(SearchEngineType.ELASTICSEARCH_7.requiresEs8JavaClient());
    assertTrue(SearchEngineType.ELASTICSEARCH_8.requiresEs8JavaClient());
    assertTrue(SearchEngineType.ELASTICSEARCH_9.requiresEs8JavaClient());
    assertFalse(SearchEngineType.OPENSEARCH_2.requiresEs8JavaClient());

    assertFalse(SearchEngineType.ELASTICSEARCH_7.requiresOpenSearchClient());
    assertFalse(SearchEngineType.ELASTICSEARCH_8.requiresOpenSearchClient());
    assertFalse(SearchEngineType.ELASTICSEARCH_9.requiresOpenSearchClient());
    assertFalse(SearchEngineType.OPENSEARCH_2.requiresOpenSearchClient());
  }

  @Test
  public void testFeatureSupport() {
    // Test that different engine types support different features
    // This is a basic test of the interface - actual feature support testing
    // would require live cluster connections

    SearchClientShim.ShimConfiguration mockConfig =
        new ShimConfigurationBuilder()
            .withEngineType(SearchEngineType.ELASTICSEARCH_7)
            .withHost("localhost")
            .withPort(9200)
            .withCredentials("user", "pass")
            .withSSL(true)
            .withPathPrefix("/es")
            .withAwsIamAuth(false, null)
            .withThreadCount(2)
            .withConnectionRequestTimeout(5000)
            .withSocketTimeout(30000)
            .build();

    // We can't test the actual shim implementations without a live cluster
    // but we can test the configuration and enum logic
    assertNotNull(mockConfig);
    assertEquals(mockConfig.getEngineType().getEngine(), "elasticsearch");
    assertEquals(mockConfig.getEngineType().getMajorVersion(), "7");
  }

  @Test
  public void testShimConfigurationBuilderWithDefaultSocketTimeout() {
    SearchClientShim.ShimConfiguration config =
        new ShimConfigurationBuilder()
            .withEngineType(SearchEngineType.ELASTICSEARCH_8)
            .withHost("localhost")
            .withPort(9200)
            .build();

    assertEquals(config.getSocketTimeout(), Integer.valueOf(30000));
  }

  @Test
  public void testShimConfigurationBuilderWithCustomSocketTimeout() {
    SearchClientShim.ShimConfiguration config =
        new ShimConfigurationBuilder()
            .withEngineType(SearchEngineType.ELASTICSEARCH_8)
            .withHost("localhost")
            .withPort(9200)
            .withSocketTimeout(600000)
            .build();

    assertEquals(config.getSocketTimeout(), Integer.valueOf(600000));
  }

  @Test
  public void testShimConfigurationBuilderCopy() {
    SearchClientShim.ShimConfiguration original =
        new ShimConfigurationBuilder()
            .withEngineType(SearchEngineType.OPENSEARCH_2)
            .withHost("localhost")
            .withPort(9200)
            .withConnectionRequestTimeout(10000)
            .withSocketTimeout(300000)
            .build();

    SearchClientShim.ShimConfiguration copy =
        new ShimConfigurationBuilder(original).withPort(9201).build();

    assertEquals(copy.getSocketTimeout(), Integer.valueOf(300000));
    assertEquals(copy.getConnectionRequestTimeout(), Integer.valueOf(10000));
    assertEquals(copy.getPort(), Integer.valueOf(9201));
  }

  // Note: Integration tests that require live Elasticsearch/OpenSearch clusters
  // should be placed in separate test classes and run only when a test cluster is available.
  // These would test:
  // 1. Actual connection establishment
  // 2. Search operations
  // 3. Index management
  // 4. Auto-detection logic
  // 5. API compatibility mode with ES 7.17 -> ES 8.x
}
