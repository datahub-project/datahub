package com.linkedin.gms.factory.search;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.common.ObjectMapperFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil.ShimConfigurationBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import java.io.IOException;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Answers;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests that iterate over all supported search client shims to verify configuration parsing,
 * factory logic, and basic validation. These tests do not require live search engine instances.
 */
@Slf4j
@TestPropertySource(
    properties = {
      "elasticsearch.host=localhost",
      "elasticsearch.port=9200",
      "elasticsearch.threadCount=1",
      "elasticsearch.connectionRequestTimeout=5000",
      "elasticsearch.socketTimeout=30000",
      "elasticsearch.username=",
      "elasticsearch.password=",
      "elasticsearch.useSSL=false",
      "elasticsearch.pathPrefix=",
      "elasticsearch.opensearchUseAwsIamAuth=false",
      "elasticsearch.region="
    })
@SpringBootTest(classes = {SearchClientShimFactory.class, ObjectMapperFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class SearchClientShimIterationTest extends AbstractTestNGSpringContextTests {

  // We mock this bean because this test is testing the util, not a live env. This avoids
  // IOException due to missing env
  // with auto-detection
  @MockitoBean(name = "searchClientShim", answers = Answers.RETURNS_MOCKS)
  SearchClientShim<?> searchClientShim;

  /** Data provider that returns all supported search engine types */
  @DataProvider(name = "searchEngineTypes")
  public Object[][] searchEngineTypes() {
    return new Object[][] {
      {SearchEngineType.ELASTICSEARCH_7},
      {SearchEngineType.ELASTICSEARCH_8},
      {SearchEngineType.ELASTICSEARCH_9},
      {SearchEngineType.OPENSEARCH_2}
    };
  }

  /** Test that all engine types can be properly configured */
  @Test(dataProvider = "searchEngineTypes")
  public void testEngineTypeConfiguration(SearchEngineType engineType) {
    log.info("Testing configuration for engine type: {}", engineType);

    // Create configuration for this engine type
    SearchClientShim.ShimConfiguration config =
        new ShimConfigurationBuilder()
            .withEngineType(engineType)
            .withHost("localhost")
            .withPort(9200)
            .withSSL(false)
            .withThreadCount(1)
            .withConnectionRequestTimeout(5000)
            .withSocketTimeout(30000)
            .build();

    assertNotNull(config);
    assertEquals(config.getEngineType(), engineType);
    assertEquals(config.getHost(), "localhost");
    assertEquals(config.getPort(), Integer.valueOf(9200));
    assertFalse(config.isUseSSL());
    assertEquals(config.getThreadCount(), Integer.valueOf(1));
    assertEquals(config.getConnectionRequestTimeout(), Integer.valueOf(5000));
    assertEquals(config.getSocketTimeout(), Integer.valueOf(30000));

    log.info("Configuration test passed for engine type: {}", engineType);
  }

  /** Test engine type properties and compatibility flags */
  @Test(dataProvider = "searchEngineTypes")
  public void testEngineTypeProperties(SearchEngineType engineType) {
    log.info("Testing properties for engine type: {}", engineType);

    // Basic properties
    assertNotNull(engineType.getEngine());
    assertNotNull(engineType.getMajorVersion());

    // Test engine family classification
    boolean isElasticsearch = engineType.isElasticsearch();
    boolean isOpenSearch = engineType.isOpenSearch();

    // Each engine should be exactly one type
    assertNotEquals(
        isElasticsearch,
        isOpenSearch,
        "Engine should be either Elasticsearch or OpenSearch, not both or neither");

    // Test client compatibility based on engine type
    switch (engineType) {
      case ELASTICSEARCH_7:
        assertTrue(isElasticsearch);
        assertFalse(isOpenSearch);
        assertTrue(engineType.supportsEs7HighLevelClient());
        assertFalse(engineType.requiresEs8JavaClient());
        assertFalse(engineType.requiresOpenSearchClient());
        assertEquals(engineType.getEngine(), "elasticsearch");
        assertEquals(engineType.getMajorVersion(), "7");
        break;

      case ELASTICSEARCH_8:
      case ELASTICSEARCH_9:
        assertTrue(isElasticsearch);
        assertFalse(isOpenSearch);
        assertFalse(engineType.supportsEs7HighLevelClient());
        assertTrue(engineType.requiresEs8JavaClient());
        assertFalse(engineType.requiresOpenSearchClient());
        assertEquals(engineType.getEngine(), "elasticsearch");
        assertTrue(
            engineType.getMajorVersion().equals("8") || engineType.getMajorVersion().equals("9"));
        break;

      case OPENSEARCH_2:
        assertFalse(isElasticsearch);
        assertTrue(isOpenSearch);
        assertTrue(engineType.supportsEs7HighLevelClient());
        assertFalse(engineType.requiresEs8JavaClient());
        assertFalse(engineType.requiresOpenSearchClient()); // Uses ES 7.x compatible client
        assertEquals(engineType.getEngine(), "opensearch");
        assertEquals(engineType.getMajorVersion(), "2");
        break;

      default:
        fail("Unexpected engine type: " + engineType);
    }

    log.info("Properties test passed for engine type: {}", engineType);
  }

  /**
   * Test that factory attempts to create shims for all engine types (will fail without live
   * clusters, but validates factory logic)
   */
  @Test(dataProvider = "searchEngineTypes")
  public void testFactoryCreation(SearchEngineType engineType) {
    log.info("Testing factory creation for engine type: {}", engineType);

    SearchClientShim.ShimConfiguration config =
        new ShimConfigurationBuilder()
            .withEngineType(engineType)
            .withHost("localhost")
            .withPort(9200)
            .withSSL(false)
            .withThreadCount(1)
            .withConnectionRequestTimeout(5000)
            .withSocketTimeout(30000)
            .build();

    try {
      SearchClientShim<?> shim = SearchClientShimUtil.createShim(config, new ObjectMapper());

      // If we get here, the factory worked (unlikely without live cluster)
      assertNotNull(shim);
      assertEquals(shim.getEngineType(), engineType);
      shim.close();

      log.info("Factory creation succeeded for engine type: {}", engineType);

    } catch (IOException e) {
      // Expected - we don't have live clusters in unit tests
      log.info(
          "Factory creation failed as expected for engine type: {} - {}",
          engineType,
          e.getMessage());

      // Verify the exception is related to connection issues, not configuration
      String errorMessage = e.getMessage().toLowerCase();
      assertTrue(
          errorMessage.contains("connection")
              || errorMessage.contains("connect")
              || errorMessage.contains("refused")
              || errorMessage.contains("timeout")
              || errorMessage.contains("unreachable"),
          "Expected connection-related error, got: " + e.getMessage());

    } catch (Exception e) {
      // For ES 8.x and ES 9.x, we might get UnsupportedOperationException
      // since those implementations are not complete yet
      if (engineType == SearchEngineType.ELASTICSEARCH_8
          || engineType == SearchEngineType.ELASTICSEARCH_9) {

        assertTrue(
            e instanceof UnsupportedOperationException || e instanceof IOException,
            "Expected UnsupportedOperationException or IOException for "
                + engineType
                + ", got: "
                + e.getClass().getSimpleName());

        log.info(
            "Got expected exception for incomplete implementation {}: {}",
            engineType,
            e.getMessage());
      } else {
        // Unexpected exception for completed implementations
        throw e;
      }
    }
  }

  /** Test configuration builder with different parameter combinations */
  @Test(dataProvider = "searchEngineTypes")
  public void testConfigurationBuilderVariations(SearchEngineType engineType) {
    log.info("Testing configuration builder variations for engine type: {}", engineType);

    // Test minimal configuration
    SearchClientShim.ShimConfiguration minimalConfig =
        new ShimConfigurationBuilder()
            .withEngineType(engineType)
            .withHost("localhost")
            .withPort(9200)
            .build();

    assertNotNull(minimalConfig);
    assertEquals(minimalConfig.getEngineType(), engineType);

    // Test full configuration
    SearchClientShim.ShimConfiguration fullConfig =
        new ShimConfigurationBuilder()
            .withEngineType(engineType)
            .withHost("test-host")
            .withPort(9201)
            .withCredentials("testuser", "testpass")
            .withSSL(true)
            .withPathPrefix("/search")
            .withAwsIamAuth(false, "us-west-2")
            .withThreadCount(4)
            .withConnectionRequestTimeout(10000)
            .withSocketTimeout(300000)
            .build();

    assertNotNull(fullConfig);
    assertEquals(fullConfig.getEngineType(), engineType);
    assertEquals(fullConfig.getHost(), "test-host");
    assertEquals(fullConfig.getPort(), Integer.valueOf(9201));
    assertEquals(fullConfig.getUsername(), "testuser");
    assertEquals(fullConfig.getPassword(), "testpass");
    assertTrue(fullConfig.isUseSSL());
    assertEquals(fullConfig.getPathPrefix(), "/search");
    assertFalse(fullConfig.isUseAwsIamAuth());
    assertEquals(fullConfig.getRegion(), "us-west-2");
    assertEquals(fullConfig.getThreadCount(), Integer.valueOf(4));
    assertEquals(fullConfig.getConnectionRequestTimeout(), Integer.valueOf(10000));
    assertEquals(fullConfig.getSocketTimeout(), Integer.valueOf(300000));

    log.info("Configuration builder test passed for engine type: {}", engineType);
  }

  /** Test expected feature support patterns across all engine types */
  @Test
  public void testFeatureSupportPatterns() {
    log.info("Testing feature support patterns across all engine types");

    // Features that all engines should support
    String[] universalFeatures = {"scroll", "bulk"};

    // Features that may vary by engine
    String[] variableFeatures = {"async_search", "point_in_time", "cross_cluster_replication"};

    for (SearchEngineType engineType : SearchEngineType.values()) {
      log.info("Testing feature support for {}", engineType);

      // We can't test actual feature support without live clusters,
      // but we can verify the method exists and returns boolean values
      // This would be better tested in integration tests with live clusters

      // Just verify the enum and basic properties work
      assertNotNull(engineType.getEngine());
      assertNotNull(engineType.getMajorVersion());

      log.info("Basic feature support validation passed for {}", engineType);
    }
  }
}
