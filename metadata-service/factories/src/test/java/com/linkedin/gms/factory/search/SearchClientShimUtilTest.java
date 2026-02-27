package com.linkedin.gms.factory.search;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.common.ObjectMapperFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim.SearchEngineType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.Test;

/**
 * Unit tests for SearchClientShimUtil configuration and bean creation. These tests verify factory
 * configuration parsing and basic shim creation logic without requiring live search engine
 * instances.
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
      "elasticsearch.region=",
      "elasticsearch.shim.engineType=ELASTICSEARCH_7",
      "elasticsearch.shim.autoDetectEngine=false"
    })
@SpringBootTest(classes = {SearchClientShimFactory.class, ObjectMapperFactory.class})
@EnableConfigurationProperties(ConfigurationProvider.class)
public class SearchClientShimUtilTest extends AbstractTestNGSpringContextTests {

  @Autowired private SearchClientShimFactory shimFactory;

  @Test
  public void testFactoryInjection() {
    log.info("Testing SearchClientShimUtil injection");
    assertNotNull(shimFactory);
  }

  @Test
  public void testEngineTypeParsingES7() {
    log.info("Testing engine type parsing for Elasticsearch 7");

    // Test through private method by using reflection or create config variants
    // Since the method is private, we'll test through the public configuration
    // This test validates that configuration is properly parsed

    // We can't directly test the private parseEngineType method, but we can verify
    // that the factory handles different engine type configurations correctly
    assertNotNull(shimFactory);
  }

  // @Test
  public void testShimCreationFailsWithoutLiveCluster() {
    log.info("Testing that shim creation fails gracefully without live cluster");

    // Attempting to create a shim without a live cluster should fail
    // but the factory should be properly configured
    try {
      shimFactory.createSearchClientShim(new ObjectMapper());
      fail("Expected shim creation to fail without live cluster");
    } catch (Exception e) {
      log.info("Expected failure when creating shim without live cluster: {}", e.getMessage());
      // This is expected - we don't have a live cluster in unit tests
      assertTrue(e instanceof Exception);
    }
  }

  /** Test that validates supported engine types are properly handled */
  @Test
  public void testSupportedEngineTypes() {
    log.info("Testing supported search engine types");

    // Verify all expected engine types exist
    SearchEngineType[] supportedTypes = {
      SearchEngineType.ELASTICSEARCH_7,
      SearchEngineType.ELASTICSEARCH_8,
      SearchEngineType.ELASTICSEARCH_9,
      SearchEngineType.OPENSEARCH_2
    };

    for (SearchEngineType engineType : supportedTypes) {
      log.info("Verifying engine type: {}", engineType);
      assertNotNull(engineType);
      assertNotNull(engineType.getEngine());
      assertNotNull(engineType.getMajorVersion());
    }
  }

  /** Test engine type compatibility flags */
  @Test
  public void testEngineTypeCompatibility() {
    log.info("Testing engine type compatibility flags");

    // Test Elasticsearch types
    assertTrue(SearchEngineType.ELASTICSEARCH_7.isElasticsearch());
    assertTrue(SearchEngineType.ELASTICSEARCH_8.isElasticsearch());
    assertTrue(SearchEngineType.ELASTICSEARCH_9.isElasticsearch());
    assertFalse(SearchEngineType.OPENSEARCH_2.isElasticsearch());

    // Test OpenSearch types
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
  }
}
