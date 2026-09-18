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
      "elasticsearch.shim.engineType=ELASTICSEARCH_8",
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
      SearchEngineType.ELASTICSEARCH_8,
      SearchEngineType.ELASTICSEARCH_9,
      SearchEngineType.OPENSEARCH_2,
      SearchEngineType.OPENSEARCH_3
    };

    for (SearchEngineType engineType : supportedTypes) {
      log.info("Verifying engine type: {}", engineType);
      assertNotNull(engineType);
      assertNotNull(engineType.getEngine());
      assertNotNull(engineType.getMajorVersion());
    }
  }
}
