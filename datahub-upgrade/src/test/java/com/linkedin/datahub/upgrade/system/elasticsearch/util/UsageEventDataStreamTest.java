package com.linkedin.datahub.upgrade.system.elasticsearch.util;

import static org.testng.Assert.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.search.BaseElasticSearchComponentsFactory;
import com.linkedin.metadata.search.elasticsearch.client.shim.SearchClientShimUtil;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import java.io.IOException;
import java.time.Duration;
import lombok.extern.slf4j.Slf4j;
import org.mockito.Mockito;
import org.opensearch.client.Request;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.utility.DockerImageName;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * Integration tests for UsageEventIndexUtils focusing on data stream creation with real
 * Elasticsearch instances.
 *
 * <p>This test validates the fix for PFP-2428: ensuring that the createDataStream() method
 * correctly uses the Elasticsearch data stream API (PUT /_data_stream/{name}) instead of the
 * regular index creation API.
 *
 * <p>The issue: When an index template has "data_stream": {} configured, Elasticsearch requires
 * using the data stream API. The old implementation was using the indices API, causing errors:
 * "cannot create index with name [...], because it matches with template [...] that creates data
 * streams only, use create data stream api instead"
 *
 * <p>Tests run against both Elasticsearch 7 and 8 using testcontainers to ensure compatibility
 * across versions.
 */
@Slf4j
public class UsageEventDataStreamTest {

  private static final int HTTP_PORT = 9200;
  private static final Duration STARTUP_TIMEOUT = Duration.ofMinutes(3);

  // Per-class container lifecycle (matches @BeforeClass/@AfterClass)
  private ElasticsearchContainer elasticsearchContainer = null;
  private SearchClientShim<?> searchClient;
  private BaseElasticSearchComponentsFactory.BaseElasticSearchComponents esComponents;

  private static final String DEFAULT_ELASTIC_VERSION = "8.17.4";

  @Parameters({"elasticVersion"})
  @BeforeClass(alwaysRun = true)
  public void setUpEnvironment(@org.testng.annotations.Optional String elasticVersion)
      throws IOException {
    // Use default version if not specified (for running outside of testng-datastream-*.xml)
    String version =
        (elasticVersion != null && !elasticVersion.isEmpty())
            ? elasticVersion
            : DEFAULT_ELASTIC_VERSION;
    System.setProperty("ELASTIC_VERSION", version);
    log.info("========================================");
    log.info("Starting Elasticsearch {} testcontainer", version);
    log.info("========================================");

    String elasticImageName =
        System.getProperty("ELASTIC_IMAGE_NAME", "docker.elastic.co/elasticsearch/elasticsearch");
    String elasticImageFullName = elasticImageName + ":" + version;

    // Create and start container
    DockerImageName dockerImageName =
        DockerImageName.parse(elasticImageFullName).asCompatibleSubstituteFor(elasticImageName);

    elasticsearchContainer =
        new ElasticsearchContainer(dockerImageName)
            .withEnv("xpack.security.enabled", "true")
            .withEnv("xpack.security.http.ssl.enabled", "false")
            .withEnv("xpack.security.authc.anonymous.username", "anonymous")
            .withEnv("xpack.security.authc.anonymous.roles", "superuser")
            .withEnv("xpack.security.authc.anonymous.authz_exception", "false")
            .withEnv("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .withStartupTimeout(STARTUP_TIMEOUT);

    elasticsearchContainer.start();

    // Create search client
    SearchClientShimUtil.ShimConfigurationBuilder shimConfigurationBuilder =
        new SearchClientShimUtil.ShimConfigurationBuilder()
            .withHost("localhost")
            .withSSL(false)
            .withPort(elasticsearchContainer.getMappedPort(HTTP_PORT))
            .withThreadCount(1)
            .withConnectionRequestTimeout(30000);

    searchClient =
        SearchClientShimUtil.createShimWithAutoDetection(
            shimConfigurationBuilder.build(), new ObjectMapper());

    // Create ES components mock - BaseElasticSearchComponents is a @lombok.Value class (final)
    // so we need to use Mockito to mock it rather than subclassing
    esComponents =
        Mockito.mock(BaseElasticSearchComponentsFactory.BaseElasticSearchComponents.class);
    // Use doReturn to avoid generic type issues with wildcards
    Mockito.doReturn(searchClient).when(esComponents).getSearchClient();

    log.info("Test environment ready:");
    log.info("  ES version: {}", searchClient.getEngineVersion());
    log.info("  Engine type: {}", searchClient.getEngineType());
  }

  @AfterClass(alwaysRun = true)
  public void tearDown() {
    if (searchClient != null) {
      try {
        searchClient.close();
      } catch (IOException e) {
        log.warn("Error closing search client", e);
      }
    }

    if (elasticsearchContainer != null) {
      elasticsearchContainer.stop();
      log.info("Stopped Elasticsearch testcontainer");
    }
  }

  /**
   * Test that createDataStream() successfully creates a data stream when a matching template
   * exists.
   *
   * <p>This is the core fix for PFP-2428: ensuring that the method uses PUT /_data_stream/{name}
   * instead of the indices API when the template has "data_stream": {} configured.
   *
   * <p>Without the fix, this test would fail with: "OpenSearch exception
   * [type=illegal_argument_exception, reason=cannot create index with name [...], because it
   * matches with template [...] that creates data streams only, use create data stream api
   * instead]"
   */
  @Test(priority = 1, timeOut = 120000) // 2 minute timeout
  public void testCreateDataStreamWithTemplate() throws Exception {
    String testPrefix = "test_ds_";
    String policyName = testPrefix + "datahub_usage_event_policy";
    String templateName = testPrefix + "datahub_usage_event_template";
    // Data stream name must match template pattern: PREFIXdatahub_usage_event*
    String dataStreamName = testPrefix + "datahub_usage_event";

    log.info("========================================");
    log.info("TEST: Create Data Stream with Template");
    log.info("========================================");

    try {
      // 1. Create ILM policy
      log.info("Step 1: Creating ILM policy: {}", policyName);
      UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);
      assertTrue(ilmPolicyExists(policyName), "ILM policy should exist after creation");

      // 2. Create index template with data_stream configuration
      log.info("Step 2: Creating index template: {}", templateName);
      UsageEventIndexUtils.createIndexTemplate(
          esComponents, templateName, policyName, 1, 0, testPrefix);
      assertTrue(templateExists(templateName), "Template should exist after creation");

      // 3. Create data stream - this should use the proper data stream API
      log.info("Step 3: Creating data stream: {}", dataStreamName);
      UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);

      // 4. Verify the data stream was created
      assertTrue(dataStreamExists(dataStreamName), "Data stream should exist after creation");

      // 5. Verify it's actually a data stream (not a regular index)
      assertTrue(
          isDataStream(dataStreamName),
          "Created resource should be a data stream, not a regular index");

      log.info("✅ Test passed: Data stream created successfully");
    } finally {
      // Cleanup in reverse order
      cleanupDataStream(dataStreamName);
      cleanupTemplate(templateName);
      cleanupIlmPolicy(policyName);
    }
  }

  /**
   * Test that createDataStream() is idempotent - calling it multiple times should not fail.
   *
   * <p>This ensures the error handling properly detects when a data stream already exists.
   */
  @Test(priority = 2, timeOut = 120000) // 2 minute timeout
  public void testCreateDataStreamIdempotent() throws Exception {
    String testPrefix = "test_idempotent_";
    String policyName = testPrefix + "datahub_usage_event_policy";
    String templateName = testPrefix + "datahub_usage_event_template";
    // Data stream name must match template pattern: PREFIXdatahub_usage_event*
    String dataStreamName = testPrefix + "datahub_usage_event";

    log.info("========================================");
    log.info("TEST: Data Stream Idempotency");
    log.info("========================================");

    try {
      // Setup
      log.info("Setting up ILM policy and template");
      UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);
      UsageEventIndexUtils.createIndexTemplate(
          esComponents, templateName, policyName, 1, 0, testPrefix);

      // Create data stream first time
      log.info("Creating data stream (first time)");
      UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);
      assertTrue(dataStreamExists(dataStreamName), "Data stream should exist after first creation");

      // Create data stream second time - should not throw exception
      log.info("Creating data stream (second time - should be idempotent)");
      UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);
      assertTrue(
          dataStreamExists(dataStreamName), "Data stream should still exist after second creation");

      log.info("✅ Test passed: Data stream creation is idempotent");
    } finally {
      // Cleanup
      cleanupDataStream(dataStreamName);
      cleanupTemplate(templateName);
      cleanupIlmPolicy(policyName);
    }
  }

  /**
   * Test that the full workflow (policy + template + data stream) works end-to-end.
   *
   * <p>This simulates what happens in CreateUsageEventIndicesStep during system upgrade - the exact
   * scenario that was failing in production (PFP-2428).
   */
  @Test(priority = 3, timeOut = 120000) // 2 minute timeout
  public void testFullDataStreamWorkflow() throws Exception {
    String prefix = "test_workflow_";
    String policyName = prefix + "datahub_usage_event_policy";
    String templateName = prefix + "datahub_usage_event_index_template";
    String dataStreamName = prefix + "datahub_usage_event";

    log.info("========================================");
    log.info("TEST: Full Data Stream Workflow");
    log.info("========================================");

    try {
      // Simulate the full workflow from CreateUsageEventIndicesStep
      log.info("Running full workflow (simulating CreateUsageEventIndicesStep):");
      log.info("  1. Create ILM policy");
      UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);

      log.info("  2. Create index template with data_stream config");
      UsageEventIndexUtils.createIndexTemplate(
          esComponents, templateName, policyName, 1, 0, prefix);

      log.info("  3. Create data stream (this is where PFP-2428 was failing)");
      UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);

      // Verify everything was created
      assertTrue(dataStreamExists(dataStreamName), "Data stream should exist");
      assertTrue(isDataStream(dataStreamName), "Should be a data stream, not a regular index");

      // Verify we can query the data stream info
      log.info("  4. Verify data stream metadata");
      RawResponse response =
          searchClient.performLowLevelRequest(
              new Request("GET", "/_data_stream/" + dataStreamName));
      assertEquals(
          response.getStatusLine().getStatusCode(),
          200,
          "Should be able to query data stream info");

      log.info("✅ Test passed: Full workflow completed successfully");
    } finally {
      // Cleanup
      cleanupDataStream(dataStreamName);
      cleanupTemplate(templateName);
      cleanupIlmPolicy(policyName);
    }
  }

  /**
   * Test that we can write data to the data stream after creation.
   *
   * <p>This ensures the data stream is not just created, but actually functional.
   */
  @Test(priority = 4, timeOut = 120000) // 2 minute timeout
  public void testDataStreamCanAcceptData() throws Exception {
    String testPrefix = "test_data_";
    String policyName = testPrefix + "datahub_usage_event_policy";
    String templateName = testPrefix + "datahub_usage_event_template";
    // Data stream name must match template pattern: PREFIXdatahub_usage_event*
    String dataStreamName = testPrefix + "datahub_usage_event";

    log.info("========================================");
    log.info("TEST: Data Stream Can Accept Data");
    log.info("========================================");

    try {
      // Setup
      log.info("Setting up data stream");
      UsageEventIndexUtils.createIlmPolicy(esComponents, policyName);
      UsageEventIndexUtils.createIndexTemplate(
          esComponents, templateName, policyName, 1, 0, testPrefix);
      UsageEventIndexUtils.createDataStream(esComponents, dataStreamName);

      // Write a test document to the data stream
      log.info("Writing test document to data stream");
      String testDoc =
          "{\"@timestamp\": \"2024-01-20T10:00:00Z\", \"type\": \"test\", \"message\": \"test data\"}";
      Request indexRequest = new Request("POST", "/" + dataStreamName + "/_doc");
      indexRequest.setJsonEntity(testDoc);
      RawResponse indexResponse = searchClient.performLowLevelRequest(indexRequest);

      assertEquals(
          indexResponse.getStatusLine().getStatusCode(),
          201,
          "Should be able to write to data stream");

      log.info("✅ Test passed: Data stream accepts data successfully");
    } finally {
      // Cleanup
      cleanupDataStream(dataStreamName);
      cleanupTemplate(templateName);
      cleanupIlmPolicy(policyName);
    }
  }

  // Helper methods

  private boolean ilmPolicyExists(String policyName) {
    try {
      RawResponse response =
          searchClient.performLowLevelRequest(new Request("GET", "/_ilm/policy/" + policyName));
      return response.getStatusLine().getStatusCode() == 200;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean templateExists(String templateName) {
    try {
      RawResponse response =
          searchClient.performLowLevelRequest(
              new Request("GET", "/_index_template/" + templateName));
      return response.getStatusLine().getStatusCode() == 200;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean dataStreamExists(String dataStreamName) {
    try {
      RawResponse response =
          searchClient.performLowLevelRequest(
              new Request("GET", "/_data_stream/" + dataStreamName));
      return response.getStatusLine().getStatusCode() == 200;
    } catch (Exception e) {
      log.debug("Data stream {} does not exist: {}", dataStreamName, e.getMessage());
      return false;
    }
  }

  private boolean isDataStream(String dataStreamName) {
    try {
      RawResponse response =
          searchClient.performLowLevelRequest(
              new Request("GET", "/_data_stream/" + dataStreamName));
      if (response.getStatusLine().getStatusCode() == 200) {
        // If we can query it via _data_stream API, it's a data stream
        return true;
      }
    } catch (Exception e) {
      // Fall through
    }
    return false;
  }

  private void cleanupDataStream(String dataStreamName) {
    try {
      Request request = new Request("DELETE", "/_data_stream/" + dataStreamName);
      searchClient.performLowLevelRequest(request);
      log.debug("Cleaned up data stream: {}", dataStreamName);
    } catch (Exception e) {
      log.debug("Error cleaning up data stream {}: {}", dataStreamName, e.getMessage());
    }
  }

  private void cleanupTemplate(String templateName) {
    try {
      Request request = new Request("DELETE", "/_index_template/" + templateName);
      searchClient.performLowLevelRequest(request);
      log.debug("Cleaned up template: {}", templateName);
    } catch (Exception e) {
      log.debug("Error cleaning up template {}: {}", templateName, e.getMessage());
    }
  }

  private void cleanupIlmPolicy(String policyName) {
    try {
      Request request = new Request("DELETE", "/_ilm/policy/" + policyName);
      searchClient.performLowLevelRequest(request);
      log.debug("Cleaned up ILM policy: {}", policyName);
    } catch (Exception e) {
      log.debug("Error cleaning up ILM policy {}: {}", policyName, e.getMessage());
    }
  }
}
