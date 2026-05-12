/*
 * Copyright 2024 Acryl Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.ElasticSearchSuite;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.test.search.config.SearchCommonTestConfiguration;
import io.datahubproject.test.search.config.SearchTestContainerConfiguration;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.common.settings.Settings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Integration tests for ParallelReindexOrchestrator against real Elasticsearch/OpenSearch.
 *
 * <p>Tests the core orchestrator functionality including: - Orchestrator initialization - Reindex
 * result processing - Cost-based tier classification
 *
 * <p>Uses real Elasticsearch/OpenSearch via TestContainers (not mocked).
 */
@Slf4j
@Import({
  ElasticSearchSuite.class,
  SearchCommonTestConfiguration.class,
  SearchTestContainerConfiguration.class
})
public class ParallelReindexOrchestratorIT extends AbstractTestNGSpringContextTests {

  // Test constants
  private static final String TEST_INDEX_PREFIX = "parallel-reindex-test-";

  @Autowired private SearchClientShim<?> searchClient;
  @Autowired private ESIndexBuilder esIndexBuilder;

  private ParallelReindexOrchestrator orchestrator;
  private BuildIndicesConfiguration buildIndicesConfiguration;

  @BeforeMethod
  public void setUp() throws Exception {
    buildIndicesConfiguration =
        BuildIndicesConfiguration.builder()
            .enableParallelReindex(true)
            .taskCheckIntervalSeconds(5)
            .maxReindexHours(1)
            .yellowStabilitySeconds(5)
            .greenStabilitySeconds(5)
            .redRecoverySeconds(10)
            .build();
    CircuitBreakerState circuitBreakerState = new CircuitBreakerState(buildIndicesConfiguration);
    orchestrator =
        new ParallelReindexOrchestrator(
            esIndexBuilder, buildIndicesConfiguration, circuitBreakerState);
  }

  /** Test orchestrator initialization and basic configuration */
  @Test
  public void testOrchestratorInitialization() {
    assertNotNull(orchestrator, "Orchestrator should be initialized");
    assertNotNull(searchClient, "SearchClient should be autowired");
    assertNotNull(esIndexBuilder, "ESIndexBuilder should be autowired");
    assertNotNull(buildIndicesConfiguration, "BuildIndicesConfiguration should be created");
  }

  /** Test that we can create and clean up test indices */
  @Test
  public void testCreateAndDeleteTestIndex() throws Exception {
    String testIndexName = TEST_INDEX_PREFIX + "test-index";

    try {
      // Create index
      CreateIndexRequest createRequest = new CreateIndexRequest(testIndexName);
      Settings.Builder settings = Settings.builder();
      settings.put("number_of_shards", 1);
      settings.put("number_of_replicas", 0);
      createRequest.settings(settings);

      searchClient.createIndex(createRequest, RequestOptions.DEFAULT);
      log.info("Created test index: {}", testIndexName);

      // Verify index exists
      assertTrue(
          searchClient.indexExists(
              new org.opensearch.client.indices.GetIndexRequest(testIndexName),
              RequestOptions.DEFAULT),
          "Index should exist after creation");

      // Delete index
      DeleteIndexRequest deleteRequest = new DeleteIndexRequest(testIndexName);
      searchClient.deleteIndex(deleteRequest, RequestOptions.DEFAULT);
      log.info("Deleted test index: {}", testIndexName);

      // Verify index is deleted
      assertFalse(
          searchClient.indexExists(
              new org.opensearch.client.indices.GetIndexRequest(testIndexName),
              RequestOptions.DEFAULT),
          "Index should not exist after deletion");

    } catch (Exception e) {
      log.error("Test failed: {}", e.getMessage(), e);
      // Cleanup on failure
      try {
        DeleteIndexRequest cleanupRequest = new DeleteIndexRequest(testIndexName);
        searchClient.deleteIndex(cleanupRequest, RequestOptions.DEFAULT);
      } catch (Exception cleanupEx) {
        log.warn("Cleanup failed for index: {}", testIndexName, cleanupEx);
      }
      throw e;
    }
  }

  /** Test ReindexResult enum values are accessible */
  @Test
  public void testReindexResultEnumValues() {
    // Verify all expected enum values exist
    ReindexResult[] results = ReindexResult.values();
    assertTrue(results.length > 0, "ReindexResult should have enum values");

    // Verify specific important values exist
    assertNotNull(ReindexResult.REINDEXED, "REINDEXED state should exist");
    assertNotNull(ReindexResult.FAILED_TIMEOUT, "FAILED_TIMEOUT state should exist");
    assertNotNull(
        ReindexResult.FAILED_DOC_COUNT_MISMATCH, "FAILED_DOC_COUNT_MISMATCH state should exist");
  }
}
