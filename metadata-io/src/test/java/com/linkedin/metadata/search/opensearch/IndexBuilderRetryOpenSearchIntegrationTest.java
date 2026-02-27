package com.linkedin.metadata.search.opensearch;

import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.StructuredPropertiesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.systemmetadata.SystemMetadataMappingsBuilder;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.test.search.FaultSpec;
import io.datahubproject.test.search.config.RetryFaultInjectionTestConfiguration;
import java.util.Map;
import java.util.Optional;
import org.opensearch.action.admin.indices.refresh.RefreshRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.testng.AbstractTestNGSpringContextTests;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Integration tests that use {@link io.datahubproject.test.search.FaultInjectingSearchClientShim}
 * to simulate count/createIndex failures and assert retry logic succeeds against a real OpenSearch
 * container.
 */
@Import({OpenSearchSuite.class, RetryFaultInjectionTestConfiguration.class})
public class IndexBuilderRetryOpenSearchIntegrationTest extends AbstractTestNGSpringContextTests {

  private static final String TEST_INDEX_NAME = "estest_datasetindex_v2";
  private static final String CREATE_INDEX_RETRY_INDEX = "estest_retry_createindex_os_v1";
  private static final int NDOCS = 5;

  @Autowired private SearchClientShim<?> searchClient;
  @Autowired private ESIndexBuilder indexBuilder;

  @AfterMethod(alwaysRun = true)
  public void clearFaultSpec() {
    FaultSpec.Holder.clear();
  }

  @Test
  public void testCreateIndexRetrySucceedsAfterSimulatedFailure() throws Exception {
    FaultSpec.Holder.set(FaultSpec.builder().createIndexFailures(1).build());

    ReindexConfig reindexConfig =
        indexBuilder.buildReindexState(
            CREATE_INDEX_RETRY_INDEX, SystemMetadataMappingsBuilder.getMappings(), Map.of());
    ReindexResult result = indexBuilder.buildIndex(reindexConfig);

    assertEquals(result, ReindexResult.CREATED_NEW);
    assertTrue(
        searchClient.indexExists(
            new GetIndexRequest(CREATE_INDEX_RETRY_INDEX), RequestOptions.DEFAULT));
    GetIndexResponse getIndex =
        searchClient.getIndex(
            new GetIndexRequest(CREATE_INDEX_RETRY_INDEX).includeDefaults(true),
            RequestOptions.DEFAULT);
    assertNotNull(getIndex.getIndices());
    assertTrue(getIndex.getIndices().length > 0);
  }

  @Test
  public void testCountRetrySucceedsAfterSimulatedTimeout() throws Exception {
    FaultSpec.Holder.set(
        FaultSpec.builder()
            .countFailures(1)
            .countExceptionType(FaultSpec.CountExceptionType.SOCKET_TIMEOUT)
            .build());

    GitVersion gitVersion = new GitVersion("0.0.0-test", "123456", Optional.empty());
    StructuredPropertiesConfiguration structPropConfig =
        StructuredPropertiesConfiguration.builder().systemUpdateEnabled(false).build();
    ElasticSearchConfiguration configWith1Shard =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .entityIndex(V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION)
            .index(
                TEST_ES_SEARCH_CONFIG.getIndex().toBuilder()
                    .numShards(1)
                    .numReplicas(0)
                    .numRetries(3)
                    .refreshIntervalSeconds(0)
                    .build())
            .build();
    ESIndexBuilder builder1Shard =
        new ESIndexBuilder(searchClient, configWith1Shard, structPropConfig, Map.of(), gitVersion);

    ReindexConfig reindexConfig1 =
        builder1Shard.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    builder1Shard.buildIndex(reindexConfig1);

    for (int i = 0; i < NDOCS; i++) {
      searchClient.indexDocument(
          new IndexRequest(TEST_INDEX_NAME).id("" + i).source(Map.of(), XContentType.JSON),
          RequestOptions.DEFAULT);
    }
    searchClient.refreshIndex(new RefreshRequest(TEST_INDEX_NAME), RequestOptions.DEFAULT);

    ElasticSearchConfiguration configWith2Shards =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .entityIndex(V2_V3_ENABLED_ENTITY_INDEX_CONFIGURATION)
            .index(
                TEST_ES_SEARCH_CONFIG.getIndex().toBuilder()
                    .numShards(2)
                    .numReplicas(0)
                    .numRetries(3)
                    .refreshIntervalSeconds(0)
                    .build())
            .build();
    ESIndexBuilder builder2Shards =
        new ESIndexBuilder(searchClient, configWith2Shards, structPropConfig, Map.of(), gitVersion);

    ReindexConfig reindexConfig2 =
        builder2Shards.buildReindexState(TEST_INDEX_NAME, Map.of(), Map.of());
    ReindexResult rr = builder2Shards.buildIndex(reindexConfig2);

    assertEquals(rr, ReindexResult.REINDEXING);
    GetIndexResponse indexResponse =
        searchClient.getIndex(
            new GetIndexRequest(TEST_INDEX_NAME).includeDefaults(true), RequestOptions.DEFAULT);
    assertNotNull(indexResponse.getIndices());
    assertTrue(indexResponse.getIndices().length > 0);
    String concreteIndex = indexResponse.getIndices()[0];
    long count = builder2Shards.getCount(concreteIndex);
    assertEquals(count, NDOCS, "Expected " + NDOCS + " documents after reindex with count retry");
  }
}
