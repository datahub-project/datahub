package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_ES_STRUCT_PROPS_DISABLED;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.IndexConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions.ReplicaHealthException;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import com.linkedin.metadata.version.GitVersion;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.http.HttpEntity;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.reindex.ReindexRequest;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ESIndexBuilderTest {

  @Mock private SearchClientShim<?> searchClient;
  @Mock private ElasticSearchConfiguration elasticSearchConfiguration;
  @Mock private BuildIndicesConfiguration buildIndicesConfig;
  @Mock private GitVersion gitVersion;
  @Mock private RawResponse response;
  @Mock private HttpEntity httpEntity;

  private ESIndexBuilder indexBuilder;
  private static final String TEST_INDEX_NAME = "test_index";
  private static final int NUM_SHARDS = 1;
  private static final int NUM_REPLICAS = 1;
  private static final int NUM_RETRIES = 3;
  private static final int REFRESH_INTERVAL_SECONDS = 1;

  @BeforeMethod
  void setUp() {
    MockitoAnnotations.openMocks(this);

    // Mock the JVM info response
    try {
      // Only mock JVM stats response, not the root endpoint
      RawResponse jvmResponse = mock(RawResponse.class);
      HttpEntity jvmEntity = mock(HttpEntity.class);
      String jvmJson =
          "{\"nodes\":{\"node1\":{\"roles\":[\"data\"],\"jvm\":{\"mem\":{\"heap_max_in_bytes\":17179869184}}}}}";
      when(jvmEntity.getContent()).thenReturn(new ByteArrayInputStream(jvmJson.getBytes()));
      when(jvmResponse.getEntity()).thenReturn(jvmEntity);

      // Only mock nodes stats endpoint
      when(searchClient.performLowLevelRequest(
              argThat(req -> req != null && req.getEndpoint().contains("_nodes/stats"))))
          .thenReturn(jvmResponse);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    when(gitVersion.getVersion()).thenReturn("1.0.0");
    when(elasticSearchConfiguration.getBuildIndices()).thenReturn(buildIndicesConfig);
    when(buildIndicesConfig.getRetentionValue()).thenReturn(7L);
    when(buildIndicesConfig.getRetentionUnit()).thenReturn(ChronoUnit.DAYS.name());
    when(buildIndicesConfig.isAllowDocCountMismatch()).thenReturn(false);
    when(buildIndicesConfig.isCloneIndices()).thenReturn(false);
    when(buildIndicesConfig.isReindexOptimizationEnabled()).thenReturn(true);
    when(buildIndicesConfig.getReindexBatchSize()).thenReturn(5000);
    when(buildIndicesConfig.getReindexMaxSlices()).thenReturn(256);
    when(buildIndicesConfig.getReindexNoProgressRetryMinutes()).thenReturn(5);

    // Create a configuration with the test values
    when(elasticSearchConfiguration.getIndex())
        .thenReturn(
            IndexConfiguration.builder()
                .numShards(NUM_SHARDS)
                .numReplicas(NUM_REPLICAS)
                .numRetries(NUM_RETRIES)
                .refreshIntervalSeconds(REFRESH_INTERVAL_SECONDS)
                .maxReindexHours(0)
                .build());

    indexBuilder =
        new ESIndexBuilder(
            searchClient,
            elasticSearchConfiguration,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);
  }

  @Test
  void testConstructor() {
    // Verify that the configuration objects are properly set
    Assert.assertNotNull(indexBuilder.getConfig());
    Assert.assertNotNull(indexBuilder.getGitVersion());
    Assert.assertNotNull(indexBuilder.getStructPropConfig());

    // Verify the configuration contains the expected values
    assertEquals(indexBuilder.getStructPropConfig(), TEST_ES_STRUCT_PROPS_DISABLED);
    assertEquals(indexBuilder.getGitVersion(), gitVersion);
    assertEquals(indexBuilder.getConfig().getIndex().getNumShards(), NUM_SHARDS);
    assertEquals(indexBuilder.getConfig().getIndex().getNumReplicas(), NUM_REPLICAS);
    assertEquals(indexBuilder.getConfig().getIndex().getNumRetries(), NUM_RETRIES);
    assertEquals(
        indexBuilder.getConfig().getIndex().getRefreshIntervalSeconds(), REFRESH_INTERVAL_SECONDS);
  }

  @Test
  void testConstructorWithMaxReindexHours() {
    int maxReindexHours = 24;

    // Create a configuration with the max reindex hours
    ElasticSearchConfiguration configWithTimeout =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .index(
                TEST_ES_SEARCH_CONFIG.getIndex().toBuilder()
                    .maxReindexHours(maxReindexHours)
                    .build())
            .build();

    ESIndexBuilder builderWithTimeout =
        new ESIndexBuilder(
            searchClient, configWithTimeout, TEST_ES_STRUCT_PROPS_DISABLED, Map.of(), gitVersion);

    // Verify the configuration contains the expected max reindex hours
    assertEquals(configWithTimeout.getIndex().getMaxReindexHours(), maxReindexHours);
  }

  @Test
  void testIsOpenSearch29OrHigher_OpenSearch29() throws IOException {
    // Get the actual lowLevelClient used by indexBuilder

    // Create fresh mocks for this test
    RawResponse opensearchResponse = mock(RawResponse.class);
    HttpEntity opensearchEntity = mock(HttpEntity.class);

    // Mock OpenSearch 2.9 response
    String responseJson =
        "{"
            + "\"name\": \"node-1\","
            + "\"cluster_name\": \"opensearch\","
            + "\"cluster_uuid\": \"test\","
            + "\"version\": {"
            + "\"number\": \"2.9.0\","
            + "\"build_flavor\": \"default\""
            + "},"
            + "\"tagline\": \"The OpenSearch Project\""
            + "}";

    when(opensearchEntity.getContent())
        .thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
    when(opensearchResponse.getEntity()).thenReturn(opensearchEntity);

    // Override the mock specifically for the root endpoint
    when(searchClient.performLowLevelRequest(
            argThat(req -> req != null && req.getEndpoint().equals("/"))))
        .thenReturn(opensearchResponse);

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    assertTrue(result);
  }

  @Test
  void testIsOpenSearch29OrHigher_Elasticsearch() throws IOException {
    // Mock Elasticsearch response
    String responseJson =
        "{"
            + "\"name\": \"node-1\","
            + "\"cluster_name\": \"elasticsearch\","
            + "\"cluster_uuid\": \"test\","
            + "\"version\": {"
            + "\"number\": \"7.17.0\","
            + "\"build_flavor\": \"default\""
            + "},"
            + "\"tagline\": \"You Know, for Search\""
            + "}";

    when(searchClient.performLowLevelRequest(any(Request.class))).thenReturn(response);
    when(response.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(responseJson.getBytes()));

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    Assert.assertFalse(result);
  }

  @Test
  void testIsOpenSearch29OrHigher_OpenSearch28() throws IOException {
    // Mock OpenSearch 2.8 response (should return false)
    String responseJson =
        "{"
            + "\"name\": \"node-1\","
            + "\"cluster_name\": \"opensearch\","
            + "\"cluster_uuid\": \"test\","
            + "\"version\": {"
            + "\"number\": \"2.8.0\","
            + "\"build_flavor\": \"default\""
            + "},"
            + "\"tagline\": \"The OpenSearch Project\""
            + "}";

    when(searchClient.performLowLevelRequest(any(Request.class))).thenReturn(response);
    when(response.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(responseJson.getBytes()));

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    Assert.assertFalse(result);
  }

  @Test
  void testIsOpenSearch29OrHigher_Exception() throws IOException {
    when(searchClient.performLowLevelRequest(any(Request.class)))
        .thenThrow(new IOException("Network error"));

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    Assert.assertFalse(result); // Should return false defensively
  }

  @Test
  void testBuildReindexState_IndexDoesNotExist() throws IOException {
    Map<String, Object> mappings = createTestMappings();
    Map<String, Object> settings = createTestSettings();

    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    ReindexConfig result = indexBuilder.buildReindexState(TEST_INDEX_NAME, mappings, settings);

    assertEquals(result.name(), TEST_INDEX_NAME);
    Assert.assertFalse(result.exists());
    assertEquals(result.targetMappings(), mappings);
    Assert.assertNotNull(result.targetSettings());
  }

  @Test
  void testBuildIndex_NewIndex() throws IOException {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(false);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    ReindexResult result = indexBuilder.buildIndex(indexState);

    assertEquals(result, ReindexResult.CREATED_NEW);
    verify(searchClient).createIndex(any(CreateIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  void testBuildIndex_NoChangesNeeded() throws IOException {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(true);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.requiresApplyMappings()).thenReturn(false);
    when(indexState.requiresApplySettings()).thenReturn(false);
    when(indexState.currentMappings()).thenReturn(createTestMappings());
    when(indexState.targetMappings()).thenReturn(createTestMappings());

    ReindexResult result = indexBuilder.buildIndex(indexState);

    assertEquals(result, ReindexResult.NOT_REINDEXED_NOTHING_APPLIED);
  }

  @Test
  void testApplyMappings_InvalidMappingsWithSuppressError() throws IOException {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.isPureMappingsAddition()).thenReturn(false);
    when(indexState.isPureStructuredPropertyAddition()).thenReturn(false);
    when(indexState.currentMappings()).thenReturn(createTestMappings());
    when(indexState.targetMappings()).thenReturn(createTestMappings());

    // Should not throw exception when suppressError is true
    indexBuilder.applyMappings(indexState, true);

    // Should not attempt to put mapping
    verify(searchClient, never())
        .putIndexMapping(any(PutMappingRequest.class), any(RequestOptions.class));
  }

  @Test
  void testGetCount() throws IOException {
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    // Mock refreshIndex response
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(
            any(org.opensearch.action.admin.indices.refresh.RefreshRequest.class),
            any(RequestOptions.class)))
        .thenReturn(refreshResponse);

    long result = indexBuilder.getCount(TEST_INDEX_NAME);

    assertEquals(result, 100L);
  }

  @DataProvider(name = "replicaTestData")
  public Object[][] provideReplicaTestData() {
    return new Object[][] {
      // currentReplicas, docCount, expectedChanged, expectedAction, dryRun
      {0, 100, true, "Increase replicas from 0 to 1", false}, // Should increase
      {0, 100, true, "Increase replicas from 0 to 1", true}, // Dry run should increase
      {1, 100, false, "No change needed", false}, // No change needed
      {0, 0, false, "No change needed", false}, // Empty index, no change
      {2, 0, true, "Decrease replicas from 2 to 0", false}, // Should decrease
      {2, 0, true, "Decrease replicas from 2 to 0", true}, // Dry run should decrease
      {0, 0, false, "No change needed", false}, // Already optimized
    };
  }

  @Test
  void testCreateOperationSummary() throws IOException {
    Map<String, Object> increaseResult = new HashMap<>();
    increaseResult.put("indexName", TEST_INDEX_NAME);
    increaseResult.put("dryRun", false);
    increaseResult.put("documentCount", 100L);
    increaseResult.put("currentReplicas", 0);
    increaseResult.put("changed", true);

    Map<String, Object> reduceResult = new HashMap<>();
    reduceResult.put("changed", false);

    String summary = indexBuilder.createOperationSummary(increaseResult, reduceResult);

    assertTrue(summary.contains(TEST_INDEX_NAME));
    assertTrue(summary.contains("LIVE"));
    assertTrue(summary.contains("Active"));
    assertTrue(summary.contains("100 docs"));
    assertTrue(summary.contains("increased"));
  }

  @Test
  void testCleanIndex() {
    // Test static method behavior - should handle exceptions gracefully
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.indexPattern()).thenReturn(TEST_INDEX_NAME + "*");
    when(indexState.indexCleanPattern()).thenReturn(TEST_INDEX_NAME + "_*");

    // This should not throw an exception
    try {
      ESIndexBuilder.cleanOrphanedIndices(searchClient, elasticSearchConfiguration, indexState);
      // If we get here without exception, test passes
      assertTrue(true);
    } catch (Exception e) {
      // Expected for mocked environment, but shouldn't be a critical failure
      assertTrue(
          e.getMessage().contains("NullPointer")
              || e.getMessage().contains("Mock")
              || e.getMessage().contains("index_not_found"));
    }
  }

  @DataProvider(name = "indexOverrideData")
  public Object[][] provideIndexOverrideData() {
    return new Object[][] {
      {"test_index", Map.of("refresh_interval", "10s"), "10s"},
      {"test_index", Map.of(), String.format("%ss", REFRESH_INTERVAL_SECONDS)},
      {
        "other_index",
        Map.of("refresh_interval", "5s"),
        String.format("%ss", REFRESH_INTERVAL_SECONDS)
      },
    };
  }

  @Test
  void testBuildReindexState_WithShardCountChange() throws IOException {
    // Setup - existing index with 1 shard
    Map<String, Object> currentMappings = createTestMappings();
    Map<String, Object> targetSettings = createTestSettings();

    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    // Mock current settings with 1 shard
    Settings currentSettings =
        Settings.builder()
            .put("index.number_of_shards", "1")
            .put("index.number_of_replicas", "1")
            .build();

    GetSettingsResponse settingsResponse = mock(GetSettingsResponse.class);
    when(settingsResponse.getIndexToSettings())
        .thenReturn(Map.of(TEST_INDEX_NAME, currentSettings));
    when(searchClient.getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsResponse);

    GetMappingsResponse mappingsResponse = mock(GetMappingsResponse.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);
    when(mappingMetadata.getSourceAsMap()).thenReturn(currentMappings);
    when(mappingsResponse.mappings()).thenReturn(Map.of(TEST_INDEX_NAME, mappingMetadata));
    when(searchClient.getIndexMapping(any(GetMappingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mappingsResponse);

    // Execute with 6 shards configured
    ElasticSearchConfiguration configWith6Shards =
        TEST_ES_SEARCH_CONFIG.toBuilder()
            .index(
                TEST_ES_SEARCH_CONFIG.getIndex().toBuilder()
                    .numShards(6)
                    .numReplicas(NUM_REPLICAS)
                    .numRetries(NUM_RETRIES)
                    .refreshIntervalSeconds(REFRESH_INTERVAL_SECONDS)
                    .build())
            .build();

    ESIndexBuilder builderWith6Shards =
        new ESIndexBuilder(
            searchClient, configWith6Shards, TEST_ES_STRUCT_PROPS_DISABLED, Map.of(), gitVersion);

    ReindexConfig result =
        builderWith6Shards.buildReindexState(TEST_INDEX_NAME, currentMappings, targetSettings);

    // Verify
    assertTrue(result.exists());
    assertTrue(result.requiresReindex());
    assertEquals(result.name(), TEST_INDEX_NAME);
    Map<String, Object> indexSettings = (Map<String, Object>) result.targetSettings().get("index");
    assertEquals(indexSettings.get("number_of_shards"), 6);
  }

  @Test
  void testBuildIndex_ReindexFailureWithTypeMismatch() throws Exception {
    // Setup reindex state that requires reindexing
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresApplyMappings()).thenReturn(true);
    when(indexState.requiresApplySettings()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetSettings()).thenReturn(Map.of("index", Map.of("number_of_shards", "6")));

    // Mock index creation
    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    // Mock document counts
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    // Test the failure case
    assertThrows(RuntimeException.class, () -> indexBuilder.buildIndex(indexState));
  }

  @Test
  void testTweakReplicas_IncreasesFor0ReplicasWithDocuments() throws IOException {
    // Setup
    GetIndexRequest getIndexRequest = new GetIndexRequest(TEST_INDEX_NAME);
    GetIndexResponse getIndexResponse = mock(GetIndexResponse.class);

    Settings settings = Settings.builder().put("index.number_of_replicas", "0").build();

    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(getIndexResponse);
    when(getIndexResponse.getSettings()).thenReturn(Map.of(TEST_INDEX_NAME, settings));

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    AcknowledgedResponse updateResponse = mock(AcknowledgedResponse.class);
    when(updateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(updateResponse);

    // Execute
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    indexBuilder.tweakReplicas(indexState, false);

    // Verify replica increase was called
    verify(searchClient)
        .updateIndexSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class));
  }

  @Test
  void testTweakReplicas_DecreasesForEmptyIndices() throws IOException {
    // Setup
    Settings settings = Settings.builder().put("index.number_of_replicas", "2").build();

    GetIndexResponse getIndexResponse = mock(GetIndexResponse.class);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);
    when(searchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(getIndexResponse);
    when(getIndexResponse.getSettings()).thenReturn(Map.of(TEST_INDEX_NAME, settings));

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(0L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    AcknowledgedResponse updateResponse = mock(AcknowledgedResponse.class);
    when(updateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(updateResponse);

    // Execute
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    indexBuilder.tweakReplicas(indexState, false);

    // Verify replica decrease was called
    verify(searchClient)
        .updateIndexSettings(any(UpdateSettingsRequest.class), any(RequestOptions.class));
  }

  @Test
  void testReindexInPlaceAsync() throws Exception {
    // Setup
    String indexAlias = "test_alias";
    GetAliasesResponse aliasesResponse = mock(GetAliasesResponse.class);
    when(aliasesResponse.getAliases()).thenReturn(Map.of("test_index_old", new HashSet<>()));
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(aliasesResponse);

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    AcknowledgedResponse aliasResponse = mock(AcknowledgedResponse.class);
    when(aliasResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(aliasResponse);

    // Mock refreshIndex
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(any(), any(RequestOptions.class))).thenReturn(refreshResponse);

    // Mock settings operations for reindex optimization
    GetSettingsResponse getSettingsResponse = mock(GetSettingsResponse.class);
    when(getSettingsResponse.getSetting(anyString(), eq("index.translog.flush_threshold_size")))
        .thenReturn("512mb");
    when(searchClient.getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(getSettingsResponse);

    AcknowledgedResponse settingsUpdateResponse = mock(AcknowledgedResponse.class);
    when(settingsUpdateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsUpdateResponse);

    when(searchClient.submitReindexTask(any(), any())).thenReturn("task123");

    ReindexConfig config = mock(ReindexConfig.class);

    // Put number_of_shards directly in targetSettings
    Map<String, Object> targetSettings = new HashMap<>();
    targetSettings.put("number_of_shards", 6);

    when(config.targetSettings()).thenReturn(targetSettings);
    when(config.targetMappings()).thenReturn(createTestMappings());

    // Execute
    String taskId =
        indexBuilder.reindexInPlaceAsync(
            indexAlias,
            null,
            new com.linkedin.metadata.timeseries.BatchWriteOperationsOptions(1000, 300),
            config);

    // Verify
    assertEquals(taskId, "task123");
    verify(searchClient).createIndex(any(CreateIndexRequest.class), any(RequestOptions.class));
    verify(searchClient).submitReindexTask(any(), any());
  }

  @Test
  void testCleanIndex_DeletesOrphanedIndices() throws Exception {
    // Setup
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.indexPattern()).thenReturn("test_index*");
    when(indexState.indexCleanPattern()).thenReturn("test_index_*");

    GetIndexResponse getIndexResponse = mock(GetIndexResponse.class);
    String orphanIndex = "test_index_1234567890";
    when(getIndexResponse.getIndices()).thenReturn(new String[] {orphanIndex});
    when(getIndexResponse.getSetting(orphanIndex, "index.creation_date"))
        .thenReturn(
            String.valueOf(System.currentTimeMillis() - 10L * 24 * 60 * 60 * 1000)); // 10 days old
    when(getIndexResponse.getAliases()).thenReturn(Map.of(orphanIndex, List.of()));

    when(searchClient.getIndex(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(getIndexResponse);

    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    AcknowledgedResponse deleteResponse = mock(AcknowledgedResponse.class);
    when(deleteResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.deleteIndex(any(DeleteIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(deleteResponse);

    // Execute
    ESIndexBuilder.cleanOrphanedIndices(searchClient, elasticSearchConfiguration, indexState);

    // Verify deletion was attempted
    verify(searchClient, atLeastOnce())
        .deleteIndex(any(DeleteIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  void testApplyMappings_WithStructuredProperties() throws IOException {
    // Setup
    Map<String, Object> currentMappings = createTestMappings();
    Map<String, Object> targetMappings =
        createTestMappingsWithStructuredProperties(Collections.emptyMap());

    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.isPureMappingsAddition()).thenReturn(false);
    when(indexState.isPureStructuredPropertyAddition()).thenReturn(true);
    when(indexState.currentMappings()).thenReturn(currentMappings);
    when(indexState.targetMappings()).thenReturn(targetMappings);

    AcknowledgedResponse putMappingResponse = mock(AcknowledgedResponse.class);
    when(putMappingResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.putIndexMapping(any(PutMappingRequest.class), any(RequestOptions.class)))
        .thenReturn(putMappingResponse);

    // Execute
    indexBuilder.applyMappings(indexState, false);

    // Verify
    verify(searchClient).putIndexMapping(any(PutMappingRequest.class), any(RequestOptions.class));
  }

  @Test
  void testBuildIndex_HandlesOpenSearchStatusException() throws IOException {
    // Setup
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(false);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    // Simulate OpenSearchStatusException
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new OpenSearchStatusException("Index is read-only", RestStatus.FORBIDDEN));

    // Execute and verify exception
    assertThrows(OpenSearchStatusException.class, () -> indexBuilder.buildIndex(indexState));
  }

  @Test
  void testCreateIndex_TimeoutButIndexExists_ProceedsWithoutRetry() throws IOException {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(false);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection timed out"));
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(true);

    indexBuilder.buildIndex(indexState);

    verify(searchClient, times(1))
        .createIndex(any(CreateIndexRequest.class), any(RequestOptions.class));
    verify(searchClient).indexExists(any(GetIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  void testCreateIndex_FailsThenSucceedsOnRetry() throws IOException {
    when(buildIndicesConfig.isCreateIndexRetryEnabled()).thenReturn(true);

    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(false);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection reset"))
        .thenReturn(createResponse);
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    ReindexResult result = indexBuilder.buildIndex(indexState);

    assertEquals(result, ReindexResult.CREATED_NEW);
    verify(searchClient, times(2))
        .createIndex(any(CreateIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  void testCreateIndex_FailsTwice_ThrowsException() throws Exception {
    when(buildIndicesConfig.isCreateIndexRetryEnabled()).thenReturn(true);

    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(false);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenThrow(new IOException("Connection reset"));
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    assertThrows(Exception.class, () -> indexBuilder.buildIndex(indexState));
    verify(searchClient, times(2))
        .createIndex(any(CreateIndexRequest.class), any(RequestOptions.class));
  }

  @Test
  void testGetCount_WithRefresh() throws IOException {
    // Setup
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(any(), any(RequestOptions.class))).thenReturn(refreshResponse);

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(42L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    // Execute
    long count = indexBuilder.getCount(TEST_INDEX_NAME);

    // Verify
    assertEquals(count, 42L);
    verify(searchClient).refreshIndex(any(), any(RequestOptions.class));
  }

  @Test
  void testCreateOperationSummary_ComplexScenarios() {
    // Test 1: Increased replicas
    Map<String, Object> increaseResult = new HashMap<>();
    increaseResult.put("indexName", TEST_INDEX_NAME);
    increaseResult.put("dryRun", false);
    increaseResult.put("documentCount", 100L);
    increaseResult.put("currentReplicas", 0);
    increaseResult.put("changed", true);

    Map<String, Object> reduceResult = new HashMap<>();
    reduceResult.put("changed", false);

    String summary = indexBuilder.createOperationSummary(increaseResult, reduceResult);

    assertTrue(summary.contains("LIVE"));
    assertTrue(summary.contains("Active"));
    assertTrue(summary.contains("100 docs"));
    assertTrue(summary.contains("increased"));

    // Test 2: Reduced replicas
    increaseResult.put("changed", false);
    increaseResult.put("documentCount", 0L);
    increaseResult.put("currentReplicas", 2);
    reduceResult.put("changed", true);

    summary = indexBuilder.createOperationSummary(increaseResult, reduceResult);

    assertTrue(summary.contains("Empty"));
    assertTrue(summary.contains("0 docs"));
    assertTrue(summary.contains("reduced"));
  }

  @Test(dataProvider = "settingsOverrideData")
  void testIndexSettingOverrides(
      String indexName, Map<String, String> overrides, String expectedRefreshInterval)
      throws IOException {
    // Setup
    Map<String, Map<String, String>> indexOverrides = new HashMap<>();
    indexOverrides.put("test_index", overrides);

    ESIndexBuilder builderWithOverrides =
        new ESIndexBuilder(
            searchClient,
            elasticSearchConfiguration,
            TEST_ES_STRUCT_PROPS_DISABLED,
            indexOverrides,
            gitVersion);

    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    // Execute
    ReindexConfig result =
        builderWithOverrides.buildReindexState(
            indexName, createTestMappings(), createTestSettings());

    // Verify
    Map<String, Object> targetSettings = result.targetSettings();
    Map<String, Object> indexSettings = (Map<String, Object>) targetSettings.get("index");
    assertEquals(indexSettings.get("refresh_interval"), expectedRefreshInterval);
  }

  @DataProvider(name = "settingsOverrideData")
  public Object[][] provideSettingsOverrideData() {
    return new Object[][] {
      {"test_index", Map.of("refresh_interval", "10s"), "10s"},
      {"test_index", Map.of(), String.format("%ss", REFRESH_INTERVAL_SECONDS)},
      {
        "other_index",
        Map.of("refresh_interval", "5s"),
        String.format("%ss", REFRESH_INTERVAL_SECONDS)
      },
    };
  }

  @Test
  void testBuildReindexState_WithOpenSearch29() throws IOException {

    // Mock OpenSearch 2.9 response for root endpoint
    String responseJson =
        "{"
            + "\"name\": \"node-1\","
            + "\"cluster_name\": \"opensearch\","
            + "\"version\": {"
            + "\"number\": \"2.9.0\""
            + "},"
            + "\"tagline\": \"The OpenSearch Project\""
            + "}";

    RawResponse rootResponse = mock(RawResponse.class);
    HttpEntity rootEntity = mock(HttpEntity.class);
    when(rootEntity.getContent()).thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
    when(rootResponse.getEntity()).thenReturn(rootEntity);

    // Override the mock to return OpenSearch 2.9 for root endpoint
    when(searchClient.performLowLevelRequest(
            argThat(req -> req != null && req.getEndpoint().equals("/"))))
        .thenReturn(rootResponse);

    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);

    // Execute
    ReindexConfig result =
        indexBuilder.buildReindexState(TEST_INDEX_NAME, createTestMappings(), createTestSettings());

    // Verify codec is set for OpenSearch 2.9+
    Map<String, Object> indexSettings = (Map<String, Object>) result.targetSettings().get("index");
    assertEquals(indexSettings.get("codec"), "zstd_no_dict");
  }

  @Test
  void testReindexWithOptimizationDisabled() throws Exception {
    // Setup zone awareness enabled configuration
    when(buildIndicesConfig.isReindexOptimizationEnabled()).thenReturn(false);

    // Create index builder with zone awareness enabled
    ESIndexBuilder optimizationDisabledIndexBuilder =
        new ESIndexBuilder(
            searchClient,
            elasticSearchConfiguration,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);

    // Setup index state that requires reindexing
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresApplyMappings()).thenReturn(true);
    when(indexState.requiresApplySettings()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());

    // Setup target settings with index structure
    Map<String, Object> indexSettings = new HashMap<>();
    indexSettings.put("number_of_shards", 6);
    indexSettings.put("number_of_replicas", 1);
    indexSettings.put("refresh_interval", "1s");
    Map<String, Object> targetSettings = new HashMap<>();
    targetSettings.put("index", indexSettings);
    when(indexState.targetSettings()).thenReturn(targetSettings);

    // Mock index creation
    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    // Mock document count to be 0 to trigger REINDEXED_SKIPPED_0DOCS path
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(0L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    // Mock task list response - return empty list (no previous tasks)
    org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse taskListResponse =
        mock(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse.class);
    when(taskListResponse.getTasks()).thenReturn(new ArrayList<>());
    when(searchClient.listTasks(
            any(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest.class), any()))
        .thenReturn(taskListResponse);

    // Mock refreshIndex response
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(any(), any(RequestOptions.class))).thenReturn(refreshResponse);

    // Mock settings operations for reindex optimization
    GetSettingsResponse getSettingsResponse = mock(GetSettingsResponse.class);
    when(getSettingsResponse.getSetting(anyString(), eq("index.translog.flush_threshold_size")))
        .thenReturn("512mb");
    when(searchClient.getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(getSettingsResponse);

    AcknowledgedResponse settingsUpdateResponse = mock(AcknowledgedResponse.class);
    when(settingsUpdateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsUpdateResponse);

    // Mock alias operations for final rename
    GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
    when(getAliasesResponse.getAliases()).thenReturn(Map.of());
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(getAliasesResponse);

    AcknowledgedResponse aliasResponse = mock(AcknowledgedResponse.class);
    when(aliasResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(aliasResponse);

    // Execute the reindex
    ReindexResult result = optimizationDisabledIndexBuilder.buildIndex(indexState);

    // Verify the result
    assertEquals(result, ReindexResult.REINDEXED_SKIPPED_0DOCS);

    // Verify that replica settings were NOT modified during reindexing
    // When zone awareness is enabled, the number of replicas should not be set to 0
    verify(searchClient, never())
        .updateIndexSettings(
            argThat(
                request ->
                    request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && // temp index name pattern
                        request.settings().get("index.number_of_replicas") != null
                        && request.settings().get("index.number_of_replicas").equals("0")),
            any(RequestOptions.class));
  }

  /**
   * Exercises the non-zero-docs path (unlike the 0-docs test above which short-circuits before
   * submitReindex). When reindexOptimizationEnabled=false, setReindexOptimalSettings must skip all
   * settings writes AND the cluster-level /_nodes/stats heap query. The latter is the call that
   * fails in reduced-permission deployments (e.g. non-blocking system upgrades) and would otherwise
   * abort the reindex with an uncaught IOException.
   */
  @Test
  void testReindexWithOptimizationDisabled_SkipsNodeStatsAndSettingsWrites() throws Exception {
    when(buildIndicesConfig.isReindexOptimizationEnabled()).thenReturn(false);

    ESIndexBuilder optimizationDisabledIndexBuilder =
        new ESIndexBuilder(
            searchClient,
            elasticSearchConfiguration,
            TEST_ES_STRUCT_PROPS_DISABLED,
            Map.of(),
            gitVersion);

    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresApplyMappings()).thenReturn(true);
    when(indexState.requiresApplySettings()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());
    when(indexState.indexPattern()).thenReturn(null);

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    // Non-zero doc count drives execution through submitReindex -> setReindexOptimalSettings,
    // which is the code path guarded by the flag.
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse taskListResponse =
        mock(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse.class);
    when(taskListResponse.getTasks()).thenReturn(new ArrayList<>());
    when(searchClient.listTasks(
            any(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest.class), any()))
        .thenReturn(taskListResponse);

    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(any(), any(RequestOptions.class))).thenReturn(refreshResponse);

    AcknowledgedResponse settingsUpdateResponse = mock(AcknowledgedResponse.class);
    when(settingsUpdateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsUpdateResponse);

    when(searchClient.submitReindexTask(any(ReindexRequest.class), any())).thenReturn("task1");

    GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
    when(getAliasesResponse.getAliases()).thenReturn(Map.of());
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(getAliasesResponse);

    AcknowledgedResponse aliasResponse = mock(AcknowledgedResponse.class);
    when(aliasResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(aliasResponse);

    ReindexResult result = optimizationDisabledIndexBuilder.buildIndex(indexState);

    // Reindex is still submitted - only the pre-reindex optimizations are skipped.
    assertEquals(result, ReindexResult.REINDEXING);
    verify(searchClient).submitReindexTask(any(ReindexRequest.class), any());

    // Critical: the cluster-level /_nodes/stats heap query must not be issued. This is the
    // call that fails in reduced-permission deployments and was previously uncaught.
    verify(searchClient, never())
        .performLowLevelRequest(
            argThat(req -> req != null && req.getEndpoint().contains("_nodes/stats")));

    // None of the optimization settings should be written to the temp index.
    verify(searchClient, never())
        .updateIndexSettings(
            argThat(
                request ->
                    request != null
                        && request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && "0".equals(request.settings().get("index.number_of_replicas"))),
            any(RequestOptions.class));
    verify(searchClient, never())
        .updateIndexSettings(
            argThat(
                request ->
                    request != null
                        && request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && "-1".equals(request.settings().get("index.refresh_interval"))),
            any(RequestOptions.class));
    verify(searchClient, never())
        .updateIndexSettings(
            argThat(
                request ->
                    request != null
                        && request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && request.settings().get("index.translog.flush_threshold_size") != null),
            any(RequestOptions.class));
  }

  /**
   * Defense-in-depth: even with reindexOptimizationEnabled=true, a failure of the cluster-level
   * /_nodes/stats call (used to pick an optimal translog.flush_threshold_size) must not abort the
   * reindex. The replica/refresh_interval tuning should still be applied, the flush_threshold
   * tuning should be skipped, and the reindex should still be submitted.
   */
  @Test
  void testReindexContinuesWhenNodeStatsFails() throws Exception {
    // Override the @BeforeMethod stub: /_nodes/stats now fails.
    when(searchClient.performLowLevelRequest(
            argThat(req -> req != null && req.getEndpoint().contains("_nodes/stats"))))
        .thenThrow(new IOException("permission denied on cluster:monitor/nodes/stats"));

    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresApplyMappings()).thenReturn(true);
    when(indexState.requiresApplySettings()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());
    when(indexState.indexPattern()).thenReturn(null);

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse taskListResponse =
        mock(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse.class);
    when(taskListResponse.getTasks()).thenReturn(new ArrayList<>());
    when(searchClient.listTasks(
            any(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest.class), any()))
        .thenReturn(taskListResponse);

    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(any(), any(RequestOptions.class))).thenReturn(refreshResponse);

    GetSettingsResponse getSettingsResponse = mock(GetSettingsResponse.class);
    when(getSettingsResponse.getSetting(anyString(), eq("index.translog.flush_threshold_size")))
        .thenReturn("512mb");
    when(searchClient.getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(getSettingsResponse);

    AcknowledgedResponse settingsUpdateResponse = mock(AcknowledgedResponse.class);
    when(settingsUpdateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsUpdateResponse);

    when(searchClient.submitReindexTask(any(ReindexRequest.class), any())).thenReturn("task1");

    GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
    when(getAliasesResponse.getAliases()).thenReturn(Map.of());
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(getAliasesResponse);

    AcknowledgedResponse aliasResponse = mock(AcknowledgedResponse.class);
    when(aliasResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(aliasResponse);

    ReindexResult result = indexBuilder.buildIndex(indexState);

    // Reindex is still submitted - the node-stats failure is swallowed.
    assertEquals(result, ReindexResult.REINDEXING);
    verify(searchClient).submitReindexTask(any(ReindexRequest.class), any());

    // Replica/refresh_interval tuning is still applied (they don't depend on heap stats).
    verify(searchClient)
        .updateIndexSettings(
            argThat(
                request ->
                    request != null
                        && request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && "0".equals(request.settings().get("index.number_of_replicas"))),
            any(RequestOptions.class));
    verify(searchClient)
        .updateIndexSettings(
            argThat(
                request ->
                    request != null
                        && request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && "-1".equals(request.settings().get("index.refresh_interval"))),
            any(RequestOptions.class));

    // Flush threshold optimization should be skipped because the heap query failed.
    verify(searchClient, never())
        .updateIndexSettings(
            argThat(
                request ->
                    request != null
                        && request.indices().length == 1
                        && request.indices()[0].contains(TEST_INDEX_NAME + "_")
                        && request.settings().get("index.translog.flush_threshold_size") != null),
            any(RequestOptions.class));
  }

  @Test
  void testBuildIndex_ReindexUsesConfigBatchSizeAndMaxSlices() throws Exception {
    when(buildIndicesConfig.getReindexBatchSize()).thenReturn(999);
    when(buildIndicesConfig.getReindexMaxSlices()).thenReturn(8);

    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresApplyMappings()).thenReturn(true);
    when(indexState.requiresApplySettings()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());
    when(indexState.indexPattern()).thenReturn(null);

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(createResponse);

    org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse taskListResponse =
        mock(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksResponse.class);
    when(taskListResponse.getTasks()).thenReturn(new ArrayList<>());
    when(searchClient.listTasks(
            any(org.opensearch.action.admin.cluster.node.tasks.list.ListTasksRequest.class), any()))
        .thenReturn(taskListResponse);

    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(searchClient.refreshIndex(any(), any(RequestOptions.class))).thenReturn(refreshResponse);

    GetSettingsResponse getSettingsResponse = mock(GetSettingsResponse.class);
    when(getSettingsResponse.getSetting(anyString(), eq("index.translog.flush_threshold_size")))
        .thenReturn("512mb");
    when(searchClient.getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(getSettingsResponse);

    AcknowledgedResponse settingsUpdateResponse = mock(AcknowledgedResponse.class);
    when(settingsUpdateResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsUpdateResponse);

    when(searchClient.submitReindexTask(any(ReindexRequest.class), any())).thenReturn("task1");

    GetAliasesResponse getAliasesResponse = mock(GetAliasesResponse.class);
    when(getAliasesResponse.getAliases()).thenReturn(Map.of());
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(getAliasesResponse);

    AcknowledgedResponse aliasResponse = mock(AcknowledgedResponse.class);
    when(aliasResponse.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(aliasResponse);

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L, 100L, 100L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    ReindexResult result = indexBuilder.buildIndex(indexState);

    assertEquals(result, ReindexResult.REINDEXING);
    verify(searchClient).submitReindexTask(any(ReindexRequest.class), any());
    verify(buildIndicesConfig).getReindexBatchSize();
    verify(buildIndicesConfig).getReindexMaxSlices();
  }

  // Helper methods
  private Map<String, Object> createTestMappings() {
    return ImmutableMap.of(
        "properties",
        ImmutableMap.of(
            "field1", ImmutableMap.of("type", "text"),
            "field2", ImmutableMap.of("type", "keyword")));
  }

  private Map<String, Object> createTestMappingsWithStructuredProperties(
      Map<String, Object> structuredProps) {
    Map<String, Object> properties = new HashMap<>();
    properties.put("field1", ImmutableMap.of("type", "text"));
    properties.put("field2", ImmutableMap.of("type", "keyword"));

    Map<String, Object> structuredPropertyMapping = new HashMap<>();
    structuredPropertyMapping.put("properties", structuredProps);
    properties.put(STRUCTURED_PROPERTY_MAPPING_FIELD, structuredPropertyMapping);

    return ImmutableMap.of("properties", properties);
  }

  private Map<String, Object> createTestSettings() {
    return ImmutableMap.of(
        "number_of_shards", 1,
        "number_of_replicas", 1);
  }

  @Test
  public void testUpdateIndexSettings_RetriesOnOpenSearchException() throws IOException {
    Settings settings = Settings.builder().put("index.refresh_interval", "60s").build();

    AtomicInteger count = new AtomicInteger();
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            (e) -> {
              if (count.get() == 1) {
                return new AcknowledgedResponse(true);
              }
              count.getAndIncrement();
              throw new OpenSearchException("Circuit breaker");
            });
    // Should succeed on retry
    indexBuilder.updateIndexSettings("test_index", settings);

    // Verify retry happened (called twice: first failed, second succeeded)
    verify(searchClient, times(2))
        .updateIndexSettings(any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  void testWaitForIndexGreenHealth_RetriesOnIOException() throws IOException {
    String indexName = "test-index";

    // Setup GREEN health response for the successful retry attempt
    ClusterHealthResponse healthResponse =
        createMockClusterHealthResponse(ClusterHealthStatus.GREEN, 0);
    Map<String, ClusterIndexHealth> healthMap = new HashMap<>();
    ClusterIndexHealth indexHealth = mock(ClusterIndexHealth.class);
    when(indexHealth.getInitializingShards()).thenReturn(0);
    when(indexHealth.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
    healthMap.put(indexName, indexHealth);
    when(healthResponse.getIndices()).thenReturn(healthMap);

    AtomicInteger count = new AtomicInteger();
    when(searchClient.clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
        .thenAnswer(
            inv -> {
              // Throw IOException on second call (index health check), not first (data node count)
              if (count.getAndIncrement() == 1) {
                throw new IOException("Transient network error");
              }
              return healthResponse;
            });

    indexBuilder.waitForIndexGreenHealth(indexName, 30);

    verify(searchClient, atLeast(1))
        .clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  void testWaitForIndexGreenHealth_AcceptsYellowWithAllPrimariesActive() throws IOException {
    String indexName = "test-index";

    // Setup YELLOW response with all primaries active (1/1)
    ClusterHealthResponse healthResponse =
        createMockClusterHealthResponse(ClusterHealthStatus.YELLOW, 1);
    Map<String, ClusterIndexHealth> healthMap = new HashMap<>();
    ClusterIndexHealth indexHealth = mock(ClusterIndexHealth.class);
    when(indexHealth.getNumberOfShards()).thenReturn(1);
    when(indexHealth.getActivePrimaryShards()).thenReturn(1); // All primaries active ✓
    when(indexHealth.getInitializingShards()).thenReturn(1); // Replicas initializing (OK)
    when(indexHealth.getNumberOfReplicas()).thenReturn(1);
    when(indexHealth.getStatus()).thenReturn(ClusterHealthStatus.YELLOW);
    healthMap.put(indexName, indexHealth);
    when(healthResponse.getIndices()).thenReturn(healthMap);
    when(searchClient.clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(healthResponse);

    // Should NOT throw - YELLOW with all primaries active is acceptable
    // Replicas will sync asynchronously in background
    indexBuilder.waitForIndexGreenHealth(indexName, 30);

    // Verify clusterHealth was called
    verify(searchClient, atLeastOnce())
        .clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT));
  }

  private Map<String, Object> createTestTargetSettings() {
    return ImmutableMap.of(
        "index",
        ImmutableMap.of(
            "number_of_shards", NUM_SHARDS,
            "number_of_replicas", NUM_REPLICAS,
            "refresh_interval", REFRESH_INTERVAL_SECONDS + "s"));
  }

  @Test
  public void testSuccessOnFirstAttemptGreenStatus() throws Exception {
    String indexName = "test-index";
    ClusterHealthResponse healthResponse =
        createMockClusterHealthResponse(ClusterHealthStatus.GREEN, 0);
    Map<String, ClusterIndexHealth> healthMap = new HashMap<>();
    ClusterIndexHealth indexHealth = mock(ClusterIndexHealth.class);
    when(indexHealth.getInitializingShards()).thenReturn(0);
    when(indexHealth.getStatus()).thenReturn(ClusterHealthStatus.GREEN);
    healthMap.put(indexName, indexHealth);
    when(healthResponse.getIndices()).thenReturn(healthMap);
    when(searchClient.clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(healthResponse);

    // Should not throw
    indexBuilder.waitForIndexGreenHealth(indexName, 30);

    // Verify clusterHealth was called exactly once
    verify(searchClient, times(1))
        .clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testFailureOnRedStatusNoRetry() throws Exception {
    String indexName = "test-index";
    ClusterHealthResponse healthResponse =
        createMockClusterHealthResponse(ClusterHealthStatus.RED, 0);

    Map<String, ClusterIndexHealth> healthMap = new HashMap<>();
    ClusterIndexHealth indexHealth = mock(ClusterIndexHealth.class);
    when(indexHealth.getInitializingShards()).thenReturn(3);
    healthMap.put(indexName, indexHealth);
    when(healthResponse.getIndices()).thenReturn(healthMap);
    when(searchClient.clusterHealth(any(), eq(RequestOptions.DEFAULT))).thenReturn(healthResponse);

    // Should throw IOException
    Exception exception = null;
    try {
      indexBuilder.waitForIndexGreenHealth(indexName, 30);
      fail("Expected RuntimeException to be thrown for RED status");
    } catch (ReplicaHealthException e) {
      exception = e;
      assertTrue(
          e.getMessage().contains("Failed to verify replica health for index test-index"),
          "Expected RED status message, got: " + e.getMessage());
    }

    assertNotNull(exception, "Expected ReplicaHealthException");

    // Verify clusterHealth was called at least once (retry logic may try multiple times)
    // But the first response already indicates RED, so we expect few retries
    verify(searchClient, atLeastOnce())
        .clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  public void testFailureWithInitializingShards() throws Exception {
    String indexName = "test-index";
    int initializingShards = 2; // Non-zero initializing shards
    ClusterHealthResponse healthResponse =
        createMockClusterHealthResponse(ClusterHealthStatus.YELLOW, initializingShards);

    Map<String, ClusterIndexHealth> healthMap = new HashMap<>();
    ClusterIndexHealth indexHealth = mock(ClusterIndexHealth.class);
    when(indexHealth.getInitializingShards()).thenReturn(2);
    healthMap.put(indexName, indexHealth);
    when(healthResponse.getIndices()).thenReturn(healthMap);
    when(searchClient.clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(healthResponse);

    // Should throw RuntimeException
    Exception exception = null;
    try {
      indexBuilder.waitForIndexGreenHealth(indexName, 30);
      fail("Expected RuntimeException to be thrown for initializing shards");
    } catch (ReplicaHealthException e) {
      exception = e;
      assertTrue(
          e.getCause().getMessage().contains("initializing shards")
              || e.getCause().getMessage().contains("initializing=2"),
          "Expected initializing shards message, got: " + e.getMessage());
    }

    assertNotNull(exception, "Expected ReplicaHealthException");

    // Verify clusterHealth was called
    verify(searchClient, atLeastOnce())
        .clusterHealth(any(ClusterHealthRequest.class), eq(RequestOptions.DEFAULT));
  }

  private ClusterHealthResponse createMockClusterHealthResponse(
      ClusterHealthStatus status, int initializingShards) {

    ClusterHealthResponse response = mock(ClusterHealthResponse.class);

    // Mock only the methods that are actually called by waitForIndexGreenHealth()
    when(response.getStatus()).thenReturn(status);
    when(response.getInitializingShards()).thenReturn(initializingShards);
    when(response.getClusterName()).thenReturn("test-cluster");
    when(response.getNumberOfNodes()).thenReturn(3);
    when(response.getNumberOfDataNodes()).thenReturn(3);

    return response;
  }

  @Test
  void testGetTaskStatusMultiple_DistinguishesNetworkErrorsFromTaskNotFound() throws IOException {

    GetTaskResponse taskResponse = mock(GetTaskResponse.class);
    when(taskResponse.isCompleted()).thenReturn(false);

    // Mock successful fetch for task1
    when(searchClient.getTask(any(GetTaskRequest.class), any(RequestOptions.class)))
        .thenAnswer(
            (in) -> {
              GetTaskRequest req = in.getArgument(0);
              long taskId = req.getTaskId();
              if (taskId == 1) {
                return Optional.of(mock(GetTaskResponse.class));
              } else if (taskId == 2) {
                throw new IOException("Connection timeout");
              }
              return Optional.empty();
            });
    // Execute: Get status for all three tasks
    ESIndexBuilder.TaskStatusResult result =
        indexBuilder.getTaskStatusMultiple(List.of("node1:1", "node2:2", "node3:3"));

    assertEquals(
        result.getResponses().size(), 1, "Should have exactly 1 successful response (node1:1)");
    assertTrue(result.getResponses().containsKey("node1:1"), "task1 should be in responses map");

    // Case 2: task2 should be in failedTaskIds (network error)
    assertEquals(
        result.getFailedTaskIds().size(),
        1,
        "Should have exactly 1 failed task due to network error (node2:2)");
    assertTrue(
        result.getFailedTaskIds().contains("node2:2"),
        "node2:2 (network error) should be in failedTaskIds, NOT treated as missing");

    // Case 3: task3 should be in neither map/set (legitimately not found)
    assertFalse(
        result.getResponses().containsKey("task3"), "task3 (not found) should NOT be in responses");
    assertFalse(
        result.getFailedTaskIds().contains("task3"),
        "task3 (not found) should NOT be in failedTaskIds");
  }

  // --- Incremental reindex tests ---

  @Test
  void testExtractTargetShards() {
    ReindexConfig config = mock(ReindexConfig.class);
    when(config.targetSettings())
        .thenReturn(ImmutableMap.of("index", ImmutableMap.of("number_of_shards", 3)));
    assertEquals(ESIndexBuilder.extractTargetShards(config), 3);
  }

  @Test
  void testExtractTargetShardsFromString() {
    ReindexConfig config = mock(ReindexConfig.class);
    when(config.targetSettings())
        .thenReturn(ImmutableMap.of("index", ImmutableMap.of("number_of_shards", "5")));
    assertEquals(ESIndexBuilder.extractTargetShards(config), 5);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  void testExtractTargetShardsMissingThrows() {
    ReindexConfig config = mock(ReindexConfig.class);
    when(config.targetSettings()).thenReturn(ImmutableMap.of("index", ImmutableMap.of()));
    ESIndexBuilder.extractTargetShards(config);
  }

  @Test
  void testGetIncrementalNextIndexNameSanitizesVersion() {
    String result = ESIndexBuilder.getIncrementalNextIndexName("datasetindex_v2", "1.2.3-4", 1000L);
    assertEquals(result, "datasetindex_v2_1_2_3-4_1000");
  }

  @Test
  void testGetIncrementalNextIndexName() {
    String result =
        ESIndexBuilder.getIncrementalNextIndexName("datasetindex_v2", "0.13.1-0", 1679000000000L);
    assertEquals(result, "datasetindex_v2_0_13_1-0_1679000000000");
  }

  @Test
  void testBuildIndexIncrementalCreatesNextIndex() throws Throwable {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    // Mock createIndex (the next index doesn't exist yet)
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(new CreateIndexResponse(true, true, "test_index_next_123"));

    // Mock getCount returning 0 (empty source index)
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(0L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    ESIndexBuilder.IncrementalReindexResult result =
        indexBuilder.buildIndexIncremental(indexState, "0.13.1-0");

    assertTrue(result.nextIndexName().startsWith(TEST_INDEX_NAME + "_0_13_1-0_"));
    assertTrue(result.reindexStartTime() > 0);
    assertTrue(result.skippedEmpty());
    // Should have created the index
    verify(searchClient).createIndex(any(CreateIndexRequest.class), any(RequestOptions.class));
    // Should NOT have submitted a reindex (0 docs)
    verify(searchClient, never())
        .submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class));
  }

  /**
   * Regression test for the concrete-index→alias migration bug. When the source index is a plain
   * concrete index (no alias), renameReindexedIndices must use REMOVE_INDEX (delete the index) so
   * that an alias of the same name can be created. Using REMOVE (remove alias relationship) causes
   * OpenSearch to reject the request with invalid_alias_name_exception.
   */
  @Test
  void testRenameReindexedIndices_ConcreteIndexUsesRemoveIndex() throws Exception {
    String originalName = "tagindex_v2";
    String newName = "tagindex_v2_v0_3_17_1000";

    // Empty aliases response → originalName is a concrete index, not an alias
    GetAliasesResponse emptyAliases = mock(GetAliasesResponse.class);
    when(emptyAliases.getAliases()).thenReturn(Map.of());
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(emptyAliases);

    AcknowledgedResponse ack = mock(AcknowledgedResponse.class);
    when(ack.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(ack);

    ESIndexBuilder.renameReindexedIndices(
        searchClient, originalName, null, newName, false, RequestOptions.DEFAULT);

    ArgumentCaptor<IndicesAliasesRequest> captor =
        ArgumentCaptor.forClass(IndicesAliasesRequest.class);
    verify(searchClient).updateIndexAliases(captor.capture(), any(RequestOptions.class));

    List<org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions> actions =
        captor.getValue().getAliasActions();
    // Exactly one REMOVE_INDEX action (deletes the concrete index) + one ADD action (creates alias)
    long removeIndexCount =
        actions.stream()
            .filter(
                a ->
                    a.actionType()
                        == org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
                            .AliasActions.Type.REMOVE_INDEX)
            .count();
    assertEquals(
        removeIndexCount, 1L, "Expected exactly one REMOVE_INDEX action for concrete index");
  }

  @Test
  void testRenameReindexedIndices_AliasedIndexUsesRemoveAlias() throws Exception {
    String aliasName = "tagindex_v2";
    String oldBacking = "tagindex_v2_v0_3_16_999";
    String newBacking = "tagindex_v2_v0_3_17_1000";

    // Non-empty aliases response → aliasName already points to a backing index
    GetAliasesResponse existingAlias = mock(GetAliasesResponse.class);
    when(existingAlias.getAliases()).thenReturn(Map.of(oldBacking, new HashSet<>()));
    when(searchClient.getIndexAliases(any(GetAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(existingAlias);

    AcknowledgedResponse ack = mock(AcknowledgedResponse.class);
    when(ack.isAcknowledged()).thenReturn(true);
    when(searchClient.updateIndexAliases(
            any(IndicesAliasesRequest.class), any(RequestOptions.class)))
        .thenReturn(ack);

    ESIndexBuilder.renameReindexedIndices(
        searchClient, aliasName, null, newBacking, false, RequestOptions.DEFAULT);

    ArgumentCaptor<IndicesAliasesRequest> captor =
        ArgumentCaptor.forClass(IndicesAliasesRequest.class);
    verify(searchClient).updateIndexAliases(captor.capture(), any(RequestOptions.class));

    List<org.opensearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions> actions =
        captor.getValue().getAliasActions();
    // Should use REMOVE (remove alias relationship), NOT REMOVE_INDEX (would delete backing data)
    long removeAliasCount =
        actions.stream()
            .filter(
                a ->
                    a.actionType()
                        == org.opensearch.action.admin.indices.alias.IndicesAliasesRequest
                            .AliasActions.Type.REMOVE)
            .count();
    assertEquals(removeAliasCount, 1L, "Expected exactly one REMOVE action for aliased index");
  }

  @Test
  void testBuildIndexIncrementalSubmitsReindex() throws Throwable {
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    when(indexState.exists()).thenReturn(true);
    when(indexState.requiresReindex()).thenReturn(true);
    when(indexState.targetMappings()).thenReturn(createTestMappings());
    when(indexState.targetSettings()).thenReturn(createTestTargetSettings());

    // Mock createIndex
    when(searchClient.indexExists(any(GetIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(false);
    when(searchClient.createIndex(any(CreateIndexRequest.class), any(RequestOptions.class)))
        .thenReturn(new CreateIndexResponse(true, true, "test_index_next_123"));

    // Mock getCount returning non-zero (has docs to reindex)
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(1000L);
    when(searchClient.count(any(CountRequest.class), any(RequestOptions.class)))
        .thenReturn(countResponse);

    // Mock refresh
    when(searchClient.refreshIndex(any(), any())).thenReturn(null);

    // Mock settings for reindex optimization
    GetSettingsResponse settingsResponse = mock(GetSettingsResponse.class);
    when(settingsResponse.getSetting(anyString(), anyString())).thenReturn("512mb");
    when(searchClient.getIndexSettings(any(GetSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(settingsResponse);

    // Mock settings update
    when(searchClient.updateIndexSettings(
            any(UpdateSettingsRequest.class), any(RequestOptions.class)))
        .thenReturn(mock(AcknowledgedResponse.class));

    // Mock submit reindex task
    when(searchClient.submitReindexTask(any(ReindexRequest.class), any(RequestOptions.class)))
        .thenReturn("node1:12345");

    ESIndexBuilder.IncrementalReindexResult result =
        indexBuilder.buildIndexIncremental(indexState, "0.13.1-0");

    assertTrue(result.nextIndexName().startsWith(TEST_INDEX_NAME + "_0_13_1-0_"));
    assertTrue(result.reindexStartTime() > 0);
    Assert.assertFalse(result.skippedEmpty());
  }
}
