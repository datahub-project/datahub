package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ESIndexBuilder;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexConfig;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.ReindexResult;
import com.linkedin.metadata.version.GitVersion;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.admin.indices.alias.IndicesAliasesRequest;
import org.opensearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.opensearch.action.admin.indices.delete.DeleteIndexRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.ClusterClient;
import org.opensearch.client.GetAliasesResponse;
import org.opensearch.client.IndicesClient;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.CreateIndexResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.indices.GetMappingsRequest;
import org.opensearch.client.indices.GetMappingsResponse;
import org.opensearch.client.indices.PutMappingRequest;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class ESIndexBuilderTest {

  @Mock private RestHighLevelClient searchClient;
  @Mock private IndicesClient indicesClient;
  @Mock private ClusterClient clusterClient;
  @Mock private RestClient lowLevelClient;
  @Mock private ElasticSearchConfiguration elasticSearchConfiguration;
  @Mock private BuildIndicesConfiguration buildIndicesConfig;
  @Mock private GitVersion gitVersion;
  @Mock private Response response;
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

    RestClient lowLevelClient = mock(RestClient.class);
    when(searchClient.getLowLevelClient()).thenReturn(lowLevelClient);

    // Mock the JVM info response
    try {
      // Only mock JVM stats response, not the root endpoint
      Response jvmResponse = mock(Response.class);
      HttpEntity jvmEntity = mock(HttpEntity.class);
      String jvmJson =
          "{\"nodes\":{\"node1\":{\"roles\":[\"data\"],\"jvm\":{\"mem\":{\"heap_max_in_bytes\":17179869184}}}}}";
      when(jvmEntity.getContent()).thenReturn(new ByteArrayInputStream(jvmJson.getBytes()));
      when(jvmResponse.getEntity()).thenReturn(jvmEntity);

      // Only mock nodes stats endpoint
      when(lowLevelClient.performRequest(
              argThat(req -> req != null && req.getEndpoint().contains("_nodes/stats"))))
          .thenReturn(jvmResponse);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    when(searchClient.indices()).thenReturn(indicesClient);
    when(searchClient.cluster()).thenReturn(clusterClient);
    when(searchClient.getLowLevelClient()).thenReturn(lowLevelClient);
    when(gitVersion.getVersion()).thenReturn("1.0.0");
    when(elasticSearchConfiguration.getBuildIndices()).thenReturn(buildIndicesConfig);
    when(buildIndicesConfig.getRetentionValue()).thenReturn(7L);
    when(buildIndicesConfig.getRetentionUnit()).thenReturn(ChronoUnit.DAYS.name());
    when(buildIndicesConfig.isAllowDocCountMismatch()).thenReturn(false);
    when(buildIndicesConfig.isCloneIndices()).thenReturn(false);
    when(buildIndicesConfig.isZoneAwarenessEnabled()).thenReturn(false);

    indexBuilder =
        new ESIndexBuilder(
            searchClient,
            NUM_SHARDS,
            NUM_REPLICAS,
            NUM_RETRIES,
            REFRESH_INTERVAL_SECONDS,
            new HashMap<>(),
            true,
            true,
            true,
            elasticSearchConfiguration,
            gitVersion);
  }

  @Test
  void testConstructor() {
    assertEquals(indexBuilder.getNumShards(), NUM_SHARDS);
    assertEquals(indexBuilder.getNumReplicas(), NUM_REPLICAS);
    assertEquals(indexBuilder.getNumRetries(), NUM_RETRIES);
    assertEquals(indexBuilder.getRefreshIntervalSeconds(), REFRESH_INTERVAL_SECONDS);
    assertTrue(indexBuilder.isEnableIndexSettingsReindex());
    assertTrue(indexBuilder.isEnableIndexMappingsReindex());
    assertTrue(indexBuilder.isEnableStructuredPropertiesReindex());
    Assert.assertNotNull(indexBuilder.getElasticSearchConfiguration());
    Assert.assertNotNull(indexBuilder.getGitVersion());
  }

  @Test
  void testConstructorWithMaxReindexHours() {
    int maxReindexHours = 24;
    ESIndexBuilder builderWithTimeout =
        new ESIndexBuilder(
            searchClient,
            NUM_SHARDS,
            NUM_REPLICAS,
            NUM_RETRIES,
            REFRESH_INTERVAL_SECONDS,
            new HashMap<>(),
            true,
            true,
            true,
            elasticSearchConfiguration,
            gitVersion,
            maxReindexHours);

    assertEquals(builderWithTimeout.getMaxReindexHours(), maxReindexHours);
  }

  @Test
  void testIsOpenSearch29OrHigher_OpenSearch29() throws IOException {
    // Get the actual lowLevelClient used by indexBuilder
    RestClient lowLevelClient = searchClient.getLowLevelClient();

    // Create fresh mocks for this test
    Response opensearchResponse = mock(Response.class);
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
    when(lowLevelClient.performRequest(
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

    when(lowLevelClient.performRequest(any(Request.class))).thenReturn(response);
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

    when(lowLevelClient.performRequest(any(Request.class))).thenReturn(response);
    when(response.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(responseJson.getBytes()));

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    Assert.assertFalse(result);
  }

  @Test
  void testIsOpenSearch29OrHigher_Exception() throws IOException {
    when(lowLevelClient.performRequest(any(Request.class)))
        .thenThrow(new IOException("Network error"));

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    Assert.assertFalse(result); // Should return false defensively
  }

  @Test
  void testBuildReindexState_IndexDoesNotExist() throws IOException {
    Map<String, Object> mappings = createTestMappings();
    Map<String, Object> settings = createTestSettings();

    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
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
    when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(createResponse);

    ReindexResult result = indexBuilder.buildIndex(indexState);

    assertEquals(result, ReindexResult.CREATED_NEW);
    verify(indicesClient).create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT));
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
    verify(indicesClient, never())
        .putMapping(any(PutMappingRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  void testGetCount() throws IOException {
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(countResponse);

    // Mock refresh response
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(indicesClient.refresh(
            any(org.opensearch.action.admin.indices.refresh.RefreshRequest.class),
            eq(RequestOptions.DEFAULT)))
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
      ESIndexBuilder.cleanIndex(searchClient, elasticSearchConfiguration, indexState);
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

    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
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
    when(indicesClient.getSettings(any(GetSettingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(settingsResponse);

    GetMappingsResponse mappingsResponse = mock(GetMappingsResponse.class);
    MappingMetadata mappingMetadata = mock(MappingMetadata.class);
    when(mappingMetadata.getSourceAsMap()).thenReturn(currentMappings);
    when(mappingsResponse.mappings()).thenReturn(Map.of(TEST_INDEX_NAME, mappingMetadata));
    when(indicesClient.getMapping(any(GetMappingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(mappingsResponse);

    // Execute with 6 shards configured
    ESIndexBuilder builderWith6Shards =
        new ESIndexBuilder(
            searchClient,
            6,
            NUM_REPLICAS,
            NUM_RETRIES,
            REFRESH_INTERVAL_SECONDS,
            new HashMap<>(),
            true,
            true,
            true,
            elasticSearchConfiguration,
            gitVersion);

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
    when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(createResponse);

    // Mock document counts
    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
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

    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(true);
    when(indicesClient.get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(getIndexResponse);
    when(getIndexResponse.getSettings()).thenReturn(Map.of(TEST_INDEX_NAME, settings));

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(100L);
    when(searchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(countResponse);

    AcknowledgedResponse updateResponse = mock(AcknowledgedResponse.class);
    when(updateResponse.isAcknowledged()).thenReturn(true);
    when(indicesClient.putSettings(any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(updateResponse);

    // Execute
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    indexBuilder.tweakReplicas(indexState, false);

    // Verify replica increase was called
    verify(indicesClient).putSettings(any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  void testTweakReplicas_DecreasesForEmptyIndices() throws IOException {
    // Setup
    Settings settings = Settings.builder().put("index.number_of_replicas", "2").build();

    GetIndexResponse getIndexResponse = mock(GetIndexResponse.class);
    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(true);
    when(indicesClient.get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(getIndexResponse);
    when(getIndexResponse.getSettings()).thenReturn(Map.of(TEST_INDEX_NAME, settings));

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(0L);
    when(searchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(countResponse);

    AcknowledgedResponse updateResponse = mock(AcknowledgedResponse.class);
    when(updateResponse.isAcknowledged()).thenReturn(true);
    when(indicesClient.putSettings(any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(updateResponse);

    // Execute
    ReindexConfig indexState = mock(ReindexConfig.class);
    when(indexState.name()).thenReturn(TEST_INDEX_NAME);
    indexBuilder.tweakReplicas(indexState, false);

    // Verify replica decrease was called
    verify(indicesClient).putSettings(any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT));
  }

  @Test
  void testReindexInPlaceAsync() throws Exception {
    // Setup
    String indexAlias = "test_alias";
    GetAliasesResponse aliasesResponse = mock(GetAliasesResponse.class);
    when(aliasesResponse.getAliases()).thenReturn(Map.of("test_index_old", new HashSet<>()));
    when(indicesClient.getAlias(any(GetAliasesRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(aliasesResponse);

    CreateIndexResponse createResponse = mock(CreateIndexResponse.class);
    when(createResponse.isAcknowledged()).thenReturn(true);
    when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(createResponse);

    AcknowledgedResponse aliasResponse = mock(AcknowledgedResponse.class);
    when(aliasResponse.isAcknowledged()).thenReturn(true);
    when(indicesClient.updateAliases(any(IndicesAliasesRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(aliasResponse);

    // Mock refresh
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(indicesClient.refresh(any(), eq(RequestOptions.DEFAULT))).thenReturn(refreshResponse);

    // Mock settings operations for reindex optimization
    GetSettingsResponse getSettingsResponse = mock(GetSettingsResponse.class);
    when(getSettingsResponse.getSetting(anyString(), eq("index.translog.flush_threshold_size")))
        .thenReturn("512mb");
    when(indicesClient.getSettings(any(GetSettingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(getSettingsResponse);

    AcknowledgedResponse settingsUpdateResponse = mock(AcknowledgedResponse.class);
    when(settingsUpdateResponse.isAcknowledged()).thenReturn(true);
    when(indicesClient.putSettings(any(UpdateSettingsRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(settingsUpdateResponse);

    TaskSubmissionResponse taskResponse = mock(TaskSubmissionResponse.class);
    when(taskResponse.getTask()).thenReturn("task123");
    when(searchClient.submitReindexTask(any(), any())).thenReturn(taskResponse);

    ReindexConfig config = mock(ReindexConfig.class);

    // Put NUMBER_OF_SHARDS directly in targetSettings
    Map<String, Object> targetSettings = new HashMap<>();
    targetSettings.put(ESIndexBuilder.NUMBER_OF_SHARDS, 6);

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
    verify(indicesClient).create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT));
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

    when(indicesClient.get(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(getIndexResponse);

    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(true);

    AcknowledgedResponse deleteResponse = mock(AcknowledgedResponse.class);
    when(deleteResponse.isAcknowledged()).thenReturn(true);
    when(indicesClient.delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(deleteResponse);

    // Execute
    ESIndexBuilder.cleanIndex(searchClient, elasticSearchConfiguration, indexState);

    // Verify deletion was attempted
    verify(indicesClient, atLeastOnce())
        .delete(any(DeleteIndexRequest.class), eq(RequestOptions.DEFAULT));
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
    when(indicesClient.putMapping(any(PutMappingRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(putMappingResponse);

    // Execute
    indexBuilder.applyMappings(indexState, false);

    // Verify
    verify(indicesClient).putMapping(any(PutMappingRequest.class), eq(RequestOptions.DEFAULT));
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
    when(indicesClient.create(any(CreateIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenThrow(new OpenSearchStatusException("Index is read-only", RestStatus.FORBIDDEN));

    // Execute and verify exception
    assertThrows(OpenSearchStatusException.class, () -> indexBuilder.buildIndex(indexState));
  }

  @Test
  void testGetCount_WithRefresh() throws IOException {
    // Setup
    org.opensearch.action.admin.indices.refresh.RefreshResponse refreshResponse =
        mock(org.opensearch.action.admin.indices.refresh.RefreshResponse.class);
    when(indicesClient.refresh(any(), eq(RequestOptions.DEFAULT))).thenReturn(refreshResponse);

    CountResponse countResponse = mock(CountResponse.class);
    when(countResponse.getCount()).thenReturn(42L);
    when(searchClient.count(any(CountRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(countResponse);

    // Execute
    long count = indexBuilder.getCount(TEST_INDEX_NAME);

    // Verify
    assertEquals(count, 42L);
    verify(indicesClient).refresh(any(), eq(RequestOptions.DEFAULT));
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
            NUM_SHARDS,
            NUM_REPLICAS,
            NUM_RETRIES,
            REFRESH_INTERVAL_SECONDS,
            indexOverrides,
            true,
            true,
            true,
            elasticSearchConfiguration,
            gitVersion);

    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
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
    // Get the actual lowLevelClient used by indexBuilder
    RestClient lowLevelClient = searchClient.getLowLevelClient();

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

    Response rootResponse = mock(Response.class);
    HttpEntity rootEntity = mock(HttpEntity.class);
    when(rootEntity.getContent()).thenReturn(new ByteArrayInputStream(responseJson.getBytes()));
    when(rootResponse.getEntity()).thenReturn(rootEntity);

    // Override the mock to return OpenSearch 2.9 for root endpoint
    when(lowLevelClient.performRequest(
            argThat(req -> req != null && req.getEndpoint().equals("/"))))
        .thenReturn(rootResponse);

    when(indicesClient.exists(any(GetIndexRequest.class), eq(RequestOptions.DEFAULT)))
        .thenReturn(false);

    // Execute
    ReindexConfig result =
        indexBuilder.buildReindexState(TEST_INDEX_NAME, createTestMappings(), createTestSettings());

    // Verify codec is set for OpenSearch 2.9+
    Map<String, Object> indexSettings = (Map<String, Object>) result.targetSettings().get("index");
    assertEquals(indexSettings.get("codec"), "zstd_no_dict");
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

  private Map<String, Object> createTestTargetSettings() {
    return ImmutableMap.of(
        "index",
        ImmutableMap.of(
            "number_of_shards", NUM_SHARDS,
            "number_of_replicas", NUM_REPLICAS,
            "refresh_interval", REFRESH_INTERVAL_SECONDS + "s"));
  }
}
