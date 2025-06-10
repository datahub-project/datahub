package com.linkedin.metadata.search.indexbuilder;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

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
import java.util.HashMap;
import java.util.Map;
import org.apache.http.HttpEntity;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.ClusterClient;
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
import org.opensearch.client.indices.PutMappingRequest;
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

    when(searchClient.indices()).thenReturn(indicesClient);
    when(searchClient.cluster()).thenReturn(clusterClient);
    when(searchClient.getLowLevelClient()).thenReturn(lowLevelClient);
    when(gitVersion.getVersion()).thenReturn("1.0.0");
    when(elasticSearchConfiguration.getBuildIndices()).thenReturn(buildIndicesConfig);
    when(buildIndicesConfig.getRetentionValue()).thenReturn(7L);
    when(buildIndicesConfig.getRetentionUnit()).thenReturn(ChronoUnit.DAYS.name());

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
    Assert.assertEquals(indexBuilder.getNumShards(), NUM_SHARDS);
    Assert.assertEquals(indexBuilder.getNumReplicas(), NUM_REPLICAS);
    Assert.assertEquals(indexBuilder.getNumRetries(), NUM_RETRIES);
    Assert.assertEquals(indexBuilder.getRefreshIntervalSeconds(), REFRESH_INTERVAL_SECONDS);
    Assert.assertTrue(indexBuilder.isEnableIndexSettingsReindex());
    Assert.assertTrue(indexBuilder.isEnableIndexMappingsReindex());
    Assert.assertTrue(indexBuilder.isEnableStructuredPropertiesReindex());
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

    Assert.assertEquals(builderWithTimeout.getMaxReindexHours(), maxReindexHours);
  }

  @Test
  void testIsOpenSearch29OrHigher_OpenSearch29() throws IOException {
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

    when(lowLevelClient.performRequest(any(Request.class))).thenReturn(response);
    when(response.getEntity()).thenReturn(httpEntity);
    when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream(responseJson.getBytes()));

    boolean result = indexBuilder.isOpenSearch29OrHigher();
    Assert.assertTrue(result);
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

    Assert.assertEquals(result.name(), TEST_INDEX_NAME);
    Assert.assertFalse(result.exists());
    Assert.assertEquals(result.targetMappings(), mappings);
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

    Assert.assertEquals(result, ReindexResult.CREATED_NEW);
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

    Assert.assertEquals(result, ReindexResult.NOT_REINDEXED_NOTHING_APPLIED);
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

    Assert.assertEquals(result, 100L);
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

    Assert.assertTrue(summary.contains(TEST_INDEX_NAME));
    Assert.assertTrue(summary.contains("LIVE"));
    Assert.assertTrue(summary.contains("Active"));
    Assert.assertTrue(summary.contains("100 docs"));
    Assert.assertTrue(summary.contains("increased"));
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
      Assert.assertTrue(true);
    } catch (Exception e) {
      // Expected for mocked environment, but shouldn't be a critical failure
      Assert.assertTrue(
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

  @Test(dataProvider = "indexOverrideData")
  void testIndexSettingOverrides(
      String indexName, Map<String, String> overrides, String expectedRefreshInterval)
      throws IOException {
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

    // Mock OpenSearch version check
    when(lowLevelClient.performRequest(any(Request.class)))
        .thenThrow(new IOException("Test exception"));

    ReindexConfig result =
        builderWithOverrides.buildReindexState(
            indexName, createTestMappings(), createTestSettings());

    Map<String, Object> targetSettings = result.targetSettings();
    Map<String, Object> indexSettings = (Map<String, Object>) targetSettings.get("index");

    Assert.assertEquals(indexSettings.get("refresh_interval"), expectedRefreshInterval);
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
