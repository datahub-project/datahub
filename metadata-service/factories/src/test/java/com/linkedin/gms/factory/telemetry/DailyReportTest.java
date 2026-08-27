package com.linkedin.gms.factory.telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.datahub.context.OperationFingerprint;
import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.util.List;
import java.util.Map;
import org.json.JSONObject;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.search.SearchHits;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Unit tests for {@link DailyReport}.
 *
 * <p>These tests focus on the anonymization logic and count methods. The actual telemetry sending
 * is not tested as it requires external services (Mixpanel).
 */
public class DailyReportTest {

  /** Size of DailyReport.REPORTING_ENTITY_TYPES. */
  private static final int REPORTED_ENTITY_TYPE_COUNT = 19;

  private OperationContext mockOperationContext;
  private SearchClientShim<?> mockElasticClient;
  private ConfigurationProvider mockConfigurationProvider;
  private EntityService<?> mockEntityService;
  private GitVersion mockGitVersion;
  private SearchContext mockSearchContext;
  private IndexConvention mockIndexConvention;

  @BeforeMethod
  public void setUp() {
    mockOperationContext = mock(OperationContext.class);
    mockElasticClient = mock(SearchClientShim.class);
    mockConfigurationProvider = mock(ConfigurationProvider.class);
    mockEntityService = mock(EntityService.class);
    mockGitVersion = mock(GitVersion.class);
    mockSearchContext = mock(SearchContext.class);
    mockIndexConvention = mock(IndexConvention.class);

    // Set up the operation context chain
    when(mockOperationContext.getSearchContext()).thenReturn(mockSearchContext);
    when(mockSearchContext.getIndexConvention()).thenReturn(mockIndexConvention);
    // A distinct index per entity name, mirroring the real convention. AnalyticsService
    // de-duplicates the batch's target indices, so stubbing one shared name for every type would
    // collapse the batch to a single index and hide whether it spans all reported types.
    when(mockIndexConvention.getEntityIndexName(any(OperationFingerprint.class), anyString()))
        .thenAnswer(invocation -> invocation.getArgument(1) + "index_v2");
    when(mockGitVersion.getVersion()).thenReturn("test-version");
  }

  /**
   * Data provider for testing anonymizeCount with various inputs.
   *
   * @return test cases as [input, expectedOutput]
   */
  @DataProvider(name = "anonymizeCountTestCases")
  public Object[][] anonymizeCountTestCases() {
    return new Object[][] {
      // Edge cases
      {0, 0},
      {-1, 0},
      {-100, 0},
      // Powers of 2 should stay the same
      {1, 1},
      {2, 2},
      {4, 4},
      {8, 8},
      {16, 16},
      {32, 32},
      {64, 64},
      {128, 128},
      {256, 256},
      {512, 512},
      {1024, 1024},
      // Non-powers of 2 should round down to nearest power of 2
      {3, 2},
      {5, 4},
      {6, 4},
      {7, 4},
      {9, 8},
      {10, 8},
      {15, 8},
      {17, 16},
      {31, 16},
      {33, 32},
      {63, 32},
      {65, 64},
      {100, 64},
      {127, 64},
      {129, 128},
      {200, 128},
      {255, 128},
      {257, 256},
      {500, 256},
      {511, 256},
      {513, 512},
      {1000, 512},
      {1023, 512},
      {1025, 1024},
      // Larger numbers
      {5000, 4096},
      {10000, 8192},
      {100000, 65536},
      {1000000, 524288},
    };
  }

  @Test(dataProvider = "anonymizeCountTestCases")
  public void testAnonymizeCount(int input, int expectedOutput) throws Exception {
    // Create DailyReport instance - constructor will fail to init Mixpanel but that's OK
    // We just need to test the anonymizeCount method
    DailyReport dailyReport = createDailyReportForTesting();

    int result = dailyReport.anonymizeCount(input);

    assertEquals(
        result,
        expectedOutput,
        String.format(
            "anonymizeCount(%d) should return %d but got %d", input, expectedOutput, result));
  }

  @Test
  public void testAnonymizeCountPreservesOrderOfMagnitude() throws Exception {
    DailyReport dailyReport = createDailyReportForTesting();

    // Test that anonymized values are always within factor of 2 of original
    int[] testValues = {1, 10, 50, 100, 500, 1000, 5000, 10000};
    for (int value : testValues) {
      int anonymized = dailyReport.anonymizeCount(value);
      assertTrue(
          anonymized <= value,
          String.format("Anonymized value %d should be <= original %d", anonymized, value));
      assertTrue(
          anonymized > value / 2,
          String.format(
              "Anonymized value %d should be > half of original %d (got %d)",
              anonymized, value, value / 2));
    }
  }

  @Test
  public void testAnonymizeCountResultsArePowersOfTwo() throws Exception {
    DailyReport dailyReport = createDailyReportForTesting();

    // Test that all results are powers of 2
    for (int i = 1; i <= 10000; i += 37) { // Sample various values
      int result = dailyReport.anonymizeCount(i);
      if (result > 0) {
        assertTrue(
            isPowerOfTwo(result),
            String.format("anonymizeCount(%d) returned %d which is not a power of 2", i, result));
      }
    }
  }

  /** Helper method to check if a number is a power of 2. */
  private boolean isPowerOfTwo(int n) {
    return n > 0 && (n & (n - 1)) == 0;
  }

  /**
   * Creates a DailyReport instance for testing. The constructor may log warnings about Mixpanel
   * initialization failing, but that's expected in tests.
   */
  private DailyReport createDailyReportForTesting() {
    return new DailyReport(
        mockOperationContext,
        mockElasticClient,
        mockConfigurationProvider,
        mockEntityService,
        mockGitVersion);
  }

  @Test
  public void testGetTotalUserCountHandlesSearchError() throws Exception {
    // Set up mock to throw exception
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenThrow(new RuntimeException("Search failed"));

    DailyReport dailyReport = createDailyReportForTesting();

    // Use reflection to call the private method
    java.lang.reflect.Method getTotalUserCountMethod =
        DailyReport.class.getDeclaredMethod("getTotalUserCount");
    getTotalUserCountMethod.setAccessible(true);

    int result = (int) getTotalUserCountMethod.invoke(dailyReport);

    // Should return 0 when search fails
    assertEquals(result, 0, "getTotalUserCount should return 0 when search fails");
  }

  @Test
  public void testGetServiceAccountCountHandlesSearchError() throws Exception {
    // Set up mock to throw exception
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenThrow(new RuntimeException("Search failed"));

    DailyReport dailyReport = createDailyReportForTesting();

    // Use reflection to call the private method
    java.lang.reflect.Method getServiceAccountCountMethod =
        DailyReport.class.getDeclaredMethod("getServiceAccountCount");
    getServiceAccountCountMethod.setAccessible(true);

    int result = (int) getServiceAccountCountMethod.invoke(dailyReport);

    // Should return 0 when search fails
    assertEquals(result, 0, "getServiceAccountCount should return 0 when search fails");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetTotalUserCountReturnsCorrectCount() throws Exception {
    // Set up mock search response
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    org.apache.lucene.search.TotalHits mockTotalHits =
        new org.apache.lucene.search.TotalHits(
            42, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO);

    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockSearchHits.getTotalHits()).thenReturn(mockTotalHits);
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);

    DailyReport dailyReport = createDailyReportForTesting();

    // Use reflection to call the private method
    java.lang.reflect.Method getTotalUserCountMethod =
        DailyReport.class.getDeclaredMethod("getTotalUserCount");
    getTotalUserCountMethod.setAccessible(true);

    int result = (int) getTotalUserCountMethod.invoke(dailyReport);

    assertEquals(result, 42, "getTotalUserCount should return the total hits count");
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testGetServiceAccountCountReturnsCorrectCount() throws Exception {
    // Set up mock search response
    SearchResponse mockSearchResponse = mock(SearchResponse.class);
    SearchHits mockSearchHits = mock(SearchHits.class);
    org.apache.lucene.search.TotalHits mockTotalHits =
        new org.apache.lucene.search.TotalHits(
            5, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO);

    when(mockSearchResponse.getHits()).thenReturn(mockSearchHits);
    when(mockSearchHits.getTotalHits()).thenReturn(mockTotalHits);
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);

    DailyReport dailyReport = createDailyReportForTesting();

    // Use reflection to call the private method
    java.lang.reflect.Method getServiceAccountCountMethod =
        DailyReport.class.getDeclaredMethod("getServiceAccountCount");
    getServiceAccountCountMethod.setAccessible(true);

    int result = (int) getServiceAccountCountMethod.invoke(dailyReport);

    assertEquals(result, 5, "getServiceAccountCount should return the total hits count");
  }

  /**
   * Data provider for testing anonymizeToBucket with various inputs.
   *
   * @return test cases as [input, expectedOutput]
   */
  @DataProvider(name = "anonymizeToBucketTestCases")
  public Object[][] anonymizeToBucketTestCases() {
    return new Object[][] {
      // Zero
      {0, "0"},
      // 0-10 bucket
      {1, "0-10"},
      {5, "0-10"},
      {10, "0-10"},
      // 10-100 bucket
      {11, "10-100"},
      {50, "10-100"},
      {100, "10-100"},
      // 100-1K bucket
      {101, "100-1K"},
      {500, "100-1K"},
      {1000, "100-1K"},
      // 1K-10K bucket
      {1001, "1K-10K"},
      {5000, "1K-10K"},
      {10000, "1K-10K"},
      // 10K-100K bucket
      {10001, "10K-100K"},
      {50000, "10K-100K"},
      {100000, "10K-100K"},
      // 100K-1M bucket
      {100001, "100K-1M"},
      {500000, "100K-1M"},
      {1000000, "100K-1M"},
      // 1M+ bucket
      {1000001, "1M+"},
      {5000000, "1M+"},
      {10000000, "1M+"},
    };
  }

  @Test(dataProvider = "anonymizeToBucketTestCases")
  public void testAnonymizeToBucket(int input, String expectedOutput) throws Exception {
    DailyReport dailyReport = createDailyReportForTesting();

    // Use reflection to call the private method
    java.lang.reflect.Method anonymizeToBucketMethod =
        DailyReport.class.getDeclaredMethod("anonymizeToBucket", int.class);
    anonymizeToBucketMethod.setAccessible(true);

    String result = (String) anonymizeToBucketMethod.invoke(dailyReport, input);

    assertEquals(
        result,
        expectedOutput,
        String.format(
            "anonymizeToBucket(%d) should return \"%s\" but got \"%s\"",
            input, expectedOutput, result));
  }

  @DataProvider(name = "extractPlatformNameTestCases")
  public Object[][] extractPlatformNameTestCases() {
    return new Object[][] {
      // Valid URNs should return just the platform name
      {"urn:li:dataPlatform:snowflake", "snowflake"},
      {"urn:li:dataPlatform:bigquery", "bigquery"},
      {"urn:li:dataPlatform:mysql", "mysql"},
      {"urn:li:dataPlatform:custom-platform", "custom-platform"},
      // Malformed URNs should fall back to the raw value
      {"not-a-urn", "not-a-urn"},
      {"urn:li:dataset:foo", "urn:li:dataset:foo"},
      {"", ""},
      // Null should return null
      {null, null},
    };
  }

  @Test(dataProvider = "extractPlatformNameTestCases")
  public void testExtractPlatformName(String input, String expected) throws Exception {
    DailyReport dailyReport = createDailyReportForTesting();

    java.lang.reflect.Method method =
        DailyReport.class.getDeclaredMethod("extractPlatformName", String.class);
    method.setAccessible(true);

    String result = (String) method.invoke(dailyReport, input);
    assertEquals(
        result,
        expected,
        String.format(
            "extractPlatformName(\"%s\") should return \"%s\" but got \"%s\"",
            input, expected, result));
  }

  @Test
  public void testAnonymizeToBucketBoundaries() throws Exception {
    DailyReport dailyReport = createDailyReportForTesting();
    java.lang.reflect.Method anonymizeToBucketMethod =
        DailyReport.class.getDeclaredMethod("anonymizeToBucket", int.class);
    anonymizeToBucketMethod.setAccessible(true);

    // Test boundaries explicitly
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 0), "0");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 10), "0-10");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 11), "10-100");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 100), "10-100");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 101), "100-1K");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 1000), "100-1K");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 1001), "1K-10K");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 10000), "1K-10K");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 10001), "10K-100K");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 100000), "10K-100K");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 100001), "100K-1M");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 1000000), "100K-1M");
    assertEquals(anonymizeToBucketMethod.invoke(dailyReport, 1000001), "1M+");
  }

  /**
   * The whole point of batching: one aggregation covering every reported entity type, rather than
   * one count query per type.
   */
  @Test
  public void testCollectEntityCountsIssuesSingleSearch() throws Exception {
    org.opensearch.search.aggregations.bucket.filter.Filters byEntity =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.class);
    // Nested mocks must be fully built before being handed to thenReturn().
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket empty = entityBucket(0L);
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket datasets = entityBucket(100L);
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket tags = entityBucket(7L);
    when(byEntity.getBucketByKey(anyString())).thenReturn(empty);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasets);
    when(byEntity.getBucketByKey("TAG")).thenReturn(tags);
    stubAggregationResponse(byEntity);

    DailyReport dailyReport = createDailyReportForTesting();
    Map<String, Integer> counts =
        dailyReport.collectEntityCounts(
            new AnalyticsService(mockElasticClient, mockIndexConvention));

    org.mockito.ArgumentCaptor<SearchRequest> captor =
        org.mockito.ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockElasticClient, times(1))
        .search(any(OperationContext.class), captor.capture(), any(RequestOptions.class));
    // One request spanning every reported entity type, not one request per type.
    assertEquals(captor.getValue().indices().length, REPORTED_ENTITY_TYPE_COUNT);

    assertEquals(counts.get("DATASET"), Integer.valueOf(100));
    assertEquals(counts.get("TAG"), Integer.valueOf(7));
  }

  /** Zero-count types are dropped so the telemetry payload stays concise. */
  @Test
  public void testCollectEntityCountsOmitsZeroCounts() throws Exception {
    org.opensearch.search.aggregations.bucket.filter.Filters byEntity =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.class);
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket empty = entityBucket(0L);
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket datasets = entityBucket(5L);
    when(byEntity.getBucketByKey(anyString())).thenReturn(empty);
    when(byEntity.getBucketByKey("DATASET")).thenReturn(datasets);
    stubAggregationResponse(byEntity);

    DailyReport dailyReport = createDailyReportForTesting();
    Map<String, Integer> counts =
        dailyReport.collectEntityCounts(
            new AnalyticsService(mockElasticClient, mockIndexConvention));

    assertEquals(counts.size(), 1, "only the non-zero type should be reported");
    assertTrue(counts.containsKey("DATASET"));
  }

  /**
   * A type whose index name cannot be resolved is dropped before the batch is built. Batched, it
   * would otherwise throw and take every other entity count with it, where the per-type queries it
   * replaced simply skipped that one type.
   */
  @Test
  public void testCollectEntityCountsSkipsUnresolvableTypes() throws Exception {
    when(mockIndexConvention.getEntityIndexName(
            any(OperationFingerprint.class), eq(Constants.DATASET_ENTITY_NAME)))
        .thenThrow(new IllegalArgumentException("no index configured for dataset"));

    org.opensearch.search.aggregations.bucket.filter.Filters byEntity =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.class);
    // Nested mocks must be fully built before being handed to thenReturn().
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket populated = entityBucket(3L);
    when(byEntity.getBucketByKey(anyString())).thenReturn(populated);
    stubAggregationResponse(byEntity);

    DailyReport dailyReport = createDailyReportForTesting();
    Map<String, Integer> counts =
        dailyReport.collectEntityCounts(
            new AnalyticsService(mockElasticClient, mockIndexConvention));

    org.mockito.ArgumentCaptor<SearchRequest> captor =
        org.mockito.ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockElasticClient, times(1))
        .search(any(OperationContext.class), captor.capture(), any(RequestOptions.class));

    // The batch still goes out, one index short, rather than failing outright.
    assertEquals(captor.getValue().indices().length, REPORTED_ENTITY_TYPE_COUNT - 1);
    assertFalse(counts.containsKey("DATASET"), "unresolvable type must not be reported");
    assertEquals(counts.size(), REPORTED_ENTITY_TYPE_COUNT - 1);
  }

  private org.opensearch.search.aggregations.bucket.filter.Filters.Bucket entityBucket(
      long docCount) {
    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket bucket =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.Bucket.class);
    when(bucket.getDocCount()).thenReturn(docCount);
    return bucket;
  }

  /** Wraps the by_entity aggregation in the filtered wrapper AnalyticsService reads back. */
  private void stubAggregationResponse(
      org.opensearch.search.aggregations.bucket.filter.Filters byEntity) throws Exception {
    org.opensearch.search.aggregations.Aggregations filteredAggs =
        mock(org.opensearch.search.aggregations.Aggregations.class);
    when(filteredAggs.get("by_entity")).thenReturn(byEntity);
    org.opensearch.search.aggregations.bucket.filter.Filter filtered =
        mock(org.opensearch.search.aggregations.bucket.filter.Filter.class);
    when(filtered.getAggregations()).thenReturn(filteredAggs);
    org.opensearch.search.aggregations.Aggregations topLevel =
        mock(org.opensearch.search.aggregations.Aggregations.class);
    when(topLevel.get("filtered")).thenReturn(filtered);
    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(topLevel);
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(response);
  }

  /**
   * Captures the telemetry payload through ping(), which is the only externally observable output
   * of dailyReport(). Uses nothing but the public entry point, so it runs unchanged against the
   * pre-batching implementation for a like-for-like payload comparison.
   */
  @Test
  public void testDailyReportPayload() throws Exception {
    stubEverything();

    DailyReport spy = org.mockito.Mockito.spy(createDailyReportForTesting());
    org.mockito.Mockito.doNothing().when(spy).ping(anyString(), any(JSONObject.class));

    spy.dailyReport();

    org.mockito.ArgumentCaptor<JSONObject> payload =
        org.mockito.ArgumentCaptor.forClass(JSONObject.class);
    verify(spy).ping(eq("service-daily"), payload.capture());

    org.mockito.ArgumentCaptor<SearchRequest> requests =
        org.mockito.ArgumentCaptor.forClass(SearchRequest.class);
    verify(mockElasticClient, atLeast(0))
        .search(any(OperationContext.class), requests.capture(), any(RequestOptions.class));

    // Pre-batching this was 25 searches: 3 active-user windows plus one per reported entity type.
    List<SearchRequest> issued = requests.getAllValues();
    assertEquals(issued.size(), 5, "expected one search per concern, not one per entity type");

    List<SearchRequest> multiIndex =
        issued.stream()
            .filter(r -> r.indices().length > 1)
            .collect(java.util.stream.Collectors.toList());
    assertEquals(multiIndex.size(), 1, "entity counts should collapse into a single search");
    assertEquals(
        multiIndex.get(0).indices().length,
        REPORTED_ENTITY_TYPE_COUNT,
        "the batch must span every reported entity type");

    // The remaining four are the usage index, total users, service accounts and platform chart.
    assertEquals(issued.size() - multiIndex.size(), 4);

    // Payload must be unchanged by the batching - these values are what the per-query
    // implementation produced from the same stubbed counts.
    JSONObject report = payload.getValue();
    assertEquals(report.get("dau"), 8);
    assertEquals(report.get("wau"), 8);
    assertEquals(report.get("mau"), 8);
    assertEquals(report.get("total_assets"), "1K-10K");
    assertEquals(report.get("total_user_count"), 32);
    assertEquals(report.get("total_service_account_count"), 32);
    assertEquals(report.get("server_type"), "test");

    long entityCountKeys =
        keysOf(report).stream().filter(k -> k.startsWith("entity_count_")).count();
    assertEquals(
        entityCountKeys,
        (long) REPORTED_ENTITY_TYPE_COUNT,
        "every reported entity type should carry a count");
  }

  /**
   * Batching made one query carry all 19 entity counts, so its failure must not also cost the
   * platform statistics, which are independent and were still collected before batching.
   */
  @Test
  public void testPlatformStatsSurviveEntityCountFailure() throws Exception {
    stubEverything();
    // Nested mocks must be fully built before being handed to the answer.
    SearchResponse ok = stubResponse();
    // The entity batch is the only multi-index search; fail just that one.
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenAnswer(
            invocation -> {
              SearchRequest request = invocation.getArgument(1);
              if (request.indices().length > 1) {
                throw new RuntimeException("Search query failed:");
              }
              return ok;
            });

    DailyReport spy = org.mockito.Mockito.spy(createDailyReportForTesting());
    org.mockito.Mockito.doNothing().when(spy).ping(anyString(), any(JSONObject.class));
    spy.dailyReport();

    org.mockito.ArgumentCaptor<JSONObject> payload =
        org.mockito.ArgumentCaptor.forClass(JSONObject.class);
    verify(spy).ping(eq("service-daily"), payload.capture());
    JSONObject report = payload.getValue();

    assertTrue(keysOf(report).contains("platform_count"), "platform stats must still be collected");
    // Omitted rather than reported as a fabricated zero.
    assertFalse(keysOf(report).contains("total_assets"));
    assertTrue(
        keysOf(report).stream().noneMatch(k -> k.startsWith("entity_count_")),
        "entity counts should be absent when their batch failed");
  }

  private static java.util.List<String> keysOf(JSONObject o) {
    java.util.List<String> keys = new java.util.ArrayList<>();
    java.util.Iterator<String> it = o.keys();
    while (it.hasNext()) {
      keys.add(it.next());
    }
    return keys;
  }

  /** Stubs the client to answer every search with {@link #stubResponse()}, plus config mocks. */
  private void stubEverything() throws Exception {
    // Nested mocks must be fully built before being handed to thenReturn().
    SearchResponse response = stubResponse();
    when(mockElasticClient.search(
            any(OperationContext.class), any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(response);

    when(mockIndexConvention.getIndexName(any(OperationFingerprint.class), anyString()))
        .thenReturn("datahub_usage_event");

    com.linkedin.metadata.config.DataHubConfiguration dataHub =
        mock(com.linkedin.metadata.config.DataHubConfiguration.class);
    when(dataHub.getServerType()).thenReturn("test");
    when(mockConfigurationProvider.getDatahub()).thenReturn(dataHub);
    when(mockGitVersion.getVersion()).thenReturn("1.0.0-test");
  }

  /** Response usable by both the batched and the per-query implementations. */
  private SearchResponse stubResponse() throws Exception {
    org.opensearch.search.aggregations.metrics.Cardinality cardinality =
        mock(org.opensearch.search.aggregations.metrics.Cardinality.class);
    when(cardinality.getValue()).thenReturn(8L);

    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket rangeBucket =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.Bucket.class);
    org.opensearch.search.aggregations.Aggregations rangeAggs =
        mock(org.opensearch.search.aggregations.Aggregations.class);
    when(rangeAggs.get("unique")).thenReturn(cardinality);
    when(rangeBucket.getAggregations()).thenReturn(rangeAggs);
    org.opensearch.search.aggregations.bucket.filter.Filters byRange =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.class);
    when(byRange.getBucketByKey(anyString())).thenReturn(rangeBucket);

    org.opensearch.search.aggregations.bucket.filter.Filters.Bucket entityBucket =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.Bucket.class);
    when(entityBucket.getDocCount()).thenReturn(100L);
    org.opensearch.search.aggregations.bucket.filter.Filters byEntity =
        mock(org.opensearch.search.aggregations.bucket.filter.Filters.class);
    when(byEntity.getBucketByKey(anyString())).thenReturn(entityBucket);

    // One filtered wrapper serving both shapes: the batched sub-aggregations, and the
    // doc count / cardinality the per-query implementation reads straight off it.
    org.opensearch.search.aggregations.Aggregations filteredAggs =
        mock(org.opensearch.search.aggregations.Aggregations.class);
    when(filteredAggs.get("by_range")).thenReturn(byRange);
    when(filteredAggs.get("by_entity")).thenReturn(byEntity);
    when(filteredAggs.get("unique")).thenReturn(cardinality);
    org.opensearch.search.aggregations.bucket.filter.Filter filtered =
        mock(org.opensearch.search.aggregations.bucket.filter.Filter.class);
    when(filtered.getAggregations()).thenReturn(filteredAggs);
    when(filtered.getDocCount()).thenReturn(100L);

    org.opensearch.search.aggregations.Aggregations topLevel =
        mock(org.opensearch.search.aggregations.Aggregations.class);
    when(topLevel.get("filtered")).thenReturn(filtered);

    org.apache.lucene.search.TotalHits totalHits =
        new org.apache.lucene.search.TotalHits(
            42, org.apache.lucene.search.TotalHits.Relation.EQUAL_TO);
    org.opensearch.search.SearchHits hits = mock(org.opensearch.search.SearchHits.class);
    when(hits.getTotalHits()).thenReturn(totalHits);

    SearchResponse response = mock(SearchResponse.class);
    when(response.getAggregations()).thenReturn(topLevel);
    when(response.getHits()).thenReturn(hits);
    return response;
  }
}
