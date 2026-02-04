package com.linkedin.gms.factory.telemetry;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.version.GitVersion;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
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
    when(mockIndexConvention.getEntityIndexName(anyString())).thenReturn("corpuserindex_v2");
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
    when(mockElasticClient.search(any(SearchRequest.class), any(RequestOptions.class)))
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
    when(mockElasticClient.search(any(SearchRequest.class), any(RequestOptions.class)))
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
    when(mockElasticClient.search(any(SearchRequest.class), any(RequestOptions.class)))
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
    when(mockElasticClient.search(any(SearchRequest.class), any(RequestOptions.class)))
        .thenReturn(mockSearchResponse);

    DailyReport dailyReport = createDailyReportForTesting();

    // Use reflection to call the private method
    java.lang.reflect.Method getServiceAccountCountMethod =
        DailyReport.class.getDeclaredMethod("getServiceAccountCount");
    getServiceAccountCountMethod.setAccessible(true);

    int result = (int) getServiceAccountCountMethod.invoke(dailyReport);

    assertEquals(result, 5, "getServiceAccountCount should return the total hits count");
  }
}
