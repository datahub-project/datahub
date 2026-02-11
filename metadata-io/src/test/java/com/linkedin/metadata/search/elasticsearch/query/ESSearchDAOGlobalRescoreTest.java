package com.linkedin.metadata.search.elasticsearch.query;

import static io.datahubproject.test.search.SearchTestUtils.TEST_OS_SEARCH_CONFIG;
import static io.datahubproject.test.search.SearchTestUtils.TEST_SEARCH_SERVICE_CONFIG;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.query.filter.QueryFilterRewriteChain;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import java.lang.reflect.Method;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for ESSearchDAO global rescore window functionality.
 *
 * <p>These tests verify the helper methods used to implement consistent rescoring across different
 * page sizes by using a global rescore window instead of per-page rescoring.
 */
public class ESSearchDAOGlobalRescoreTest {

  private ESSearchDAO esSearchDAO;
  private SearchClientShim<?> mockClient;

  @BeforeMethod
  public void setUp() {
    mockClient = Mockito.mock(SearchClientShim.class);

    esSearchDAO =
        new ESSearchDAO(
            mockClient,
            false,
            TEST_OS_SEARCH_CONFIG,
            null,
            QueryFilterRewriteChain.EMPTY,
            TEST_SEARCH_SERVICE_CONFIG);
  }

  @Test
  public void testCalculateRescoreWindow_SmallPage() throws Exception {
    // Input: from=0, size=20, configuredWindow=100, maxWindow=5000
    // Expected: 100 (fetch window, not page size)
    int result = invokeCalculateRescoreWindow(0, 20, 100, 5000);
    assertEquals(result, 100, "Should fetch configured window size for small pages");
  }

  @Test
  public void testCalculateRescoreWindow_LargePage() throws Exception {
    // Input: from=0, size=200, configuredWindow=100, maxWindow=5000
    // Expected: 200 (fetch requested range which exceeds window)
    int result = invokeCalculateRescoreWindow(0, 200, 100, 5000);
    assertEquals(result, 200, "Should fetch up to requested end when larger than window");
  }

  @Test
  public void testCalculateRescoreWindow_DeepPagination() throws Exception {
    // Input: from=900, size=100, configuredWindow=100, maxWindow=5000
    // Expected: 1000 (need to fetch up to position 1000)
    int result = invokeCalculateRescoreWindow(900, 100, 100, 5000);
    assertEquals(result, 1000, "Should fetch up to end of requested range for deep pagination");
  }

  @Test
  public void testCalculateRescoreWindow_MaxCap() throws Exception {
    // Input: from=10000, size=100, configuredWindow=100, maxWindow=5000
    // Expected: 5000 (capped at maxWindow)
    int result = invokeCalculateRescoreWindow(10000, 100, 100, 5000);
    assertEquals(result, 5000, "Should cap at maxWindow for safety");
  }

  @Test
  public void testCalculateRescoreWindow_ExactlyAtBoundary() throws Exception {
    // Input: from=100, size=20, configuredWindow=100, maxWindow=5000
    // Expected: 120 (fetch through requested range)
    // Note: In practice, the optimization in executeAndExtract() will skip rescoring
    // entirely when from >= configuredWindow, but this tests the helper method logic
    int result = invokeCalculateRescoreWindow(100, 20, 100, 5000);
    assertEquals(result, 120, "Should fetch through requested range when at boundary");
  }

  @Test
  public void testCalculateRescoreWindow_BeyondWindow() throws Exception {
    // Input: from=150, size=20, configuredWindow=100, maxWindow=5000
    // Expected: 170 (fetch through requested range)
    // Note: In practice, the optimization in executeAndExtract() will skip rescoring
    // entirely when from >= configuredWindow, so this helper is never called
    int result = invokeCalculateRescoreWindow(150, 20, 100, 5000);
    assertEquals(result, 170, "Should fetch through requested range when beyond window");
  }

  @Test
  public void testCalculateRescoreWindow_AtMaxBoundary() throws Exception {
    // Input: from=4900, size=100, configuredWindow=100, maxWindow=5000
    // Expected: 5000 (exactly at max)
    int result = invokeCalculateRescoreWindow(4900, 100, 100, 5000);
    assertEquals(result, 5000, "Should return maxWindow when requested end equals max");
  }

  @Test
  public void testSliceSearchResultForPagination() throws Exception {
    // Create SearchResult with 100 entities
    SearchResult fullResult = createSearchResultWithEntities(100);

    // Slice from=20, size=10
    SearchResult sliced = invokeSliceSearchResultForPagination(fullResult, 20, 10);

    assertNotNull(sliced, "Sliced result should not be null");
    assertEquals(sliced.getEntities().size(), 10, "Should return 10 entities");
    assertEquals(sliced.getFrom(), 20, "Should preserve requested from value");
    assertEquals(sliced.getPageSize(), 10, "Should preserve requested page size");

    // Verify we got the right entities (positions 20-29)
    for (int i = 0; i < 10; i++) {
      SearchEntity entity = sliced.getEntities().get(i);
      String expectedUrn = "urn:li:dataset:entity" + (20 + i);
      assertEquals(
          entity.getEntity().toString(),
          expectedUrn,
          "Should contain entity at position " + (20 + i));
    }
  }

  @Test
  public void testSliceSearchResultForPagination_BeyondEnd() throws Exception {
    // Create SearchResult with 50 entities
    SearchResult fullResult = createSearchResultWithEntities(50);

    // Request from=45, size=20 (only 5 entities available)
    SearchResult sliced = invokeSliceSearchResultForPagination(fullResult, 45, 20);

    assertNotNull(sliced, "Sliced result should not be null");
    assertEquals(sliced.getEntities().size(), 5, "Should return only available entities");
    assertEquals(sliced.getFrom(), 45, "Should preserve requested from value");
    assertEquals(sliced.getPageSize(), 20, "Should preserve requested page size");
  }

  @Test
  public void testSliceSearchResultForPagination_FromBeyondEnd() throws Exception {
    // Create SearchResult with 50 entities
    SearchResult fullResult = createSearchResultWithEntities(50);

    // Request from=100, size=10 (no entities available)
    SearchResult sliced = invokeSliceSearchResultForPagination(fullResult, 100, 10);

    assertNotNull(sliced, "Sliced result should not be null");
    assertEquals(
        sliced.getEntities().size(), 0, "Should return empty list when from is beyond end");
    assertEquals(sliced.getFrom(), 100, "Should preserve requested from value");
  }

  // Helper methods to invoke private methods via reflection

  private int invokeCalculateRescoreWindow(
      int from, Integer size, int configuredWindow, int maxWindow) throws Exception {
    Method method =
        ESSearchDAO.class.getDeclaredMethod(
            "calculateRescoreWindow", int.class, Integer.class, int.class, int.class);
    method.setAccessible(true);
    return (int) method.invoke(esSearchDAO, from, size, configuredWindow, maxWindow);
  }

  private SearchResult invokeSliceSearchResultForPagination(
      SearchResult rescored, int requestedFrom, Integer requestedSize) throws Exception {
    Method method =
        ESSearchDAO.class.getDeclaredMethod(
            "sliceSearchResultForPagination", SearchResult.class, int.class, Integer.class);
    method.setAccessible(true);
    return (SearchResult) method.invoke(esSearchDAO, rescored, requestedFrom, requestedSize);
  }

  private SearchResult createSearchResultWithEntities(int count) {
    SearchEntityArray entities = new SearchEntityArray();
    for (int i = 0; i < count; i++) {
      SearchEntity entity = new SearchEntity();
      try {
        entity.setEntity(com.linkedin.common.urn.Urn.createFromString("urn:li:dataset:entity" + i));
      } catch (Exception e) {
        throw new RuntimeException("Failed to create URN", e);
      }
      entity.setScore((double) (count - i)); // Descending scores
      entities.add(entity);
    }

    SearchResult result = new SearchResult();
    result.setEntities(entities);
    result.setNumEntities(count);
    result.setFrom(0);
    result.setPageSize(count);
    return result;
  }
}
