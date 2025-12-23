package com.linkedin.metadata.test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.r2.RemoteInvocationException;
import com.linkedin.test.TestInfo;
import com.linkedin.test.TestMode;
import com.linkedin.test.TestStatus;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.*;

public class TestFetcherTest {

  @Mock private EntityService<?> mockEntityService;

  @Mock private EntitySearchService mockEntitySearchService;

  @Mock
  private OperationContext operationContext =
      TestOperationContexts.systemContextNoSearchAuthorization();

  private TestFetcher testFetcher;

  private static final String TEST_URN_STRING = "urn:li:test:test1";
  private static final String TEST_URN_STRING_2 = "urn:li:test:test2";
  private static final String TEST_URN_STRING_3 = "urn:li:test:test3";

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    testFetcher = new TestFetcher(mockEntityService, mockEntitySearchService);
  }

  @Test
  public void testIsPartial() {
    // Test the isPartial method
    assertFalse(testFetcher.isPartial(), "isPartial should return false");
  }

  @Test
  public void testFetchOne_Success() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    TestInfo testInfo = createActiveTestInfo();
    EntityResponse entityResponse = createEntityResponse(testUrn, testInfo);

    Map<Urn, EntityResponse> responseMap = ImmutableMap.of(testUrn, entityResponse);
    when(mockEntityService.getEntitiesV2(
            eq(operationContext),
            eq(TEST_ENTITY_NAME),
            eq(ImmutableSet.of(testUrn)),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(responseMap);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetchOne(operationContext, testUrn);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getTests().size(), 1);
    assertEquals(result.getTests().get(0).getUrn(), testUrn);
    assertEquals(result.getTests().get(0).getTestInfo(), testInfo);
  }

  @Test
  public void testFetchOne_NotFound() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);

    when(mockEntityService.getEntitiesV2(
            eq(operationContext),
            eq(TEST_ENTITY_NAME),
            eq(ImmutableSet.of(testUrn)),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(new HashMap<>());

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetchOne(operationContext, testUrn);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getTests().isEmpty());
  }

  @Test
  public void testFetchOne_InactiveTest() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    TestInfo testInfo = createInactiveTestInfo();
    EntityResponse entityResponse = createEntityResponse(testUrn, testInfo);

    Map<Urn, EntityResponse> responseMap = ImmutableMap.of(testUrn, entityResponse);
    when(mockEntityService.getEntitiesV2(
            eq(operationContext),
            eq(TEST_ENTITY_NAME),
            eq(ImmutableSet.of(testUrn)),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(responseMap);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetchOne(operationContext, testUrn);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getTests().isEmpty());
  }

  @Test
  public void testFetchOne_URISyntaxException() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    when(mockEntityService.getEntitiesV2(any(), any(), any(), any()))
        .thenThrow(new URISyntaxException("Invalid URI", "test"));

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetchOne(operationContext, testUrn);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getTests().isEmpty());
  }

  @Test
  public void testFetch_WithoutQuery_Success()
      throws RemoteInvocationException, URISyntaxException {
    // Given

    SearchResult searchResult = createSearchResult(TEST_URN_STRING, TEST_URN_STRING_2);
    when(mockEntitySearchService.search(any(), anyList(), eq(""), any(), anyList(), eq(0), eq(10)))
        .thenReturn(searchResult);

    Map<Urn, EntityResponse> entityResponses = createEntityResponses();
    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(TEST_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(entityResponses);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetch(operationContext, 0, 10);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getTests().size(), 2);
  }

  @Test
  public void testFetch_WithQuery_Success() throws RemoteInvocationException, URISyntaxException {
    // Given
    String query = "test query";

    SearchResult searchResult = createSearchResult(TEST_URN_STRING);
    when(mockEntitySearchService.search(
            any(), anyList(), eq(query), any(), anyList(), eq(0), eq(10)))
        .thenReturn(searchResult);

    Map<Urn, EntityResponse> entityResponses = createSingleEntityResponse();
    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(TEST_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(entityResponses);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetch(operationContext, 0, 10, query);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getTests().size(), 1);
  }

  @Test
  public void testFetch_EmptySearchResult() throws RemoteInvocationException, URISyntaxException {
    // Given

    SearchResult emptySearchResult =
        new SearchResult().setEntities(new SearchEntityArray()).setNumEntities(0);

    when(mockEntitySearchService.search(any(), anyList(), eq(""), any(), anyList(), eq(0), eq(10)))
        .thenReturn(emptySearchResult);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetch(operationContext, 0, 10);

    // Then
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getTests().isEmpty());
  }

  @Test
  public void testFetch_MixedActiveInactiveTests()
      throws RemoteInvocationException, URISyntaxException {
    // Given
    SearchResult searchResult =
        createSearchResult(TEST_URN_STRING, TEST_URN_STRING_2, TEST_URN_STRING_3);
    when(mockEntitySearchService.search(any(), anyList(), eq(""), any(), any(), eq(0), eq(10)))
        .thenReturn(searchResult);

    Map<Urn, EntityResponse> entityResponses = createMixedEntityResponses();
    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(TEST_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(entityResponses);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetch(operationContext, 0, 10);

    // Then
    assertNotNull(result);
    // Total should be the search result count (3), not the extracted count (2)
    // This ensures pagination continues correctly even when tests are filtered out
    assertEquals(result.getTotal(), 3);
    // Only 2 tests are extracted (1 is INACTIVE)
    assertEquals(result.getTests().size(), 2);
  }

  @Test
  public void testFetch_PaginationTotalReflectsSearchResult()
      throws RemoteInvocationException, URISyntaxException {
    // This test verifies that the pagination bug is fixed:
    // When some tests are filtered out during extraction, the total should still
    // reflect the search result count to ensure pagination continues correctly.

    // Given: Search returns 50 total, but we're fetching page of 10
    SearchResult searchResult =
        createSearchResultWithTotal(
            50, TEST_URN_STRING, TEST_URN_STRING_2); // 50 total, 2 in this page
    when(mockEntitySearchService.search(any(), anyList(), eq(""), any(), any(), eq(0), eq(10)))
        .thenReturn(searchResult);

    // One test is INACTIVE, so only 1 will be extracted
    Map<Urn, EntityResponse> entityResponses = new HashMap<>();
    Urn urn1 = Urn.createFromString(TEST_URN_STRING);
    Urn urn2 = Urn.createFromString(TEST_URN_STRING_2);
    entityResponses.put(urn1, createEntityResponse(urn1, createActiveTestInfo()));
    entityResponses.put(urn2, createEntityResponse(urn2, createInactiveTestInfo()));

    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class),
            eq(TEST_ENTITY_NAME),
            any(),
            eq(ImmutableSet.of(TEST_INFO_ASPECT_NAME))))
        .thenReturn(entityResponses);

    // When
    TestFetcher.TestFetchResult result = testFetcher.fetch(operationContext, 0, 10);

    // Then
    assertNotNull(result);
    // Total MUST be 50 (search total) to ensure pagination continues
    // If this were 1 (extracted count), pagination would stop early and miss tests 10-50!
    assertEquals(result.getTotal(), 50);
    // Only 1 test extracted (the other is INACTIVE)
    assertEquals(result.getTests().size(), 1);
  }

  @Test
  public void testFetch_MultiplePaginationCalls_WithFilteredTests()
      throws RemoteInvocationException, URISyntaxException {
    // This test simulates the actual pagination loop from TestRefreshRunnable
    // to verify that multiple pages are fetched even when tests are filtered out.
    //
    // Scenario: 5 tests total, page size of 2, with some INACTIVE tests
    // Page 1 (start=0): 2 tests, 1 filtered -> extracted=1, total=5
    // Page 2 (start=2): 2 tests, 1 filtered -> extracted=1, total=5
    // Page 3 (start=4): 1 test, 0 filtered -> extracted=1, total=5
    // Without the fix, pagination would stop after page 1 (total=1, start=2, 2>=1)

    int pageSize = 2;
    int totalTests = 5;

    // Set up page 1 (start=0)
    SearchResult page1 =
        createSearchResultWithTotal(totalTests, "urn:li:test:t1", "urn:li:test:t2");
    when(mockEntitySearchService.search(
            any(), anyList(), eq(""), any(), any(), eq(0), eq(pageSize)))
        .thenReturn(page1);

    // Set up page 2 (start=2)
    SearchResult page2 =
        createSearchResultWithTotal(totalTests, "urn:li:test:t3", "urn:li:test:t4");
    when(mockEntitySearchService.search(
            any(), anyList(), eq(""), any(), any(), eq(2), eq(pageSize)))
        .thenReturn(page2);

    // Set up page 3 (start=4)
    SearchResult page3 = createSearchResultWithTotal(totalTests, "urn:li:test:t5");
    when(mockEntitySearchService.search(
            any(), anyList(), eq(""), any(), any(), eq(4), eq(pageSize)))
        .thenReturn(page3);

    // Set up entity responses - some active, some inactive
    when(mockEntityService.getEntitiesV2(
            any(OperationContext.class), eq(TEST_ENTITY_NAME), any(), any()))
        .thenAnswer(
            invocation -> {
              @SuppressWarnings("unchecked")
              java.util.Set<Urn> urns = invocation.getArgument(2);
              Map<Urn, EntityResponse> responses = new HashMap<>();
              for (Urn urn : urns) {
                // Make t2 and t4 inactive (to simulate filtering)
                boolean isInactive =
                    urn.toString().equals("urn:li:test:t2")
                        || urn.toString().equals("urn:li:test:t4");
                TestInfo testInfo = isInactive ? createInactiveTestInfo() : createActiveTestInfo();
                responses.put(urn, createEntityResponse(urn, testInfo));
              }
              return responses;
            });

    // Simulate the pagination loop from TestRefreshRunnable
    int start = 0;
    int total = pageSize;
    int totalExtracted = 0;
    int fetchCallCount = 0;

    while (start < total) {
      TestFetcher.TestFetchResult result = testFetcher.fetch(operationContext, start, pageSize);
      total = result.getTotal();
      totalExtracted += result.getTests().size();
      start += pageSize;
      fetchCallCount++;
    }

    // Verify all 3 pages were fetched
    assertEquals(fetchCallCount, 3, "Should have made 3 fetch calls to get all pages");

    // Verify total extracted (t1, t3, t5 are active; t2, t4 are inactive)
    assertEquals(totalExtracted, 3, "Should have extracted 3 active tests");

    // Verify search was called 3 times with correct start values
    verify(mockEntitySearchService, times(1))
        .search(any(), anyList(), eq(""), any(), any(), eq(0), eq(pageSize));
    verify(mockEntitySearchService, times(1))
        .search(any(), anyList(), eq(""), any(), any(), eq(2), eq(pageSize));
    verify(mockEntitySearchService, times(1))
        .search(any(), anyList(), eq(""), any(), any(), eq(4), eq(pageSize));
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testFetch_RemoteInvocationException()
      throws RemoteInvocationException, URISyntaxException {
    // Given
    // Use doThrow for checked exceptions
    doThrow(new RemoteInvocationException("Network error"))
        .when(mockEntitySearchService)
        .search(any(), anyList(), eq(""), isNull(), anyList(), eq(0), eq(10));

    // When/Then - should throw exception
    testFetcher.fetch(operationContext, 0, 10);
  }

  @Test
  public void testExtractTest_ValidActiveTest() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    TestInfo testInfo = createActiveTestInfo();
    EntityResponse entityResponse = createEntityResponse(testUrn, testInfo);

    // When
    var result = testFetcher.extractTest(entityResponse);

    // Then
    assertTrue(result.isPresent());
    assertEquals(result.get().getUrn(), testUrn);
    assertEquals(result.get().getTestInfo(), testInfo);
  }

  @Test
  public void testExtractTest_InactiveTest() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    TestInfo testInfo = createInactiveTestInfo();
    EntityResponse entityResponse = createEntityResponse(testUrn, testInfo);

    // When
    var result = testFetcher.extractTest(entityResponse);

    // Then
    assertFalse(result.isPresent());
  }

  @Test
  public void testExtractTest_NullEntityResponse() {
    // When
    var result = testFetcher.extractTest(null);

    // Then
    assertFalse(result.isPresent());
  }

  @Test
  public void testExtractTest_MissingTestInfoAspect() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    EntityResponse entityResponse =
        new EntityResponse().setUrn(testUrn).setAspects(new EnvelopedAspectMap());

    // When
    var result = testFetcher.extractTest(entityResponse);

    // Then
    assertFalse(result.isPresent());
  }

  @Test
  public void testExtractTest_NullStatus() throws URISyntaxException {
    // Given
    Urn testUrn = Urn.createFromString(TEST_URN_STRING);
    TestInfo testInfo = new TestInfo(); // No status set
    EntityResponse entityResponse = createEntityResponse(testUrn, testInfo);

    // When
    var result = testFetcher.extractTest(entityResponse);

    // Then
    assertTrue(result.isPresent());
  }

  @Test
  public void testTestFetchResult_EmptyConstant() {
    // Test the EMPTY constant
    TestFetcher.TestFetchResult empty = TestFetcher.TestFetchResult.EMPTY;

    assertNotNull(empty);
    assertTrue(empty.getTests().isEmpty());
    assertEquals(empty.getTotal(), 0);
  }

  // Helper methods

  private TestInfo createActiveTestInfo() {
    TestInfo testInfo = new TestInfo();
    TestStatus status = new TestStatus();
    status.setMode(TestMode.ACTIVE);
    testInfo.setStatus(status);
    return testInfo;
  }

  private TestInfo createInactiveTestInfo() {
    TestInfo testInfo = new TestInfo();
    TestStatus status = new TestStatus();
    status.setMode(TestMode.INACTIVE);
    testInfo.setStatus(status);
    return testInfo;
  }

  private EntityResponse createEntityResponse(Urn urn, TestInfo testInfo) {
    EnvelopedAspectMap aspectMap = new EnvelopedAspectMap();
    EnvelopedAspect envelopedAspect = new EnvelopedAspect();
    envelopedAspect.setValue(new Aspect(testInfo.data()));
    aspectMap.put(TEST_INFO_ASPECT_NAME, envelopedAspect);

    return new EntityResponse().setUrn(urn).setAspects(aspectMap);
  }

  private SearchResult createSearchResult(String... urnStrings) throws URISyntaxException {
    SearchEntityArray entities = new SearchEntityArray();
    for (String urnString : urnStrings) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(Urn.createFromString(urnString));
      entities.add(entity);
    }

    return new SearchResult().setEntities(entities).setNumEntities(entities.size());
  }

  /**
   * Creates a search result with a specific total count (for pagination testing). This simulates a
   * paginated search where total is greater than the entities in the current page.
   */
  private SearchResult createSearchResultWithTotal(int total, String... urnStrings)
      throws URISyntaxException {
    SearchEntityArray entities = new SearchEntityArray();
    for (String urnString : urnStrings) {
      SearchEntity entity = new SearchEntity();
      entity.setEntity(Urn.createFromString(urnString));
      entities.add(entity);
    }

    return new SearchResult().setEntities(entities).setNumEntities(total);
  }

  private Map<Urn, EntityResponse> createEntityResponses() throws URISyntaxException {
    Map<Urn, EntityResponse> responses = new HashMap<>();

    Urn urn1 = Urn.createFromString(TEST_URN_STRING);
    Urn urn2 = Urn.createFromString(TEST_URN_STRING_2);

    responses.put(urn1, createEntityResponse(urn1, createActiveTestInfo()));
    responses.put(urn2, createEntityResponse(urn2, createActiveTestInfo()));

    return responses;
  }

  private Map<Urn, EntityResponse> createSingleEntityResponse() throws URISyntaxException {
    Urn urn = Urn.createFromString(TEST_URN_STRING);
    return ImmutableMap.of(urn, createEntityResponse(urn, createActiveTestInfo()));
  }

  private Map<Urn, EntityResponse> createMixedEntityResponses() throws URISyntaxException {
    Map<Urn, EntityResponse> responses = new HashMap<>();

    Urn urn1 = Urn.createFromString(TEST_URN_STRING);
    Urn urn2 = Urn.createFromString(TEST_URN_STRING_2);
    Urn urn3 = Urn.createFromString(TEST_URN_STRING_3);

    responses.put(urn1, createEntityResponse(urn1, createActiveTestInfo()));
    responses.put(urn2, createEntityResponse(urn2, createInactiveTestInfo())); // Inactive
    responses.put(urn3, createEntityResponse(urn3, createActiveTestInfo()));

    return responses;
  }
}
