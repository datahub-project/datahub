package com.linkedin.metadata.test;

import static com.linkedin.metadata.Constants.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;
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
    assertEquals(result.getTotal(), 2); // Only active tests
    assertEquals(result.getTests().size(), 2);
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
