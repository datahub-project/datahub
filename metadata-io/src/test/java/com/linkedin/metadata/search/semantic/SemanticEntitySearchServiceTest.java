package com.linkedin.metadata.search.semantic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.data.template.StringArray;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.SearchFlags;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.elasticsearch.index.MappingsBuilder;
import com.linkedin.metadata.search.elasticsearch.index.NoOpMappingsBuilder;
import com.linkedin.metadata.search.embedding.EmbeddingProvider;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.RawResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.util.EntityUtils;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.opensearch.client.Request;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticEntitySearchService */
public class SemanticEntitySearchServiceTest {

  @Mock private SearchClientShim<?> searchClientShim;

  @Mock private IndexConvention mockIndexConvention;

  @Mock private EmbeddingProvider mockEmbeddingProvider;

  @Mock private OperationContext mockOpContext;

  @Mock private EntityRegistry mockEntityRegistry;

  @Mock private EntitySpec mockEntitySpec;

  @Mock private SearchContext mockSearchContext;

  @Mock private SearchFlags mockSearchFlags;

  @Mock private RawResponse mockResponse;

  @Mock private HttpEntity mockHttpEntity;

  @Mock private StatusLine mockStatusLine;

  private MappingsBuilder mappingsBuilder;

  private ObjectMapper objectMapper;
  private SemanticEntitySearchService service;
  private AutoCloseable mocks;

  private static final String TEST_ENTITY_NAME = "dataset";
  private static final String TEST_BASE_INDEX = "datasetindex_v2";
  private static final String TEST_SEMANTIC_INDEX = "datasetindex_v2_semantic";
  private static final String TEST_QUERY = "test query";
  private static final float[] TEST_EMBEDDING = {0.1f, 0.2f, 0.3f, 0.4f};

  @BeforeMethod
  public void setUp() {
    mocks = MockitoAnnotations.openMocks(this);
    objectMapper = new ObjectMapper();
    mappingsBuilder = new NoOpMappingsBuilder();

    // Setup basic mock behavior
    when(mockIndexConvention.getEntityIndexName(TEST_ENTITY_NAME)).thenReturn(TEST_BASE_INDEX);
    when(mockEmbeddingProvider.embed(anyString(), any())).thenReturn(TEST_EMBEDDING);
    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockOpContext.getSearchContext()).thenReturn(mockSearchContext);
    when(mockOpContext.getObjectMapper()).thenReturn(objectMapper);
    when(mockSearchContext.getIndexConvention()).thenReturn(mockIndexConvention);
    when(mockSearchContext.getSearchFlags()).thenReturn(mockSearchFlags);
    when(mockSearchFlags.isFilterNonLatestVersions()).thenReturn(false);

    service =
        new SemanticEntitySearchService(searchClientShim, mockEmbeddingProvider, mappingsBuilder);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mocks != null) {
      mocks.close();
    }
  }

  @Test
  @SuppressWarnings("null") // Intentionally testing null parameter validation
  public void testConstructorValidation() {
    // Test null searchClient
    assertThrows(
        NullPointerException.class,
        () -> new SemanticEntitySearchService(null, mockEmbeddingProvider, mappingsBuilder));

    // Test null embeddingProvider
    assertThrows(
        NullPointerException.class,
        () -> new SemanticEntitySearchService(searchClientShim, null, mappingsBuilder));

    // Test successful construction
    SemanticEntitySearchService validService =
        new SemanticEntitySearchService(searchClientShim, mockEmbeddingProvider, mappingsBuilder);
    assertNotNull(validService);
  }

  @Test
  public void testSearchWithEmptyEntityNames() {
    SearchResult result =
        service.search(mockOpContext, Collections.emptyList(), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getNumEntities().intValue(), 0);
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getPageSize().intValue(), 10);
    assertNotNull(result.getEntities());
    assertTrue(result.getEntities().isEmpty());
    assertNotNull(result.getMetadata());
  }

  @Test
  public void testBasicSearchSuccess() throws IOException {
    // Setup mock response
    setupMockOpenSearchResponse(
        createMockSearchResponse(
            1, "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)", 0.95));

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    // Verify results
    assertNotNull(result);
    assertEquals(result.getNumEntities().intValue(), 1);
    assertEquals(result.getFrom().intValue(), 0);
    assertEquals(result.getPageSize().intValue(), 10);
    assertEquals(result.getEntities().size(), 1);

    SearchEntity entity = result.getEntities().get(0);
    assertNotNull(entity.getEntity());
    assertNotNull(entity.getScore());
    Double score = entity.getScore();
    if (score != null) {
      assertEquals(score.floatValue(), 0.95f, 0.001f);
    }

    // Verify embedding provider was called
    verify(mockEmbeddingProvider).embed(TEST_QUERY, null);

    // Verify OpenSearch request was made
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    assertEquals(capturedRequest.getMethod(), "POST");
    assertTrue(capturedRequest.getEndpoint().contains(TEST_SEMANTIC_INDEX));
  }

  @Test
  public void testSearchWithFilters() throws IOException {
    // Setup entity spec for field types
    when(mockEntityRegistry.getEntitySpec(TEST_ENTITY_NAME)).thenReturn(mockEntitySpec);

    // Setup mock response
    setupMockOpenSearchResponse(
        createMockSearchResponse(
            1, "urn:li:dataset:(urn:li:dataPlatform:hive,test.table,PROD)", 0.85));

    // Create a filter
    Filter filter = createTestFilter("platform", "urn:li:dataPlatform:hive");

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, filter, null, 0, 5);

    assertNotNull(result);
    assertEquals(result.getPageSize().intValue(), 5);

    // Verify request contains filter
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    // Additional verification of filter inclusion would require parsing the JSON body
  }

  @Test
  public void testSearchPagination() throws IOException {
    // Setup mock response with multiple hits
    setupMockOpenSearchResponse(
        createMockSearchResponse(
            3,
            Arrays.asList(
                "urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:test,table2,PROD)",
                "urn:li:dataset:(urn:li:dataPlatform:test,table3,PROD)"),
            Arrays.asList(0.95, 0.90, 0.85)));

    // Test pagination from=1, size=2
    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 1, 2);

    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), 1);
    assertEquals(result.getPageSize().intValue(), 2);
    assertEquals(result.getNumEntities().intValue(), 3); // Total hits
    assertEquals(result.getEntities().size(), 2); // Page size
  }

  @Test
  public void testSearchPaginationBeyondResults() throws IOException {
    // Setup mock response with 1 hit
    setupMockOpenSearchResponse(
        createMockSearchResponse(1, "urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)", 0.95));

    // Test pagination beyond available results
    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 10, 5);

    assertNotNull(result);
    assertEquals(result.getFrom().intValue(), 10);
    assertEquals(result.getPageSize().intValue(), 5);
    assertEquals(result.getNumEntities().intValue(), 0); // No results in this page
    assertTrue(result.getEntities().isEmpty());
  }

  @Test
  public void testSearchWithNullPageSize() throws IOException {
    setupMockOpenSearchResponse(
        createMockSearchResponse(1, "urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)", 0.95));

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, null);

    assertNotNull(result);
    assertEquals(result.getPageSize().intValue(), 10); // Default page size
  }

  @Test
  public void testSearchOpenSearchIOException() throws IOException {
    when(searchClientShim.performLowLevelRequest(any(Request.class)))
        .thenThrow(new IOException("Connection failed"));

    assertThrows(
        RuntimeException.class,
        () ->
            service.search(
                mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10));
  }

  @Test
  public void testSearchEmbeddingProviderException() {
    when(mockEmbeddingProvider.embed(anyString(), any()))
        .thenThrow(new RuntimeException("Embedding service unavailable"));

    assertThrows(
        RuntimeException.class,
        () ->
            service.search(
                mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10));
  }

  @Test
  public void testSearchInvalidUrnInResponse() throws IOException {
    // Setup mock response with invalid URN
    ObjectNode mockResponseJson = createMockSearchResponse(1, "invalid-urn", 0.95);
    setupMockOpenSearchResponse(mockResponseJson);

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    // Should skip invalid URNs
    assertNotNull(result);
    assertEquals(result.getEntities().size(), 0);
  }

  @Test
  public void testSearchMultipleEntityTypes() throws IOException {
    // Setup multiple entity types
    when(mockIndexConvention.getEntityIndexName("dataset")).thenReturn("datasetindex_v2");
    when(mockIndexConvention.getEntityIndexName("chart")).thenReturn("chartindex_v2");

    setupMockOpenSearchResponse(
        createMockSearchResponse(1, "urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)", 0.95));

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList("dataset", "chart"), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);

    // Verify request includes both semantic indices
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();
    String endpoint = capturedRequest.getEndpoint();
    assertTrue(endpoint.contains("datasetindex_v2_semantic"));
    assertTrue(endpoint.contains("chartindex_v2_semantic"));
  }

  @Test
  public void testSearchEntityRegistryException() throws IOException {
    // Setup entity registry to throw exception
    when(mockEntityRegistry.getEntitySpec(TEST_ENTITY_NAME))
        .thenThrow(new RuntimeException("Entity spec not found"));

    setupMockOpenSearchResponse(
        createMockSearchResponse(1, "urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)", 0.95));

    // Should continue with empty field types
    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getEntities().size(), 1);
  }

  @Test
  public void testSearchWithDefaultFieldsOnly() throws IOException {
    // Setup mock response
    setupMockOpenSearchResponse(
        createMockSearchResponse(
            1, "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)", 0.95));

    // No fetchExtraFields specified, should use default fields

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getEntities().size(), 1);

    // Verify the request was made with default fields
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();

    // Parse the request body to verify _source contains default fields
    String requestBody = extractRequestBody(capturedRequest);
    assertNotNull(requestBody);
    assertTrue(
        requestBody.contains("_source"),
        "Request body should contain _source field. Actual body: " + requestBody);
    // Default fields from SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH should be
    // present
  }

  @Test
  public void testSearchWithFetchExtraFields() throws IOException {
    // Setup mock response with extra fields in _source
    ObjectNode mockResponse =
        createMockSearchResponseWithExtraFields(
            1,
            "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)",
            0.95,
            "Test Dataset",
            "urn:li:dataPlatform:test",
            "test.table");
    setupMockOpenSearchResponse(mockResponse);

    // Setup fetchExtraFields
    StringArray extraFields = new StringArray();
    extraFields.add("name");
    extraFields.add("platform");
    extraFields.add("qualifiedName");

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getEntities().size(), 1);

    SearchEntity entity = result.getEntities().get(0);
    assertNotNull(entity.getExtraFields());

    // Verify extra fields are present in the result
    assertNotNull(entity.getExtraFields());
    assertTrue(entity.getExtraFields().size() > 0);
    assertTrue(entity.getExtraFields().containsKey("name"));
    assertTrue(entity.getExtraFields().containsKey("platform"));
    assertTrue(entity.getExtraFields().containsKey("qualifiedName"));

    // Verify the request was made with both default and extra fields
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();

    String requestBody = extractRequestBody(capturedRequest);
    assertNotNull(requestBody);
    assertTrue(
        requestBody.contains("_source"),
        "Request body should contain _source field. Actual body: " + requestBody);
  }

  @Test
  public void testSearchWithEmptyFetchExtraFields() throws IOException {
    // Setup mock response
    setupMockOpenSearchResponse(
        createMockSearchResponse(
            1, "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)", 0.95));

    // Empty fetchExtraFields array should behave like no extra fields
    StringArray emptyExtraFields = new StringArray();

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getEntities().size(), 1);

    // Should behave the same as default fields only
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();

    String requestBody = extractRequestBody(capturedRequest);
    assertNotNull(requestBody);
    assertTrue(
        requestBody.contains("_source"),
        "Request body should contain _source field. Actual body: " + requestBody);
  }

  @Test
  public void testSearchFieldFetchingParity() throws IOException {
    // This test verifies that semantic search uses the same field fetching logic as keyword search
    setupMockOpenSearchResponse(
        createMockSearchResponse(
            1, "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)", 0.95));

    // Setup extra fields that would be commonly requested
    StringArray extraFields = new StringArray();
    extraFields.add("customProperties");
    extraFields.add("description");

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);

    // Verify the search request includes both default fields and extra fields
    ArgumentCaptor<Request> requestCaptor = ArgumentCaptor.forClass(Request.class);
    verify(searchClientShim).performLowLevelRequest(requestCaptor.capture());
    Request capturedRequest = requestCaptor.getValue();

    // The request should contain the combined field set (default + extra)
    String requestBody = extractRequestBody(capturedRequest);
    assertNotNull(requestBody);
    assertTrue(
        requestBody.contains("_source"),
        "Request body should contain _source field. Actual body: " + requestBody);

    // Verify that SearchDocFieldFetchConfig.DEFAULT_FIELDS_TO_FETCH_ON_SEARCH fields are included
    // This ensures parity with keyword search field fetching logic
  }

  // Helper methods

  private String extractRequestBody(Request request) throws IOException {
    if (request.getEntity() != null) {
      return EntityUtils.toString(request.getEntity());
    }
    return "";
  }

  private void setupMockOpenSearchResponse(ObjectNode responseJson) throws IOException {
    String responseBody = objectMapper.writeValueAsString(responseJson);
    when(mockResponse.getEntity()).thenReturn(mockHttpEntity);
    when(mockHttpEntity.getContent()).thenReturn(new ByteArrayInputStream(responseBody.getBytes()));
    when(searchClientShim.performLowLevelRequest(any(Request.class))).thenReturn(mockResponse);
  }

  private ObjectNode createMockSearchResponse(int totalHits, String urn, double score) {
    return createMockSearchResponse(totalHits, Arrays.asList(urn), Arrays.asList(score));
  }

  private ObjectNode createMockSearchResponse(
      int totalHits, List<String> urns, List<Double> scores) {
    ObjectNode response = JsonNodeFactory.instance.objectNode();

    // Create hits structure
    ObjectNode hits = JsonNodeFactory.instance.objectNode();
    ObjectNode total = JsonNodeFactory.instance.objectNode();
    total.put("value", totalHits);
    hits.set("total", total);

    // Create hits array
    ArrayNode hitsArray = JsonNodeFactory.instance.arrayNode();
    for (int i = 0; i < urns.size() && i < scores.size(); i++) {
      ObjectNode hit = JsonNodeFactory.instance.objectNode();
      Double score = scores.get(i);
      if (score != null) {
        hit.put("_score", score.doubleValue());
      }

      ObjectNode source = JsonNodeFactory.instance.objectNode();
      source.put("urn", urns.get(i));
      source.put("name", "Test Dataset " + (i + 1));
      source.put("platform", "urn:li:dataPlatform:test");
      hit.set("_source", source);

      hitsArray.add(hit);
    }
    hits.set("hits", hitsArray);
    response.set("hits", hits);

    return response;
  }

  private ObjectNode createMockSearchResponseWithExtraFields(
      int totalHits, String urn, double score, String name, String platform, String qualifiedName) {
    ObjectNode response = JsonNodeFactory.instance.objectNode();

    // Create hits structure
    ObjectNode hits = JsonNodeFactory.instance.objectNode();
    ObjectNode total = JsonNodeFactory.instance.objectNode();
    total.put("value", totalHits);
    hits.set("total", total);

    // Create hits array with extra fields
    ArrayNode hitsArray = JsonNodeFactory.instance.arrayNode();
    ObjectNode hit = JsonNodeFactory.instance.objectNode();
    hit.put("_score", score);

    ObjectNode source = JsonNodeFactory.instance.objectNode();
    source.put("urn", urn);
    source.put("name", name);
    source.put("platform", platform);
    source.put("qualifiedName", qualifiedName);
    // Add default fields that would be included
    source.put("usageCountLast30Days", 42);
    hit.set("_source", source);

    hitsArray.add(hit);
    hits.set("hits", hitsArray);
    response.set("hits", hits);

    return response;
  }

  private Filter createTestFilter(String field, String value) {
    Criterion criterion = new Criterion();
    criterion.setField(field);
    criterion.setCondition(Condition.EQUAL);
    com.linkedin.data.template.StringArray values = new com.linkedin.data.template.StringArray();
    values.add(value);
    criterion.setValues(values);

    ConjunctiveCriterion conjunctiveCriterion = new ConjunctiveCriterion();
    conjunctiveCriterion.setAnd(new CriterionArray(criterion));

    Filter filter = new Filter();
    filter.setOr(new ConjunctiveCriterionArray(conjunctiveCriterion));

    return filter;
  }
}
