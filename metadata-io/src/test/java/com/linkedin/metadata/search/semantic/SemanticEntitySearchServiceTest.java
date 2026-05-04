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
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchRequest;
import com.linkedin.metadata.utils.elasticsearch.shim.KnnSearchResponse;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SearchContext;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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
  public void setUp() throws IOException {
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

    // Default: return empty KnnSearchResponse so tests without specific setup don't NPE
    when(searchClientShim.searchKnn(any(KnnSearchRequest.class)))
        .thenReturn(new KnnSearchResponse(List.of()));

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
    // Setup mock response via searchKnn shim
    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)"), List.of(0.95));

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

    // Verify searchKnn was called with the correct index name
    ArgumentCaptor<KnnSearchRequest> requestCaptor =
        ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(searchClientShim).searchKnn(requestCaptor.capture());
    KnnSearchRequest capturedRequest = requestCaptor.getValue();
    assertTrue(
        capturedRequest.indexName().contains(TEST_SEMANTIC_INDEX),
        "KnnSearchRequest indexName should contain the semantic index. Got: "
            + capturedRequest.indexName());
  }

  @Test
  public void testSearchWithFilters() throws IOException {
    // Setup entity spec for field types
    when(mockEntityRegistry.getEntitySpec(TEST_ENTITY_NAME)).thenReturn(mockEntitySpec);

    // Setup mock response
    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:hive,test.table,PROD)"), List.of(0.85));

    // Create a filter
    Filter filter = createTestFilter("platform", "urn:li:dataPlatform:hive");

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, filter, null, 0, 5);

    assertNotNull(result);
    assertEquals(result.getPageSize().intValue(), 5);

    // Verify searchKnn was called
    verify(searchClientShim).searchKnn(any(KnnSearchRequest.class));
  }

  @Test
  public void testSearchPagination() throws IOException {
    // Setup mock response with multiple hits
    setupMockKnnResponse(
        Arrays.asList(
            "urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,table2,PROD)",
            "urn:li:dataset:(urn:li:dataPlatform:test,table3,PROD)"),
        Arrays.asList(0.95, 0.90, 0.85));

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
    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)"), List.of(0.95));

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
    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)"), List.of(0.95));

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, null);

    assertNotNull(result);
    assertEquals(result.getPageSize().intValue(), 10); // Default page size
  }

  @Test
  public void testSearchKnnIOException() throws IOException {
    when(searchClientShim.searchKnn(any(KnnSearchRequest.class)))
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
    // Setup mock response with invalid URN — should be skipped
    setupMockKnnResponse(List.of("invalid-urn"), List.of(0.95));

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

    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)"), List.of(0.95));

    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList("dataset", "chart"), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);

    // Verify searchKnn was called with comma-joined index names
    ArgumentCaptor<KnnSearchRequest> requestCaptor =
        ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(searchClientShim).searchKnn(requestCaptor.capture());
    String indexName = requestCaptor.getValue().indexName();
    assertTrue(indexName.contains("datasetindex_v2_semantic"), "indexName must include dataset");
    assertTrue(indexName.contains("chartindex_v2_semantic"), "indexName must include chart");
  }

  @Test
  public void testSearchEntityRegistryException() throws IOException {
    // Setup entity registry to throw exception
    when(mockEntityRegistry.getEntitySpec(TEST_ENTITY_NAME))
        .thenThrow(new RuntimeException("Entity spec not found"));

    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,table1,PROD)"), List.of(0.95));

    // Should continue with empty field types
    SearchResult result =
        service.search(
            mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    assertNotNull(result);
    assertEquals(result.getEntities().size(), 1);
  }

  @Test
  public void testSearchWithFetchExtraFields() throws IOException {
    // Setup mock response with extra fields in source
    Map<String, Object> source =
        Map.of(
            "urn", "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)",
            "name", "Test Dataset",
            "platform", "urn:li:dataPlatform:test",
            "qualifiedName", "test.table",
            "usageCountLast30Days", 42);

    KnnSearchResponse response =
        new KnnSearchResponse(
            List.of(
                new KnnSearchResponse.Hit(
                    "urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)", 0.95, source)));
    when(searchClientShim.searchKnn(any(KnnSearchRequest.class))).thenReturn(response);

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
    assertTrue(entity.getExtraFields().size() > 0);
    assertTrue(entity.getExtraFields().containsKey("name"));
    assertTrue(entity.getExtraFields().containsKey("platform"));
    assertTrue(entity.getExtraFields().containsKey("qualifiedName"));
  }

  @Test
  public void testSearchFieldFetchingPassedToShim() throws IOException {
    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)"), List.of(0.95));

    service.search(mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    ArgumentCaptor<KnnSearchRequest> requestCaptor =
        ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(searchClientShim).searchKnn(requestCaptor.capture());

    // fieldsToFetch must not be empty — default fields should always be populated
    assertTrue(
        !requestCaptor.getValue().fieldsToFetch().isEmpty(),
        "fieldsToFetch must not be empty; default fields should be included");
  }

  @Test
  public void testSearchRoutedThroughSearchKnn() throws IOException {
    // This test asserts that SemanticEntitySearchService calls searchClientShim.searchKnn(...)
    // — never performLowLevelRequest — so ES 8 and OS 2 both receive the correct query format.
    setupMockKnnResponse(
        List.of("urn:li:dataset:(urn:li:dataPlatform:test,test.table,PROD)"), List.of(0.9));

    service.search(mockOpContext, Arrays.asList(TEST_ENTITY_NAME), TEST_QUERY, null, null, 0, 10);

    ArgumentCaptor<KnnSearchRequest> requestCaptor =
        ArgumentCaptor.forClass(KnnSearchRequest.class);
    verify(searchClientShim).searchKnn(requestCaptor.capture());

    KnnSearchRequest req = requestCaptor.getValue();
    assertEquals(req.indexName(), TEST_SEMANTIC_INDEX, "indexName must be the semantic index");
    assertEquals(req.vectorField(), "embeddings.text_embedding_3_large.chunks.vector");
    assertEquals(req.queryVector(), TEST_EMBEDDING);
    assertTrue(req.k() >= 1, "k must be positive");
  }

  // -------------------------------------------------------------------------
  // Helper methods
  // -------------------------------------------------------------------------

  private void setupMockKnnResponse(List<String> urns, List<Double> scores) throws IOException {
    List<KnnSearchResponse.Hit> hits = new java.util.ArrayList<>();
    for (int i = 0; i < urns.size() && i < scores.size(); i++) {
      Map<String, Object> source =
          Map.of(
              "urn",
              urns.get(i),
              "name",
              "Test Dataset " + (i + 1),
              "platform",
              "urn:li:dataPlatform:test");
      hits.add(new KnnSearchResponse.Hit(urns.get(i), scores.get(i), source));
    }
    when(searchClientShim.searchKnn(any(KnnSearchRequest.class)))
        .thenReturn(new KnnSearchResponse(hits));
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
