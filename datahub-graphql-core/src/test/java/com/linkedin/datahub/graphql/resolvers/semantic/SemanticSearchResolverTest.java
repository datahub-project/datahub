package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.datahub.graphql.TestUtils.getMockDenyContext;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.LongMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.DefaultEntityFiltersUtil;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadata;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.FilterValueArray;
import com.linkedin.metadata.search.QueryEmbeddingException;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateNoAnchorException;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticSearchResolver */
public class SemanticSearchResolverTest {

  private SemanticSearchService mockSemanticSearchService;
  private EntityClient mockEntityClient;
  private DataFetchingEnvironment mockEnvironment;
  private QueryContext mockQueryContext;
  private SemanticSearchResolver resolver;

  @BeforeMethod
  public void setup() {
    mockSemanticSearchService = mock(SemanticSearchService.class);
    mockEntityClient = mock(EntityClient.class);
    mockEnvironment = mock(DataFetchingEnvironment.class);
    // A real operation context: the template path records the filters it adds on it
    mockQueryContext = getMockAllowContext();

    when(mockEnvironment.getContext()).thenReturn(mockQueryContext);

    resolver = new SemanticSearchResolver(mockSemanticSearchService, mockEntityClient);
  }

  @Test
  public void testSemanticSearch() throws Exception {
    // Given: Semantic search input
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("machine learning models");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity()
                        .setEntity(
                            Urn.createFromString(
                                "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/ml/models,PROD)"))
                        .setScore(0.95)))
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(1)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearch(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    SearchResults result = resultFuture.get();

    // Then: Verify the result
    assertNotNull(result);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 10);
    assertEquals(result.getTotal(), 1);
    assertNotNull(result.getSearchResults());
    assertEquals(result.getSearchResults().size(), 1);

    // Verify service was called correctly
    verify(mockSemanticSearchService, times(1))
        .semanticSearch(
            any(OperationContext.class),
            eq(Collections.singletonList(DATASET_ENTITY_NAME)),
            eq("machine learning models"),
            any(),
            eq(Collections.emptyList()),
            eq(0),
            eq(10));
  }

  @Test
  public void testQueryIsPassedUnescaped() throws Exception {
    // Given: a query containing "/". Keyword resolvers escape it for query_string, but the
    // semantic query is only embedded, so it must reach the service exactly as typed.
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("path/to/data");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.semanticSearch(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(0)
                .setMetadata(new SearchResultMetadata()));

    // When
    resolver.get(mockEnvironment).get();

    // Then: Query reaches the service unescaped
    verify(mockSemanticSearchService, times(1))
        .semanticSearch(
            any(OperationContext.class),
            eq(Collections.singletonList(DATASET_ENTITY_NAME)),
            eq("path/to/data"),
            any(),
            eq(Collections.emptyList()),
            eq(0),
            eq(10));
  }

  @Test
  public void testBlankQueryIsRejectedBeforeSearch() {
    // Given: a whitespace-only query. Under the classical provider it would embed to the
    // empty-text sentinel vector and kNN would return arbitrary neighbours.
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("   ");

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // When/Then: rejected as bad input without reaching the service
    org.testng.Assert.expectThrows(
        IllegalArgumentException.class, () -> resolver.get(mockEnvironment));
    verifyNoInteractions(mockSemanticSearchService);
  }

  @Test
  public void testSemanticSearchWithFilters() throws Exception {
    // Given: Semantic search input with filters
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("customer data");
    input.setStart(5);
    input.setCount(20);
    // Note: filters would be set here if testing filter functionality

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setFrom(5)
            .setPageSize(20)
            .setNumEntities(0)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearch(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    SearchResults result = resultFuture.get();

    // Then: Verify the result
    assertNotNull(result);
    assertEquals(result.getStart(), 5);
    assertEquals(result.getCount(), 20);

    // Verify service was called with correct parameters
    verify(mockSemanticSearchService, times(1))
        .semanticSearch(
            any(OperationContext.class),
            eq(Collections.singletonList(DATASET_ENTITY_NAME)),
            eq("customer data"),
            any(),
            eq(Collections.emptyList()),
            eq(5),
            eq(20));
  }

  @Test
  public void testSemanticSearchDisabledIsMappedToBadRequest() throws Exception {
    // Given: Semantic search input
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("test query");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // And: Service throws SemanticSearchDisabledException
    when(mockSemanticSearchService.semanticSearch(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenThrow(new SemanticSearchDisabledException());

    // When/Then: Resolver maps to DataHubGraphQLException with BAD_REQUEST
    CompletionException ex =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnvironment).join());
    assertTrue(ex.getCause() instanceof DataHubGraphQLException);
    assertEquals(
        ((DataHubGraphQLException) ex.getCause()).errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
  }

  @Test
  public void testDefaultPagination() throws Exception {
    // Given: Search input without pagination parameters
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("analytics");
    // start and count are null - should use defaults

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(0)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearch(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    resultFuture.get();

    // Then: Should use default pagination
    verify(mockSemanticSearchService, times(1))
        .semanticSearch(
            any(OperationContext.class),
            anyList(),
            eq("analytics"),
            any(),
            anyList(),
            eq(0), // Default start
            eq(10)); // Default count
  }

  @Test
  public void testWithTheSwitchOffSemanticSearchKeepsItsLegacyPath() throws Exception {
    SearchInput input = new SearchInput();
    input.setType(EntityType.DOCUMENT);
    input.setQuery("quarterly revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.semanticSearch(
            any(), anyList(), anyString(), any(), anyList(), anyInt(), anyInt()))
        .thenReturn(emptyResult());

    resolver.get(mockEnvironment).get();

    // The caller's filter alone: the legacy search adds no document defaults
    verify(mockSemanticSearchService)
        .semanticSearch(
            any(),
            eq(Collections.singletonList("document")),
            eq("quarterly revenue"),
            isNull(),
            anyList(),
            eq(0),
            eq(10));
    verify(mockSemanticSearchService, never())
        .semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());
  }

  @Test
  public void testTheTemplateSwitchServesTheTemplateWithTheDocumentDefaults() throws Exception {
    SearchInput input = new SearchInput();
    input.setType(EntityType.DOCUMENT);
    input.setQuery("path/to/revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    // The template's page facets a default filter field, which the response leaves out
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(
            emptyResult()
                .setMetadata(
                    new SearchResultMetadata()
                        .setAggregations(
                            new AggregationMetadataArray(
                                facet("showInGlobalContext"), facet("platform")))));

    SearchResults result = resolver.get(mockEnvironment).get();

    ArgumentCaptor<OperationContext> searching = ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    verify(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            searching.capture(),
            eq(Collections.singletonList("document")),
            eq("path/to/revenue"),
            filter.capture(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
    verify(mockSemanticSearchService, never())
        .semanticSearch(any(), anyList(), anyString(), any(), anyList(), anyInt(), anyInt());
    // The document defaults keyword search adds, and no caller filter of its own. This caller can
    // manage documents, so unpublished documents without a lifecycle stage stay in
    Filter defaults = documentDefaults();
    assertEquals(filter.getValue(), defaults);
    assertFalse(excludesUnpublished(filter.getValue()));
    assertFalse(searching.getValue().getSearchContext().getCallerFiltered());
    // A URN query the template routes to similarity holds its anchor to the same defaults
    assertEquals(searching.getValue().getSearchContext().getAnchorFilter(), defaults);
    assertEquals(
        result.getFacets().stream().map(FacetMetadata::getField).toList(),
        Collections.singletonList("platform"));
  }

  @Test
  public void testAUserWhoCannotManageDocumentsGetsNoUnpublishedDocument() throws Exception {
    QueryContext cannotManageDocuments = getMockDenyContext();
    when(mockEnvironment.getContext()).thenReturn(cannotManageDocuments);
    SearchInput input = new SearchInput();
    input.setType(EntityType.DOCUMENT);
    input.setQuery("quarterly revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(emptyResult());

    resolver.get(mockEnvironment).get();

    ArgumentCaptor<OperationContext> searching = ArgumentCaptor.forClass(OperationContext.class);
    ArgumentCaptor<Filter> filter = ArgumentCaptor.forClass(Filter.class);
    verify(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            searching.capture(),
            anyList(),
            anyString(),
            filter.capture(),
            anyList(),
            anyInt(),
            any(),
            anyList());
    // Neither a result nor an anchor
    assertTrue(excludesUnpublished(filter.getValue()), String.valueOf(filter.getValue()));
    assertTrue(
        excludesUnpublished(searching.getValue().getSearchContext().getAnchorFilter()),
        String.valueOf(searching.getValue().getSearchContext().getAnchorFilter()));
  }

  @Test
  public void testAnotherEntityTypeStillHoldsADocumentAnchorToTheDocumentDefaults()
      throws Exception {
    // A dataset search adds no document defaults, but its URN query can name a document anchor
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("urn:li:document:revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(emptyResult());

    resolver.get(mockEnvironment).get();

    ArgumentCaptor<OperationContext> searching = ArgumentCaptor.forClass(OperationContext.class);
    verify(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            searching.capture(),
            eq(Collections.singletonList(DATASET_ENTITY_NAME)),
            eq("urn:li:document:revenue"),
            isNull(),
            anyList(),
            anyInt(),
            any(),
            anyList());
    assertEquals(searching.getValue().getSearchContext().getAnchorFilter(), documentDefaults());
  }

  @Test
  public void testMinSimilarityReachesTheTemplateAndTheLegacySearchRejectsIt() throws Exception {
    SearchInput input = new SearchInput();
    input.setType(EntityType.DOCUMENT);
    input.setQuery("quarterly revenue");
    SearchFlags flags = new SearchFlags();
    flags.setMinSimilarity(0.8f);
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // The legacy kNN search has no raw cosine floor, so it refuses one rather than ignore it
    org.testng.Assert.expectThrows(ValidationException.class, () -> resolver.get(mockEnvironment));
    verify(mockSemanticSearchService, never())
        .semanticSearch(any(), anyList(), anyString(), any(), anyList(), anyInt(), anyInt());

    // The template's vector rung reads it
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(emptyResult());
    resolver.get(mockEnvironment).get();
    ArgumentCaptor<OperationContext> searching = ArgumentCaptor.forClass(OperationContext.class);
    verify(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            searching.capture(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());
    assertEquals(searching.getValue().getSearchContext().getSearchFlags().getMinSimilarity(), 0.8f);
  }

  @Test
  public void testAQueryEmbeddingFailureIsATypedErrorUnlessTheCallerOptsIntoKeywordResults()
      throws Exception {
    SearchInput input = new SearchInput();
    input.setType(EntityType.DOCUMENT);
    input.setQuery("path/to/revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    doThrow(
            new RuntimeException(
                "Vector retrieval failed",
                new QueryEmbeddingException(QueryEmbeddingException.TIMEOUT, null)))
        .when(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());

    // By default the error names the reason
    DataHubGraphQLException timedOut = typedError();
    assertEquals(timedOut.errorCode(), DataHubGraphQLErrorCode.DEADLINE_EXCEEDED);
    assertTrue(timedOut.getMessage().contains(QueryEmbeddingException.TIMEOUT));
    verifyNoInteractions(mockEntityClient);

    // Opted in, keyword search over the same entity type serves the same request, marked with the
    // reason
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    input.setSearchFlags(flags);
    when(mockEntityClient.searchAcrossEntities(
            any(), anyList(), anyString(), any(), anyInt(), anyInt(), any(), anyList()))
        .thenReturn(
            emptyResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:document:revenue"))))
                .setNumEntities(1));

    SearchResults result = resolver.get(mockEnvironment).get();

    assertEquals(result.getKeywordFallback(), QueryEmbeddingException.TIMEOUT);
    assertEquals(result.getTotal(), 1);
    Filter defaults = documentDefaults();
    verify(mockEntityClient)
        .searchAcrossEntities(
            any(),
            eq(Collections.singletonList("document")),
            eq(ResolverUtils.escapeForwardSlash("path/to/revenue")),
            eq(defaults),
            eq(0),
            eq(10),
            anyList(),
            anyList());
  }

  @Test
  public void testAUrnThatCannotAnchorIsABadRequest() {
    SearchInput input = new SearchInput();
    input.setType(EntityType.DOCUMENT);
    input.setQuery("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    doThrow(
            new RuntimeException(
                "Search failed",
                new SearchTemplateNoAnchorException(
                    "Search template urn:li:searchTemplate:similarity finds similar entities")))
        .when(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());

    // The URN routes to similarity, and an anchor it cannot use is the caller's error, which no
    // keyword fallback covers
    assertEquals(typedError().errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    verifyNoInteractions(mockEntityClient);
  }

  private DataHubGraphQLException typedError() {
    CompletionException failure =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnvironment).join());
    assertTrue(failure.getCause() instanceof DataHubGraphQLException, failure.toString());
    return (DataHubGraphQLException) failure.getCause();
  }

  /** Whether a clause of {@code filter} leaves out documents whose state is UNPUBLISHED. */
  private static boolean excludesUnpublished(Filter filter) {
    return filter.getOr().stream()
        .flatMap(clause -> clause.getAnd().stream())
        .anyMatch(
            criterion ->
                "state".equals(criterion.getField())
                    && Boolean.TRUE.equals(criterion.isNegated())
                    && criterion.getValues().contains("UNPUBLISHED"));
  }

  /** The document defaults keyword search adds, for this caller. */
  private Filter documentDefaults() {
    return DefaultEntityFiltersUtil.applyDefaultEntityFilters(
        null, Collections.singletonList("document"), null, mockQueryContext);
  }

  private static SearchResult emptyResult() {
    return new SearchResult()
        .setEntities(new SearchEntityArray())
        .setFrom(0)
        .setPageSize(10)
        .setNumEntities(0)
        .setMetadata(new SearchResultMetadata());
  }

  private static AggregationMetadata facet(String name) {
    return new AggregationMetadata()
        .setName(name)
        .setDisplayName(name)
        .setAggregations(new LongMap())
        .setFilterValues(new FilterValueArray());
  }
}
