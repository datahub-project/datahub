package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static com.linkedin.metadata.Constants.DATASET_ENTITY_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.SearchInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticSearchResolver */
public class SemanticSearchResolverTest {

  private SemanticSearchService mockSemanticSearchService;
  private DataFetchingEnvironment mockEnvironment;
  private QueryContext mockQueryContext;
  private OperationContext mockOperationContext;
  private SemanticSearchResolver resolver;

  @BeforeMethod
  public void setup() {
    mockSemanticSearchService = mock(SemanticSearchService.class);
    mockEnvironment = mock(DataFetchingEnvironment.class);
    mockQueryContext = getMockAllowContext();
    mockOperationContext = mock(OperationContext.class);

    when(mockQueryContext.getOperationContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.withSearchFlags(any())).thenReturn(mockOperationContext);
    when(mockEnvironment.getContext()).thenReturn(mockQueryContext);

    resolver = new SemanticSearchResolver(mockSemanticSearchService);
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
  public void testHandlesEmptyQuery() throws Exception {
    // Given: Empty query
    SearchInput input = new SearchInput();
    input.setType(EntityType.DATASET);
    input.setQuery("");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response for empty query
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
    SearchResults result = resultFuture.get();

    // Then: Should handle gracefully
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);

    // Verify service was called with sanitized empty string
    verify(mockSemanticSearchService, times(1))
        .semanticSearch(
            any(OperationContext.class),
            anyList(),
            eq(""), // Empty string should be passed through
            any(),
            anyList(),
            eq(0),
            eq(10));
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
}
