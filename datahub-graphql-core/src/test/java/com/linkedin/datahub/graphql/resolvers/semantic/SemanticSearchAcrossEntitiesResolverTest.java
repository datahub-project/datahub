/**
 * SAAS-SPECIFIC: This test is part of the semantic search feature exclusive to DataHub SaaS. It
 * should NOT be merged back to the open-source DataHub repository. Dependencies: Tests
 * SemanticSearchAcrossEntitiesResolver functionality.
 */
package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
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
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for SemanticSearchAcrossEntitiesResolver */
public class SemanticSearchAcrossEntitiesResolverTest {

  private SemanticSearchService mockSemanticSearchService;
  private ViewService mockViewService;
  private FormService mockFormService;
  private EntityClient mockEntityClient;
  private DataFetchingEnvironment mockEnvironment;
  private QueryContext mockQueryContext;
  private OperationContext mockOperationContext;
  private SemanticSearchAcrossEntitiesResolver resolver;

  @BeforeMethod
  public void setup() {
    mockSemanticSearchService = mock(SemanticSearchService.class);
    mockViewService = mock(ViewService.class);
    mockFormService = mock(FormService.class);
    mockEntityClient = mock(EntityClient.class);
    mockEnvironment = mock(DataFetchingEnvironment.class);
    mockQueryContext = getMockAllowContext();
    mockOperationContext = mock(OperationContext.class);

    when(mockQueryContext.getOperationContext()).thenReturn(mockOperationContext);
    when(mockOperationContext.withSearchFlags(any())).thenReturn(mockOperationContext);
    when(mockEnvironment.getContext()).thenReturn(mockQueryContext);

    resolver =
        new SemanticSearchAcrossEntitiesResolver(
            mockSemanticSearchService, mockViewService, mockFormService, mockEntityClient);
  }

  @Test
  public void testSemanticSearchAcrossEntities() throws Exception {
    // Given: Multi-entity semantic search input
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Arrays.asList(EntityType.DATASET, EntityType.CHART));
    input.setQuery("customer analytics dashboard");
    input.setStart(0);
    input.setCount(15);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity()
                        .setEntity(
                            Urn.createFromString(
                                "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/customers,PROD)"))
                        .setScore(0.92),
                    new SearchEntity()
                        .setEntity(
                            Urn.createFromString(
                                "urn:li:chart:(looker,analytics.customer_dashboard)"))
                        .setScore(0.87)))
            .setFrom(0)
            .setPageSize(15)
            .setNumEntities(2)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    SearchResults result = resultFuture.get();

    // Then: Verify the result
    assertNotNull(result);
    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 15);
    assertEquals(result.getTotal(), 2);
    assertNotNull(result.getSearchResults());
    assertEquals(result.getSearchResults().size(), 2);

    // Verify service was called correctly
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            eq(Arrays.asList("dataset", "chart")),
            eq("customer analytics dashboard"),
            any(),
            anyList(),
            eq(0),
            eq(15),
            anyList()); // facets
  }

  @Test
  public void testSemanticSearchSingleEntityType() throws Exception {
    // Given: Single entity type search (should still work)
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("machine learning");
    input.setStart(10);
    input.setCount(5);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setFrom(10)
            .setPageSize(5)
            .setNumEntities(0)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    SearchResults result = resultFuture.get();

    // Then: Verify the result
    assertNotNull(result);
    assertEquals(result.getStart(), 10);
    assertEquals(result.getCount(), 5);

    // Verify service was called with single entity type
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            eq(Collections.singletonList("dataset")),
            eq("machine learning"),
            any(),
            anyList(),
            eq(10),
            eq(5),
            anyList());
  }

  @Test
  public void testSemanticSearchNoEntityTypes() throws Exception {
    // Given: Empty entity types (should search across all)
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.emptyList());
    input.setQuery("analytics");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(0)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    SearchResults result = resultFuture.get();

    // Then: Should handle gracefully
    assertNotNull(result);

    // Verify service was called with all searchable entity types (default when none specified)
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(), // Should be all searchable entity types, not empty list
            eq("analytics"),
            any(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
  }

  @Test
  public void testSemanticSearchDisabledIsMappedToBadRequest() throws Exception {
    // Given: Semantic search input
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("test query");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // And: Service throws SemanticSearchDisabledException
    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
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
  public void testHandlesRuntimeException() throws Exception {
    // Given: Search input
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("test");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // And: Service throws RuntimeException
    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenThrow(new RuntimeException("Search service error"));

    // When/Then: Should propagate as RuntimeException
    CompletionException ex =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnvironment).join());
    assertTrue(ex.getCause() instanceof RuntimeException);
    assertTrue(ex.getCause().getMessage().contains("Failed to execute semantic search"));
  }

  @Test
  public void testDefaultPagination() throws Exception {
    // Given: Search input without pagination parameters
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("data");
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

    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    resultFuture.get();

    // Then: Should use default pagination
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("data"),
            any(),
            anyList(),
            eq(0), // Default start
            eq(10), // Default count
            anyList());
  }

  @Test
  public void testQuerySanitization() throws Exception {
    // Given: Search input with forward slashes (should be escaped)
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("path/to/data");
    input.setStart(0);
    input.setCount(10);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(new SearchEntityArray())
            .setFrom(0)
            .setPageSize(10)
            .setNumEntities(0)
            .setMetadata(new SearchResultMetadata());

    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList()))
        .thenReturn(mockSearchResult);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    resultFuture.get();

    // Then: Query should be sanitized (forward slashes escaped)
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("path\\/to\\/data"), // Forward slashes should be escaped
            any(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
  }
}
