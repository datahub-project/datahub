package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.generated.SortCriterion;
import com.linkedin.datahub.graphql.generated.SortOrder;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.search.SemanticSearchDisabledException;
import com.linkedin.metadata.search.SemanticSearchService;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
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

  @Test
  public void testSemanticSearchWithViewFiltersCombined() throws Exception {
    // Given: Search input with a viewUrn that has filters
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("customer data");
    input.setStart(0);
    input.setCount(10);
    String viewUrnStr = "urn:li:dataHubView:filtered-view";
    input.setViewUrn(viewUrnStr);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock view with a filter
    Filter viewFilter =
        new Filter()
            .setOr(
                new ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new CriterionArray(
                                new Criterion()
                                    .setField("platform")
                                    .setValue("snowflake")
                                    .setCondition(Condition.EQUAL)))));
    DataHubViewInfo viewInfo =
        new DataHubViewInfo()
            .setType(DataHubViewType.PERSONAL)
            .setName("Filtered View")
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(new StringArray(Collections.singletonList("dataset")))
                    .setFilter(viewFilter));
    when(mockViewService.getViewInfo(any(), eq(UrnUtils.getUrn(viewUrnStr)))).thenReturn(viewInfo);

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

    // Then: Verify view was resolved and semantic search was called
    verify(mockViewService, times(1)).getViewInfo(any(), eq(UrnUtils.getUrn(viewUrnStr)));
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(), // Entity types from view
            eq("customer data"),
            any(), // Filter should be combined
            anyList(),
            eq(0),
            eq(10),
            anyList());
  }

  @Test
  public void testSemanticSearchWithOrFilters() throws Exception {
    // Given: Search input with orFilters
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("test");
    input.setStart(0);
    input.setCount(10);

    // Create OR filters
    FacetFilterInput filterInput = new FacetFilterInput();
    filterInput.setField("platform");
    filterInput.setValues(Arrays.asList("snowflake", "databricks"));
    filterInput.setCondition(FilterOperator.EQUAL);

    AndFilterInput andFilter = new AndFilterInput();
    andFilter.setAnd(Collections.singletonList(filterInput));
    input.setOrFilters(Collections.singletonList(andFilter));

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

    // Then: Should call semantic search with filters
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("test"),
            any(Filter.class), // Filter should be non-null
            anyList(),
            eq(0),
            eq(10),
            anyList());
  }

  @Test
  public void testSemanticSearchWithSortInput() throws Exception {
    // Given: Search input with sort criteria
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("analytics");
    input.setStart(0);
    input.setCount(10);

    SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField("name");
    sortCriterion.setSortOrder(SortOrder.ASCENDING);

    SearchSortInput sortInput = new SearchSortInput();
    sortInput.setSortCriteria(Collections.singletonList(sortCriterion));
    input.setSortInput(sortInput);

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

    // Then: Should call semantic search with sort criteria
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("analytics"),
            any(),
            anyList(), // Should contain sort criteria
            eq(0),
            eq(10),
            anyList());
  }

  @Test
  public void testSemanticSearchWithStructuredPropertyFacets() throws Exception {
    // Given: Search input with structured property facets enabled
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("test");
    input.setStart(0);
    input.setCount(10);

    SearchFlags flags = new SearchFlags();
    flags.setIncludeStructuredPropertyFacets(true);
    input.setSearchFlags(flags);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock structured property search result
    SearchResult structuredPropResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity()
                        .setEntity(
                            UrnUtils.getUrn(
                                "urn:li:structuredProperty:io.acryl.privacy.classification"))))
            .setFrom(0)
            .setPageSize(1000)
            .setNumEntities(1)
            .setMetadata(new SearchResultMetadata());

    when(mockEntityClient.searchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyInt(),
            anyInt(),
            any(),
            any()))
        .thenReturn(structuredPropResult);

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

    // Then: Should search for structured properties first
    verify(mockEntityClient, times(1))
        .searchAcrossEntities(
            any(OperationContext.class),
            eq(Collections.singletonList("structuredProperty")),
            eq("*"),
            any(),
            eq(0),
            eq(1000),
            any(),
            any());

    // And then call semantic search with structured property facets
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("test"),
            any(),
            anyList(),
            eq(0),
            eq(10),
            anyList()); // Should include structured property URNs as facets
  }

  @Test
  public void testSemanticSearchWithNullTypes() throws Exception {
    // Given: Search input with null types (should default to all searchable types)
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(null); // null types
    input.setQuery("data");
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

    // Then: Should call semantic search with default searchable entity types
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(), // Should be all searchable entity types
            eq("data"),
            any(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
  }

  @Test
  public void testSemanticSearchViewIntersectionResultsInEmptyEntities() throws Exception {
    // Given: Search input where view entity types don't intersect with input types
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET)); // Only dataset
    input.setQuery("test");
    input.setStart(0);
    input.setCount(10);
    String viewUrnStr = "urn:li:dataHubView:chart-only-view";
    input.setViewUrn(viewUrnStr);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock view that only allows charts (no intersection with dataset)
    DataHubViewInfo viewInfo =
        new DataHubViewInfo()
            .setType(DataHubViewType.PERSONAL)
            .setName("Chart Only View")
            .setDefinition(
                new DataHubViewDefinition()
                    .setEntityTypes(new StringArray(Collections.singletonList("chart")))
                    .setFilter(new Filter()));
    when(mockViewService.getViewInfo(any(), eq(UrnUtils.getUrn(viewUrnStr)))).thenReturn(viewInfo);

    // When: Resolver processes the request
    CompletableFuture<SearchResults> resultFuture = resolver.get(mockEnvironment);
    SearchResults result = resultFuture.get();

    // Then: Should return empty results without calling semantic search
    assertNotNull(result);
    assertEquals(result.getTotal(), 0);
    assertTrue(result.getSearchResults().isEmpty());

    // Semantic search service should NOT be called
    verify(mockSemanticSearchService, never())
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            anyString(),
            any(),
            anyList(),
            anyInt(),
            anyInt(),
            anyList());
  }

  @Test
  public void testSemanticSearchWithAllParameters() throws Exception {
    // Given: Search input with all parameters set
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Arrays.asList(EntityType.DATASET, EntityType.CHART));
    input.setQuery("comprehensive test");
    input.setStart(5);
    input.setCount(25);

    // Filters
    FacetFilterInput filterInput = new FacetFilterInput();
    filterInput.setField("origin");
    filterInput.setValues(Collections.singletonList("PROD"));
    filterInput.setCondition(FilterOperator.EQUAL);

    AndFilterInput andFilter = new AndFilterInput();
    andFilter.setAnd(Collections.singletonList(filterInput));
    input.setOrFilters(Collections.singletonList(andFilter));

    // Sort
    SortCriterion sortCriterion = new SortCriterion();
    sortCriterion.setField("created");
    sortCriterion.setSortOrder(SortOrder.DESCENDING);

    SearchSortInput sortInput = new SearchSortInput();
    sortInput.setSortCriteria(Collections.singletonList(sortCriterion));
    input.setSortInput(sortInput);

    // Search flags
    SearchFlags flags = new SearchFlags();
    flags.setIncludeStructuredPropertyFacets(false);
    input.setSearchFlags(flags);

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Mock service response
    SearchResult mockSearchResult =
        new SearchResult()
            .setEntities(
                new SearchEntityArray(
                    new SearchEntity()
                        .setEntity(
                            Urn.createFromString(
                                "urn:li:dataset:(urn:li:dataPlatform:hdfs,/data/test,PROD)"))
                        .setScore(0.95)))
            .setFrom(5)
            .setPageSize(25)
            .setNumEntities(1)
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

    // Then
    assertNotNull(result);
    assertEquals(result.getStart(), 5);
    assertEquals(result.getCount(), 25);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getSearchResults().size(), 1);

    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            eq(Arrays.asList("dataset", "chart")),
            eq("comprehensive test"),
            any(Filter.class),
            anyList(),
            eq(5),
            eq(25),
            anyList());
  }
}
