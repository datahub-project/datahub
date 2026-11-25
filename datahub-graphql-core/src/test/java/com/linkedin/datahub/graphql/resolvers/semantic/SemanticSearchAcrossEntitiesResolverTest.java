package com.linkedin.datahub.graphql.resolvers.semantic;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
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
import com.linkedin.data.template.StringArray;
import com.linkedin.data.template.StringMap;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLErrorCode;
import com.linkedin.datahub.graphql.exception.DataHubGraphQLException;
import com.linkedin.datahub.graphql.exception.ValidationException;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FacetMetadata;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchFlags;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SearchSortInput;
import com.linkedin.datahub.graphql.generated.SortCriterion;
import com.linkedin.datahub.graphql.generated.SortOrder;
import com.linkedin.datahub.graphql.resolvers.ResolverUtils;
import com.linkedin.datahub.graphql.resolvers.search.DefaultEntityFiltersUtil;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.CriterionArray;
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
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateFailClosedException;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMetrics;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateNoAnchorException;
import com.linkedin.metadata.service.FormService;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import com.linkedin.view.DataHubViewType;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.mockito.ArgumentCaptor;
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
    mockOperationContext = mockQueryContext.getOperationContext();

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

    // Verify service was called with configured default searchable entity types
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            argThat(types -> types != null && !types.isEmpty()),
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
  public void testQueryIsPassedUnescaped() throws Exception {
    // Given: Search input with forward slashes. Keyword resolvers escape "/" for query_string, but
    // the semantic query is only embedded, so it must reach the service exactly as typed.
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

    // Then: Query reaches the service unescaped
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("path/to/data"),
            any(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
  }

  @Test
  public void testBlankQueryIsRejectedBeforeSearch() {
    // Given: a whitespace-only query. Under the classical provider it would embed to the
    // empty-text sentinel vector and kNN would return arbitrary neighbours.
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("   ");

    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // When/Then: rejected as bad input before view resolution or any service call
    org.testng.Assert.expectThrows(
        IllegalArgumentException.class, () -> resolver.get(mockEnvironment));
    verifyNoInteractions(mockSemanticSearchService, mockViewService, mockEntityClient);
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
                                    .setValues(new StringArray("snowflake"))
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
  public void testStructuredPropertyFacetsScopedToSearchedEntityTypes() throws Exception {
    // Given: dataset-only search with structured property facets enabled
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DATASET));
    input.setQuery("test");
    input.setStart(0);
    input.setCount(10);
    SearchFlags flags = new SearchFlags();
    flags.setIncludeStructuredPropertyFacets(true);
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // Two properties: one scoped to glossary terms only, one scoped to datasets
    SearchEntity termScopedSp =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:structuredProperty:termScoped"))
            .setExtraFields(
                new StringMap(
                    java.util.Map.of(
                        "entityTypes", "[\"urn:li:entityType:datahub.glossaryTerm\"]")));
    SearchEntity datasetScopedSp =
        new SearchEntity()
            .setEntity(UrnUtils.getUrn("urn:li:structuredProperty:datasetScoped"))
            .setExtraFields(
                new StringMap(
                    java.util.Map.of("entityTypes", "[\"urn:li:entityType:datahub.dataset\"]")));
    SearchResult structuredPropResult =
        new SearchResult()
            .setEntities(new SearchEntityArray(termScopedSp, datasetScopedSp))
            .setFrom(0)
            .setPageSize(1000)
            .setNumEntities(2)
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

    // When
    resolver.get(mockEnvironment).get();

    // Then: only the dataset-scoped property survives as a facet; the glossary-term-only
    // property is filtered out.
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            anyList(),
            eq("test"),
            any(),
            anyList(),
            eq(0),
            eq(10),
            eq(Collections.singletonList("urn:li:structuredProperty:datasetScoped")));
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

    // Then: Should call semantic search with configured default searchable entity types
    verify(mockSemanticSearchService, times(1))
        .semanticSearchAcrossEntities(
            any(OperationContext.class),
            argThat(types -> types != null && !types.isEmpty()),
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

  @Test
  public void testTheTemplateSwitchServesTheTemplateWithTheDocumentDefaults() throws Exception {
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("quarterly revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    // The template's page facets a default filter field, which the response leaves out
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(0)
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
            eq("quarterly revenue"),
            filter.capture(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
    verify(mockSemanticSearchService, never())
        .semanticSearchAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());
    // The document defaults keyword search adds, and no caller filter of its own
    assertEquals(
        filter.getValue(),
        DefaultEntityFiltersUtil.applyDefaultEntityFilters(
            null, Collections.singletonList("document"), null, mockQueryContext));
    assertFalse(searching.getValue().getSearchContext().getCallerFiltered());
    // A URN query the template routes to similarity holds its anchor to the same defaults
    assertEquals(searching.getValue().getSearchContext().getAnchorFilter(), filter.getValue());
    assertEquals(
        result.getFacets().stream().map(FacetMetadata::getField).toList(),
        Collections.singletonList("platform"));
  }

  @Test
  public void testWithTheSwitchOffSemanticSearchKeepsItsLegacyPath() throws Exception {
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("quarterly revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.semanticSearchAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(0)
                .setMetadata(new SearchResultMetadata()));

    resolver.get(mockEnvironment).get();

    // The caller's filter alone: the legacy search adds no document defaults
    verify(mockSemanticSearchService)
        .semanticSearchAcrossEntities(
            any(),
            anyList(),
            eq("quarterly revenue"),
            isNull(),
            anyList(),
            eq(0),
            eq(10),
            anyList());
    verify(mockSemanticSearchService, never())
        .semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());
  }

  @Test
  public void testMinSimilarityReachesTheTemplateAndTheLegacySearchRejectsIt() throws Exception {
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("quarterly revenue");
    SearchFlags flags = new SearchFlags();
    flags.setMinSimilarity(0.8f);
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);

    // The legacy kNN search has no raw cosine floor, so it refuses one rather than ignore it
    org.testng.Assert.expectThrows(ValidationException.class, () -> resolver.get(mockEnvironment));
    verify(mockSemanticSearchService, never())
        .semanticSearchAcrossEntities(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());

    // The template's vector rung reads it
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(0)
                .setMetadata(new SearchResultMetadata()));
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
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("path/to/revenue");
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    embeddingFails(QueryEmbeddingException.TIMEOUT);

    // By default the error names the reason: a timeout is an exceeded deadline ...
    DataHubGraphQLException timedOut = typedError();
    assertEquals(timedOut.errorCode(), DataHubGraphQLErrorCode.DEADLINE_EXCEEDED);
    assertTrue(timedOut.getMessage().contains(QueryEmbeddingException.TIMEOUT));
    // ... any other failure of the provider stays a server error, still naming the reason ...
    embeddingFails(QueryEmbeddingException.FAILED);
    DataHubGraphQLException failed = typedError();
    assertEquals(failed.errorCode(), DataHubGraphQLErrorCode.SERVER_ERROR);
    assertTrue(failed.getMessage().contains(QueryEmbeddingException.FAILED));
    // ... and a throttle is an unavailable service
    embeddingFails(QueryEmbeddingException.REJECTED);
    assertEquals(typedError().errorCode(), DataHubGraphQLErrorCode.SERVICE_UNAVAILABLE);
    verifyNoInteractions(mockEntityClient);

    // Opted in, keyword search serves the same request, marked with the reason
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    input.setSearchFlags(flags);
    when(mockEntityClient.searchAcrossEntities(
            any(), anyList(), anyString(), any(), anyInt(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        new SearchEntity().setEntity(UrnUtils.getUrn("urn:li:document:revenue"))))
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(1)
                .setMetadata(new SearchResultMetadata()));

    SearchResults result = resolver.get(mockEnvironment).get();

    assertEquals(result.getKeywordFallback(), QueryEmbeddingException.REJECTED);
    assertEquals(result.getTotal(), 1);
    Filter defaults =
        DefaultEntityFiltersUtil.applyDefaultEntityFilters(
            null, Collections.singletonList("document"), null, mockQueryContext);
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
  public void testAFailedKeywordFallbackReportsItsOwnFailure() throws Exception {
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("quarterly revenue");
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    flags.setSearchTemplate("urn:li:searchTemplate:semantic");
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    embeddingFails(QueryEmbeddingException.TIMEOUT);
    when(mockEntityClient.searchAcrossEntities(
            any(), anyList(), anyString(), any(), anyInt(), anyInt(), any(), anyList()))
        .thenThrow(new RuntimeException("keyword search unavailable"));

    // Not the embedding's exceeded deadline, which a caller may retry: the fallback's own failure.
    // The message keeps the embedding's reason.
    DataHubGraphQLException error = typedError();

    assertEquals(error.errorCode(), DataHubGraphQLErrorCode.SERVER_ERROR);
    assertTrue(error.getMessage().contains(QueryEmbeddingException.TIMEOUT));
    // The fallback does not run the template the caller named again
    ArgumentCaptor<OperationContext> fallback = ArgumentCaptor.forClass(OperationContext.class);
    verify(mockEntityClient)
        .searchAcrossEntities(
            fallback.capture(),
            anyList(),
            anyString(),
            any(),
            anyInt(),
            anyInt(),
            any(),
            anyList());
    assertFalse(fallback.getValue().getSearchContext().getSearchFlags().hasSearchTemplate());

    // A fallback that rejected the request is a bad request
    doThrow(
            new RuntimeException(
                "Keyword search failed", new IllegalArgumentException("Unknown filter field")))
        .when(mockEntityClient)
        .searchAcrossEntities(
            any(), anyList(), anyString(), any(), anyInt(), anyInt(), any(), anyList());
    assertEquals(typedError().errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
  }

  @Test
  public void testTheKeywordFallbackIsCountedByReason() throws Exception {
    MetricUtils metrics = mock(MetricUtils.class);
    OperationContext base = mockQueryContext.getOperationContext();
    OperationContext measured =
        base.toBuilder()
            .systemTelemetryContext(
                SystemTelemetryContext.TEST.toBuilder().metricUtils(metrics).build())
            .build(base.getSessionActorContext(), false);
    when(mockQueryContext.getOperationContext()).thenReturn(measured);
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("quarterly revenue");
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    embeddingFails(QueryEmbeddingException.REJECTED);
    when(mockEntityClient.searchAcrossEntities(
            any(), anyList(), anyString(), any(), anyInt(), anyInt(), any(), anyList()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setFrom(0)
                .setPageSize(10)
                .setNumEntities(0)
                .setMetadata(new SearchResultMetadata()));

    resolver.get(mockEnvironment).get();

    verify(metrics)
        .incrementMicrometer(
            SearchTemplateMetrics.KEYWORD_FALLBACK, 1, "reason", QueryEmbeddingException.REJECTED);

    // An embedding service can name any reason; the tag and the result's keywordFallback count
    // one outside the known codes as failed, so their values stay the documented codes
    embeddingFails("tenantQuotaExceeded");
    SearchResults unknownReason = resolver.get(mockEnvironment).get();

    verify(metrics)
        .incrementMicrometer(
            SearchTemplateMetrics.KEYWORD_FALLBACK, 1, "reason", QueryEmbeddingException.FAILED);
    assertEquals(unknownReason.getKeywordFallback(), QueryEmbeddingException.FAILED);
  }

  @Test
  public void testOnlyAQueryEmbeddingFailureFallsBackToKeywordResults() {
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
    input.setQuery("quarterly revenue");
    SearchFlags flags = new SearchFlags();
    flags.setKeywordFallbackOnEmbeddingFailure(true);
    input.setSearchFlags(flags);
    when(mockEnvironment.getArgument("input")).thenReturn(input);
    when(mockSemanticSearchService.isServedByTemplate(any())).thenReturn(true);
    // A failed kNN request, as similarity's failures are too: nothing was left unembedded
    when(mockSemanticSearchService.semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList()))
        .thenThrow(new RuntimeException("kNN unavailable"));

    CompletionException failure =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnvironment).join());

    assertFalse(failure.getCause() instanceof DataHubGraphQLException);
    verifyNoInteractions(mockEntityClient);
  }

  /**
   * On the template path an entity URN routes to similarity. One that cannot anchor the search,
   * such as a URN of a type with no embedding profile, is a bad request, not a server error, and no
   * keyword fallback covers it. Another fail-closed failure is not mapped to it.
   */
  @Test
  public void testAUrnThatCannotAnchorIsABadRequest() throws Exception {
    SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setTypes(Collections.singletonList(EntityType.DOCUMENT));
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

    assertEquals(typedError().errorCode(), DataHubGraphQLErrorCode.BAD_REQUEST);
    verifyNoInteractions(mockEntityClient);

    doThrow(
            new RuntimeException(
                "Search failed", new SearchTemplateFailClosedException("kNN unavailable")))
        .when(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());
    CompletionException other =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnvironment).join());
    assertFalse(other.getCause() instanceof DataHubGraphQLException, other.toString());
  }

  private void embeddingFails(String reason) {
    doThrow(
            new RuntimeException(
                "Vector retrieval failed", new QueryEmbeddingException(reason, null)))
        .when(mockSemanticSearchService)
        .semanticSearchAcrossEntitiesByTemplate(
            any(), anyList(), anyString(), any(), any(), anyInt(), any(), anyList());
  }

  private DataHubGraphQLException typedError() {
    CompletionException failure =
        org.testng.Assert.expectThrows(
            CompletionException.class, () -> resolver.get(mockEnvironment).join());
    assertTrue(failure.getCause() instanceof DataHubGraphQLException, failure.toString());
    return (DataHubGraphQLException) failure.getCause();
  }

  private static AggregationMetadata facet(String name) {
    return new AggregationMetadata()
        .setName(name)
        .setDisplayName(name)
        .setAggregations(new LongMap())
        .setFilterValues(new FilterValueArray());
  }
}
