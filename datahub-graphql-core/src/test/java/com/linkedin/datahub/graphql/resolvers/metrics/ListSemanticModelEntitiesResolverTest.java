package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.AndFilterInput;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.FilterOperator;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ListSemanticModelEntitiesResolverTest {

  private static final String TEST_SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.model,sales_model)";

  private EntityClient _entityClient;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private ListSemanticModelEntitiesResolver _resolver;
  private SemanticModel _entity;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _entity = new SemanticModel();
    _entity.setUrn(TEST_SEMANTIC_MODEL_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(_entity);
    _resolver = new ListSemanticModelEntitiesResolver(_entityClient);
  }

  @Test
  public void testGetFiltersBySemanticModelField() throws Exception {
    final SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(2);
    mockSearchResult.setMetadata(new SearchResultMetadata());
    mockSearchResult.setEntities(new SearchEntityArray());

    final SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("*");
    input.setStart(0);
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(null);

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(List.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME)),
                eq("*"),
                any(),
                eq(0),
                eq(10),
                isNull()))
        .thenReturn(mockSearchResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final SearchResults result = _resolver.get(_dataFetchingEnvironment).get();

    assertEquals(result.getTotal(), Integer.valueOf(2));
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));

    ArgumentCaptor<com.linkedin.metadata.query.filter.Filter> filterCaptor =
        ArgumentCaptor.forClass(com.linkedin.metadata.query.filter.Filter.class);
    Mockito.verify(_entityClient)
        .searchAcrossEntities(
            any(),
            eq(List.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME)),
            eq("*"),
            filterCaptor.capture(),
            eq(0),
            eq(10),
            isNull());

    final com.linkedin.metadata.query.filter.Filter filter = filterCaptor.getValue();
    assertNotNull(filter);
    assertEquals(filter.getOr().size(), 1);
    assertEquals(filter.getOr().get(0).getAnd().size(), 1);
    assertEquals(
        filter.getOr().get(0).getAnd().get(0).getField(),
        ListSemanticModelEntitiesResolver.SEMANTIC_MODEL_FIELD_NAME);
    assertEquals(filter.getOr().get(0).getAnd().get(0).getValues().get(0), TEST_SEMANTIC_MODEL_URN);
  }

  @Test
  public void testGetNullInputUsesDefaults() throws Exception {
    final SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(0);
    mockSearchResult.setMetadata(new SearchResultMetadata());
    mockSearchResult.setEntities(new SearchEntityArray());

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(null);
    Mockito.when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(null);

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(List.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME)),
                eq("*"),
                any(),
                eq(0),
                eq(10),
                isNull()))
        .thenReturn(mockSearchResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final SearchResults result = _resolver.get(_dataFetchingEnvironment).get();

    assertEquals(result.getTotal(), Integer.valueOf(0));
    assertEquals(result.getStart(), Integer.valueOf(0));
    assertEquals(result.getCount(), Integer.valueOf(10));
  }

  @Test
  public void testGetCombinesMembershipAndInputFilters() throws Exception {
    final SearchResult mockSearchResult = new SearchResult();
    mockSearchResult.setFrom(0);
    mockSearchResult.setPageSize(10);
    mockSearchResult.setNumEntities(0);
    mockSearchResult.setMetadata(new SearchResultMetadata());
    mockSearchResult.setEntities(new SearchEntityArray());

    final FacetFilterInput facetFilter = new FacetFilterInput();
    facetFilter.setField("origin");
    facetFilter.setCondition(FilterOperator.EQUAL);
    facetFilter.setValues(Collections.singletonList("PROD"));
    final AndFilterInput andFilter = new AndFilterInput();
    andFilter.setAnd(Collections.singletonList(facetFilter));
    final SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("*");
    input.setStart(0);
    input.setCount(10);
    input.setOrFilters(Collections.singletonList(andFilter));

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(null);

    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(List.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME)),
                eq("*"),
                any(),
                eq(0),
                eq(10),
                isNull()))
        .thenReturn(mockSearchResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    _resolver.get(_dataFetchingEnvironment).get();

    final ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(_entityClient)
        .searchAcrossEntities(
            any(),
            eq(List.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME)),
            eq("*"),
            filterCaptor.capture(),
            eq(0),
            eq(10),
            isNull());

    final Filter filter = filterCaptor.getValue();
    assertNotNull(filter);
    assertEquals(filter.getOr().size(), 1);
    assertEquals(filter.getOr().get(0).getAnd().get(0).getField(), "origin");
    assertEquals(filter.getOr().get(0).getAnd().get(0).getValues().get(0), "PROD");
    assertEquals(
        filter.getOr().get(0).getAnd().get(1).getField(),
        ListSemanticModelEntitiesResolver.SEMANTIC_MODEL_FIELD_NAME);
    assertEquals(filter.getOr().get(0).getAnd().get(1).getValues().get(0), TEST_SEMANTIC_MODEL_URN);
  }

  @Test
  public void testGetSearchFailurePropagates() throws Exception {
    final SearchAcrossEntitiesInput input = new SearchAcrossEntitiesInput();
    input.setQuery("*");
    input.setStart(0);
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(_dataFetchingEnvironment.getArgument("urn")).thenReturn(null);

    final RuntimeException rootCause = new RuntimeException("search failed");
    Mockito.when(
            _entityClient.searchAcrossEntities(
                any(),
                eq(List.of(Constants.METRIC_ENTITY_NAME, Constants.DATASET_ENTITY_NAME)),
                eq("*"),
                any(),
                eq(0),
                eq(10),
                isNull()))
        .thenThrow(rootCause);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final ExecutionException executionException =
        expectThrows(ExecutionException.class, () -> _resolver.get(_dataFetchingEnvironment).get());
    assertTrue(executionException.getCause() instanceof RuntimeException);
    assertSame(executionException.getCause().getCause(), rootCause);
  }
}
