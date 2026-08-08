package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.SearchAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.datahub.graphql.generated.SemanticModel;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.List;
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
}
