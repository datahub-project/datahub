package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SemanticModelMetricsResolverTest {

  private static final String TEST_SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.model,sales_model)";

  private EntityClient _entityClient;
  private ViewService _viewService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private SemanticModelMetricsResolver _resolver;
  private Entity _entity;

  @BeforeMethod
  public void setupTest() {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _entity = Mockito.mock(Entity.class);
    Mockito.when(_entity.getUrn()).thenReturn(TEST_SEMANTIC_MODEL_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(_entity);

    _resolver = new SemanticModelMetricsResolver(_entityClient, _viewService);
  }

  @Test
  public void testGetSuccess() throws Exception {
    final ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setScrollId("test-scroll-id");
    mockScrollResult.setPageSize(3);
    mockScrollResult.setNumEntities(3);
    mockScrollResult.setMetadata(new SearchResultMetadata());
    mockScrollResult.setEntities(new SearchEntityArray());

    final ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setQuery("*");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(),
                eq(Collections.singletonList(Constants.METRIC_ENTITY_NAME)),
                eq("*"),
                any(),
                eq(null),
                eq("5m"),
                eq(Collections.emptyList()),
                eq(10),
                eq(Collections.emptyList())))
        .thenReturn(mockScrollResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final ScrollResults result = _resolver.get(_dataFetchingEnvironment).get();

    assertEquals(result.getNextScrollId(), "test-scroll-id");
    assertEquals(result.getCount(), 3);
    assertEquals(result.getTotal(), 3);
  }

  @Test
  public void testSemanticModelFilterIsInjected() throws Exception {
    final ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setPageSize(0);
    mockScrollResult.setNumEntities(0);
    mockScrollResult.setMetadata(new SearchResultMetadata());
    mockScrollResult.setEntities(new SearchEntityArray());

    final ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setQuery("*");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(mockScrollResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    _resolver.get(_dataFetchingEnvironment).get();

    // Verify the filter contains both semanticModel=<urn> and hasParentMetric=false in the same
    // conjunction, so the resolver returns only root metrics of the given semantic model.
    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            Mockito.argThat(
                filter -> {
                  if (filter == null) return false;
                  return filter.getOr().stream()
                      .anyMatch(
                          cc -> {
                            boolean hasSemanticModel =
                                cc.getAnd().stream()
                                    .anyMatch(
                                        c ->
                                            SemanticModelMetricsResolver.SEMANTIC_MODEL_FIELD_NAME
                                                    .equals(c.getField())
                                                && c.getValues() != null
                                                && c.getValues().contains(TEST_SEMANTIC_MODEL_URN));
                            boolean hasNoParent =
                                cc.getAnd().stream()
                                    .anyMatch(
                                        c ->
                                            SemanticModelMetricsResolver
                                                    .HAS_PARENT_METRIC_FIELD_NAME
                                                    .equals(c.getField())
                                                && c.getValues() != null
                                                && c.getValues().contains("false"));
                            return hasSemanticModel && hasNoParent;
                          });
                }),
            any(),
            any(),
            any(),
            any(),
            any());
  }

  @Test
  public void testAlwaysSearchesOnlyMetricEntities() throws Exception {
    final ScrollResult mockScrollResult = new ScrollResult();
    mockScrollResult.setPageSize(0);
    mockScrollResult.setNumEntities(0);
    mockScrollResult.setMetadata(new SearchResultMetadata());
    mockScrollResult.setEntities(new SearchEntityArray());

    final ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setQuery("*");
    input.setCount(10);

    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);
    Mockito.when(
            _entityClient.scrollAcrossEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(mockScrollResult);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    _resolver.get(_dataFetchingEnvironment).get();

    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            eq(Collections.singletonList(Constants.METRIC_ENTITY_NAME)),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(),
            any());
  }
}
