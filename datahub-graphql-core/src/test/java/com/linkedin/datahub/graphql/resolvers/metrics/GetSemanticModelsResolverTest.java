package com.linkedin.datahub.graphql.resolvers.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetRootEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetSemanticModelsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetSemanticModelsResolverTest {

  private static final String SEMANTIC_MODEL_URN_1 =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.model,sales_model)";
  private static final String SEMANTIC_MODEL_URN_2 =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.model,finance_model)";

  @Test
  public void testGetSuccess() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext mockContext = Mockito.mock(QueryContext.class);
    final OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(new GetRootEntitiesInput(0, 10, "*"));

    Mockito.when(
            mockClient.search(
                any(),
                eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
                eq("*"),
                isNull(),
                any(),
                eq(0),
                eq(10)))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity()
                                .setEntity(Urn.createFromString(SEMANTIC_MODEL_URN_1)),
                            new SearchEntity()
                                .setEntity(Urn.createFromString(SEMANTIC_MODEL_URN_2)))))
                .setFrom(0)
                .setPageSize(2)
                .setNumEntities(2));

    final GetSemanticModelsResolver resolver = new GetSemanticModelsResolver(mockClient);
    final GetSemanticModelsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getSemanticModels().get(0).getUrn(), SEMANTIC_MODEL_URN_1);
    assertEquals(result.getSemanticModels().get(1).getUrn(), SEMANTIC_MODEL_URN_2);
  }

  @Test
  public void testPaginationFlowsThrough() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext mockContext = Mockito.mock(QueryContext.class);
    final OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input"))
        .thenReturn(new GetRootEntitiesInput(10, 50, "my-model"));

    Mockito.when(
            mockClient.search(
                any(),
                eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
                eq("my-model"),
                isNull(),
                any(),
                eq(10),
                eq(50)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray(Collections.emptyList()))
                .setFrom(10)
                .setPageSize(0)
                .setNumEntities(10));

    final GetSemanticModelsResolver resolver = new GetSemanticModelsResolver(mockClient);
    final GetSemanticModelsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getStart(), 10);
    assertEquals(result.getTotal(), 10);
    Mockito.verify(mockClient)
        .search(
            any(),
            eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
            eq("my-model"),
            isNull(),
            any(),
            eq(10),
            eq(50));
  }

  @Test
  public void testNoFilterApplied() throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final QueryContext mockContext = Mockito.mock(QueryContext.class);
    final OperationContext mockOpContext = Mockito.mock(OperationContext.class);
    Mockito.when(mockContext.getOperationContext()).thenReturn(mockOpContext);
    Mockito.when(mockOpContext.withSearchFlags(any())).thenReturn(mockOpContext);

    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    Mockito.when(mockEnv.getArgument("input")).thenReturn(new GetRootEntitiesInput(0, 10, "*"));

    Mockito.when(
            mockClient.search(
                any(),
                eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
                any(),
                isNull(),
                any(),
                anyInt(),
                any()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray(Collections.emptyList()))
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0));

    final GetSemanticModelsResolver resolver = new GetSemanticModelsResolver(mockClient);
    resolver.get(mockEnv).get();

    // No filter (null) should be passed — unlike GetRootMetricsResolver which filters by
    // hasParentMetric
    Mockito.verify(mockClient)
        .search(
            any(),
            eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
            any(),
            isNull(),
            any(),
            anyInt(),
            any());
  }
}
