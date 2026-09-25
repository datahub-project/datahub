package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.metadata.utils.CriterionUtils.buildCriterion;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.GetRootEntitiesInput;
import com.linkedin.datahub.graphql.generated.GetRootMetricsResult;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.ConjunctiveCriterionArray;
import com.linkedin.metadata.query.filter.CriterionArray;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class GetRootMetricsResolverTest {

  private static final String METRIC_URN_1 =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.model,revenue)";
  private static final String METRIC_URN_2 =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.model,cost)";

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
                eq(Constants.METRIC_ENTITY_NAME),
                eq("*"),
                eq(buildRootMetricsFilter()),
                any(),
                eq(0),
                eq(10)))
        .thenReturn(
            new SearchResult()
                .setEntities(
                    new SearchEntityArray(
                        ImmutableList.of(
                            new SearchEntity().setEntity(Urn.createFromString(METRIC_URN_1)),
                            new SearchEntity().setEntity(Urn.createFromString(METRIC_URN_2)))))
                .setFrom(0)
                .setPageSize(2)
                .setNumEntities(2));

    final GetRootMetricsResolver resolver = new GetRootMetricsResolver(mockClient);
    final GetRootMetricsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getStart(), 0);
    assertEquals(result.getCount(), 2);
    assertEquals(result.getTotal(), 2);
    assertEquals(result.getMetrics().get(0).getUrn(), METRIC_URN_1);
    assertEquals(result.getMetrics().get(1).getUrn(), METRIC_URN_2);
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
    Mockito.when(mockEnv.getArgument("input")).thenReturn(new GetRootEntitiesInput(5, 25, "*"));

    Mockito.when(
            mockClient.search(
                any(),
                eq(Constants.METRIC_ENTITY_NAME),
                eq("*"),
                eq(buildRootMetricsFilter()),
                any(),
                eq(5),
                eq(25)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray(Collections.emptyList()))
                .setFrom(5)
                .setPageSize(0)
                .setNumEntities(5));

    final GetRootMetricsResolver resolver = new GetRootMetricsResolver(mockClient);
    final GetRootMetricsResult result = resolver.get(mockEnv).get();

    assertEquals(result.getStart(), 5);
    assertEquals(result.getTotal(), 5);
    Mockito.verify(mockClient)
        .search(any(), eq(Constants.METRIC_ENTITY_NAME), eq("*"), any(), any(), eq(5), eq(25));
  }

  @Test
  public void testHasParentMetricFilterIsApplied() throws Exception {
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
                eq(Constants.METRIC_ENTITY_NAME),
                any(),
                eq(buildRootMetricsFilter()),
                any(),
                eq(0),
                eq(10)))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray(Collections.emptyList()))
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0));

    final GetRootMetricsResolver resolver = new GetRootMetricsResolver(mockClient);
    resolver.get(mockEnv).get();

    Mockito.verify(mockClient)
        .search(
            any(),
            eq(Constants.METRIC_ENTITY_NAME),
            any(),
            eq(buildRootMetricsFilter()),
            any(),
            anyInt(),
            anyInt());
  }

  private Filter buildRootMetricsFilter() {
    final CriterionArray array =
        new CriterionArray(
            ImmutableList.of(
                buildCriterion(
                    GetRootMetricsResolver.HAS_PARENT_METRIC_FIELD_NAME,
                    Condition.EQUAL,
                    "false")));
    final Filter filter = new Filter();
    filter.setOr(
        new ConjunctiveCriterionArray(ImmutableList.of(new ConjunctiveCriterion().setAnd(array))));
    return filter;
  }
}
