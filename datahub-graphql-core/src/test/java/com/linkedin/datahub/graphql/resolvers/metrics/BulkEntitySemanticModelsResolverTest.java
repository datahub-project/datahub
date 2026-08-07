package com.linkedin.datahub.graphql.resolvers.metrics;

import static org.mockito.ArgumentMatchers.any;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertThrows;

import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BulkEntitySemanticModelsInput;
import com.linkedin.datahub.graphql.generated.BulkEntitySemanticModelsResult;
import com.linkedin.datahub.graphql.generated.EntitySemanticModel;
import com.linkedin.metadata.aspect.CachingAspectRetriever;
import com.linkedin.metadata.aspect.GraphRetriever;
import com.linkedin.metadata.aspect.models.graph.RelatedEntities;
import com.linkedin.metadata.aspect.models.graph.RelatedEntitiesScrollResult;
import com.linkedin.metadata.entity.SearchRetriever;
import com.linkedin.metadata.query.filter.RelationshipDirection;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.metadata.context.RetrieverContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Covers the input-size guard and end-to-end Contains scroll in {@link
 * BulkEntitySemanticModelsResolver}.
 */
public class BulkEntitySemanticModelsResolverTest {

  private static final String METRIC =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.orders_model,revenue)";
  private static final String DATASET =
      "urn:li:dataset:(urn:li:dataPlatform:dbt,analytics.orders_model.orders_ds,PROD)";
  private static final String SM =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.orders_model,orders_model)";
  private static final String OTHER_CONTAINER = "urn:li:dataProduct:not-a-semantic-model";

  @Test
  public void testGetRejectsTooManyUrns() {
    final BulkEntitySemanticModelsInput input = new BulkEntitySemanticModelsInput();
    input.setUrns(
        IntStream.rangeClosed(1, 101)
            .mapToObj(
                i -> String.format("urn:li:metric:(urn:li:dataPlatform:dbt,analytics,m%d)", i))
            .collect(Collectors.toList()));

    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(Mockito.mock(QueryContext.class));
    Mockito.when(environment.getArgument("input")).thenReturn(input);

    assertThrows(
        IllegalArgumentException.class,
        () -> new BulkEntitySemanticModelsResolver().get(environment));
  }

  @Test
  public void testGetHappyPathMetricAndDataset() throws Exception {
    final GraphRetriever graphRetriever = Mockito.mock(GraphRetriever.class);
    Mockito.when(
            graphRetriever.scrollRelatedEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2, 2, null, List.of(relatedEntity(SM, METRIC), relatedEntity(SM, DATASET))));

    final BulkEntitySemanticModelsInput input = new BulkEntitySemanticModelsInput();
    input.setUrns(List.of(METRIC, DATASET));

    final QueryContext queryContext = queryContext(graphRetriever);
    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(queryContext);
    Mockito.when(environment.getArgument("input")).thenReturn(input);

    final BulkEntitySemanticModelsResult result =
        new BulkEntitySemanticModelsResolver().get(environment).get();

    assertEquals(result.getEntities().size(), 2);
    final EntitySemanticModel metricRow = result.getEntities().get(0);
    assertEquals(metricRow.getUrn(), METRIC);
    assertEquals(metricRow.getSemanticModel().getUrn(), SM);
    final EntitySemanticModel datasetRow = result.getEntities().get(1);
    assertEquals(datasetRow.getUrn(), DATASET);
    assertEquals(datasetRow.getSemanticModel().getUrn(), SM);
  }

  @Test
  public void testGetSkipsNonSemanticModelContainsSources() throws Exception {
    final GraphRetriever graphRetriever = Mockito.mock(GraphRetriever.class);
    Mockito.when(
            graphRetriever.scrollRelatedEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                1, 1, null, List.of(relatedEntity(OTHER_CONTAINER, METRIC))));

    final BulkEntitySemanticModelsInput input = new BulkEntitySemanticModelsInput();
    input.setUrns(List.of(METRIC));

    final QueryContext queryContext = queryContext(graphRetriever);
    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(queryContext);
    Mockito.when(environment.getArgument("input")).thenReturn(input);

    final BulkEntitySemanticModelsResult result =
        new BulkEntitySemanticModelsResolver().get(environment).get();

    assertEquals(result.getEntities().size(), 1);
    assertNull(result.getEntities().get(0).getSemanticModel());
  }

  private static QueryContext queryContext(final GraphRetriever graphRetriever) {
    final OperationContext base = TestOperationContexts.systemContextNoSearchAuthorization();
    final RetrieverContext retrieverContext =
        RetrieverContext.builder()
            .graphRetriever(graphRetriever)
            .searchRetriever(SearchRetriever.EMPTY)
            .cachingAspectRetriever(CachingAspectRetriever.EMPTY)
            .build();
    final OperationContext opContext =
        base.toBuilder()
            .retrieverContext(retrieverContext)
            .build(base.getSessionAuthentication(), false);
    final QueryContext queryContext = Mockito.mock(QueryContext.class);
    Mockito.when(queryContext.getOperationContext()).thenReturn(opContext);
    return queryContext;
  }

  private static RelatedEntities relatedEntity(
      final String semanticModelUrn, final String entityUrn) {
    return new RelatedEntities(
        "Contains", semanticModelUrn, entityUrn, RelationshipDirection.INCOMING, null);
  }
}
