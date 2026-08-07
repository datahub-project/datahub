package com.linkedin.datahub.graphql.resolvers.metrics;

import static com.linkedin.datahub.graphql.TestUtils.*;
import static com.linkedin.metadata.Constants.SEMANTIC_MODEL_INFO_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.*;

import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.Entity;
import com.linkedin.datahub.graphql.generated.ScrollAcrossEntitiesInput;
import com.linkedin.datahub.graphql.generated.ScrollResults;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.semanticmodel.SemanticModelInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class SemanticModelMetricsResolverTest {

  private static final String TEST_SEMANTIC_MODEL_URN =
      "urn:li:semanticModel:(urn:li:dataPlatform:dbt,analytics.model,sales_model)";
  private static final String METRIC_URN =
      "urn:li:metric:(urn:li:dataPlatform:dbt,analytics.model,revenue)";

  private EntityClient _entityClient;
  private ViewService _viewService;
  private DataFetchingEnvironment _dataFetchingEnvironment;
  private SemanticModelMetricsResolver _resolver;
  private Entity _entity;

  @BeforeMethod
  public void setupTest() throws Exception {
    _entityClient = Mockito.mock(EntityClient.class);
    _viewService = Mockito.mock(ViewService.class);
    _dataFetchingEnvironment = Mockito.mock(DataFetchingEnvironment.class);
    _entity = Mockito.mock(Entity.class);
    Mockito.when(_entity.getUrn()).thenReturn(TEST_SEMANTIC_MODEL_URN);
    Mockito.when(_dataFetchingEnvironment.getSource()).thenReturn(_entity);

    Mockito.when(
            _entityClient.getV2(
                any(),
                eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_SEMANTIC_MODEL_URN)),
                eq(Collections.singleton(SEMANTIC_MODEL_INFO_ASPECT_NAME)),
                eq(false)))
        .thenReturn(semanticModelResponse(METRIC_URN));

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
  public void testMembershipAndRootFiltersAreInjected() throws Exception {
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

    // Membership comes from semanticModelInfo.metrics (urn filter); root-only via hasParentMetric.
    Mockito.verify(_entityClient)
        .scrollAcrossEntities(
            any(),
            any(),
            any(),
            Mockito.argThat(
                filter -> {
                  if (filter == null || filter.getOr() == null) {
                    return false;
                  }
                  final String filterStr = filter.toString();
                  return filterStr.contains(METRIC_URN)
                      && filterStr.contains(
                          SemanticModelMetricsResolver.HAS_PARENT_METRIC_FIELD_NAME);
                }),
            any(),
            any(),
            any(),
            any(),
            any());
  }

  @Test
  public void testEmptyMembershipReturnsEmptyWithoutScroll() throws Exception {
    Mockito.when(
            _entityClient.getV2(
                any(),
                eq(Constants.SEMANTIC_MODEL_ENTITY_NAME),
                eq(UrnUtils.getUrn(TEST_SEMANTIC_MODEL_URN)),
                eq(Collections.singleton(SEMANTIC_MODEL_INFO_ASPECT_NAME)),
                eq(false)))
        .thenReturn(semanticModelResponse());

    final ScrollAcrossEntitiesInput input = new ScrollAcrossEntitiesInput();
    input.setQuery("*");
    input.setCount(10);
    Mockito.when(_dataFetchingEnvironment.getArgument("input")).thenReturn(input);

    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(_dataFetchingEnvironment.getContext()).thenReturn(mockContext);

    final ScrollResults result = _resolver.get(_dataFetchingEnvironment).get();

    assertEquals(result.getTotal(), 0);
    Mockito.verify(_entityClient, Mockito.never())
        .scrollAcrossEntities(any(), any(), any(), any(), any(), any(), any(), any(), any());
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

  private static EntityResponse semanticModelResponse(final String... metricUrns) throws Exception {
    final UrnArray metrics = new UrnArray();
    for (String metricUrn : metricUrns) {
      metrics.add(Urn.createFromString(metricUrn));
    }
    final SemanticModelInfo info =
        new SemanticModelInfo().setName("sales_model").setMetrics(metrics);
    return entityResponse(TEST_SEMANTIC_MODEL_URN, SEMANTIC_MODEL_INFO_ASPECT_NAME, info);
  }

  private static EntityResponse entityResponse(
      final String urn, final String aspectName, final RecordTemplate aspect) {
    final EnvelopedAspect envelopedAspect =
        new EnvelopedAspect().setValue(new Aspect(aspect.data()));
    final Map<String, EnvelopedAspect> aspects = new HashMap<>();
    aspects.put(aspectName, envelopedAspect);
    return new EntityResponse()
        .setUrn(UrnUtils.getUrn(urn))
        .setAspects(new EnvelopedAspectMap(aspects));
  }
}
