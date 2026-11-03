package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.StringArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.datahub.graphql.generated.SearchResults;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.query.filter.Condition;
import com.linkedin.metadata.query.filter.ConjunctiveCriterion;
import com.linkedin.metadata.query.filter.Criterion;
import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import com.linkedin.metadata.service.ViewService;
import com.linkedin.metadata.utils.CriterionUtils;
import com.linkedin.view.DataHubViewDefinition;
import com.linkedin.view.DataHubViewInfo;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListDataProductAssetsResolverTest {

  private static final Urn ASSET_A = Urn.createFromTuple("dataset", "urn-a");
  private static final Urn ASSET_B = Urn.createFromTuple("dataset", "urn-b");

  private static ListDataProductAssetsResolver newResolver() {
    return new ListDataProductAssetsResolver(
        Mockito.mock(EntityClient.class), Mockito.mock(ViewService.class));
  }

  @Test
  public void testGetUrnsToFilterOn_noFilters_returnsAllAssets() {
    ListDataProductAssetsResolver resolver = newResolver();

    List<Urn> result =
        resolver.getUrnsToFilterOn(
            ImmutableList.of(ASSET_A, ASSET_B), ImmutableSet.of(ASSET_A.toString()), null);

    assertEquals(result.size(), 2);
    assertTrue(result.contains(ASSET_A));
    assertTrue(result.contains(ASSET_B));
  }

  @Test
  public void testGetUrnsToFilterOn_outputPortTrue_filtersToOutputPortUrnsOnly() {
    ListDataProductAssetsResolver resolver = newResolver();

    FacetFilterInput filter = new FacetFilterInput();
    filter.setField("isOutputPort");
    filter.setValues(ImmutableList.of("true"));

    List<Urn> result =
        resolver.getUrnsToFilterOn(
            ImmutableList.of(ASSET_A, ASSET_B),
            ImmutableSet.of(ASSET_A.toString()),
            new ArrayList<>(Collections.singletonList(filter)));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), ASSET_A);
  }

  @Test
  public void testGetUrnsToFilterOn_outputPortFalse_excludesOutputPorts() {
    ListDataProductAssetsResolver resolver = newResolver();

    FacetFilterInput filter = new FacetFilterInput();
    filter.setField("isOutputPort");
    filter.setValues(ImmutableList.of("false"));

    List<Urn> result =
        resolver.getUrnsToFilterOn(
            ImmutableList.of(ASSET_A, ASSET_B),
            ImmutableSet.of(ASSET_A.toString()),
            new ArrayList<>(Collections.singletonList(filter)));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), ASSET_B);
  }

  @Test
  public void testGetUrnsToFilterOn_outputPortEmptyValues_excludesOutputPorts() {
    ListDataProductAssetsResolver resolver = newResolver();

    FacetFilterInput filter = new FacetFilterInput();
    filter.setField("isOutputPort");
    filter.setValues(ImmutableList.of());

    List<Urn> result =
        resolver.getUrnsToFilterOn(
            ImmutableList.of(ASSET_A, ASSET_B),
            ImmutableSet.of(ASSET_A.toString()),
            new ArrayList<>(Collections.singletonList(filter)));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), ASSET_B);
  }

  @Test
  public void testGetUrnsToFilterOn_outputPortNullValues_excludesOutputPorts() {
    ListDataProductAssetsResolver resolver = newResolver();

    FacetFilterInput filter = new FacetFilterInput();
    filter.setField("isOutputPort");
    filter.setValues(null);

    List<Urn> result =
        resolver.getUrnsToFilterOn(
            ImmutableList.of(ASSET_A, ASSET_B),
            ImmutableSet.of(ASSET_A.toString()),
            new ArrayList<>(Collections.singletonList(filter)));

    assertEquals(result.size(), 1);
    assertEquals(result.get(0), ASSET_B);
  }

  private static final Urn DATA_PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:test");
  private static final String VIEW_URN = "urn:li:dataHubView:test";

  @Test
  public void testSelectedViewNarrowsTheAssetSearch() throws Exception {
    final EntityClient mockClient = mockClientWithAssets(ASSET_A);
    final ViewService mockViewService = mockViewService("dataset");
    final ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(mockClient, mockViewService);

    resolver.get(mockEnv(VIEW_URN)).get();

    final ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(
            any(), eq(List.of("dataset")), eq("*"), filterCaptor.capture(), anyInt(), any(), any());
    // Every branch of the final filter keeps the data product's urns (matched by urn or schema
    // field alias) and adds the view's filter.
    for (ConjunctiveCriterion and : filterCaptor.getValue().getOr()) {
      final List<String> fields =
          and.getAnd().stream()
              .map(Criterion::getField)
              .collect(java.util.stream.Collectors.toList());
      assertTrue(
          fields.contains("urn") || fields.contains("schemaFieldAliases.keyword"),
          fields.toString());
      assertTrue(fields.contains("platform"), fields.toString());
    }
  }

  @Test
  public void testSelectedViewWithoutCommonEntityTypesSkipsTheSearch() throws Exception {
    final EntityClient mockClient = mockClientWithAssets(ASSET_A);
    final ViewService mockViewService = mockViewService("chart");
    final ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(mockClient, mockViewService);

    final SearchResults results = resolver.get(mockEnv(VIEW_URN)).get();

    assertEquals((int) results.getTotal(), 0);
    Mockito.verify(mockClient, Mockito.never())
        .searchAcrossEntities(any(), any(), any(), any(), anyInt(), any(), any());
  }

  @Test
  public void testNoViewSearchesTheDataProductAssetsOnly() throws Exception {
    final EntityClient mockClient = mockClientWithAssets(ASSET_A);
    final ViewService mockViewService = Mockito.mock(ViewService.class);
    final ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(mockClient, mockViewService);

    resolver.get(mockEnv(null)).get();

    final ArgumentCaptor<Filter> filterCaptor = ArgumentCaptor.forClass(Filter.class);
    Mockito.verify(mockClient, Mockito.times(1))
        .searchAcrossEntities(
            any(), eq(List.of("dataset")), eq("*"), filterCaptor.capture(), anyInt(), any(), any());
    for (ConjunctiveCriterion and : filterCaptor.getValue().getOr()) {
      assertTrue(and.getAnd().stream().noneMatch(c -> c.getField().equals("platform")));
    }
    Mockito.verifyNoInteractions(mockViewService);
  }

  private static EntityClient mockClientWithAssets(final Urn... assets) throws Exception {
    final EntityClient mockClient = Mockito.mock(EntityClient.class);
    final DataProductAssociationArray associations = new DataProductAssociationArray();
    for (Urn asset : assets) {
      associations.add(new DataProductAssociation().setDestinationUrn(asset));
    }
    final DataProductProperties properties = new DataProductProperties().setAssets(associations);
    Mockito.when(
            mockClient.getV2(
                any(),
                eq(Constants.DATA_PRODUCT_ENTITY_NAME),
                eq(DATA_PRODUCT_URN),
                eq(Collections.singleton(Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            new EntityResponse()
                .setUrn(DATA_PRODUCT_URN)
                .setEntityName(Constants.DATA_PRODUCT_ENTITY_NAME)
                .setAspects(
                    new EnvelopedAspectMap(
                        Map.of(
                            Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME,
                            new EnvelopedAspect().setValue(new Aspect(properties.data()))))));
    Mockito.when(
            mockClient.searchAcrossEntities(any(), any(), any(), any(), anyInt(), any(), any()))
        .thenReturn(
            new SearchResult()
                .setFrom(0)
                .setPageSize(0)
                .setNumEntities(0)
                .setEntities(new SearchEntityArray())
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));
    return mockClient;
  }

  private static ViewService mockViewService(final String entityType) throws Exception {
    final ViewService mockViewService = Mockito.mock(ViewService.class);
    final Filter viewFilter =
        new Filter()
            .setOr(
                new com.linkedin.metadata.query.filter.ConjunctiveCriterionArray(
                    new ConjunctiveCriterion()
                        .setAnd(
                            new com.linkedin.metadata.query.filter.CriterionArray(
                                CriterionUtils.buildCriterion(
                                    "platform",
                                    Condition.EQUAL,
                                    "urn:li:dataPlatform:snowflake")))));
    Mockito.when(mockViewService.getViewInfo(any(), eq(UrnUtils.getUrn(VIEW_URN))))
        .thenReturn(
            new DataHubViewInfo()
                .setDefinition(
                    new DataHubViewDefinition()
                        .setEntityTypes(new StringArray(List.of(entityType)))
                        .setFilter(viewFilter)));
    return mockViewService;
  }

  private static DataFetchingEnvironment mockEnv(final String viewUrn) {
    final QueryContext mockContext = getMockAllowContext();
    Mockito.when(mockContext.getDataHubAppConfig())
        .thenReturn(Mockito.mock(DataHubAppConfiguration.class, Mockito.RETURNS_DEEP_STUBS));
    final java.util.HashMap<String, Object> input = new java.util.HashMap<>();
    input.put("query", "*");
    input.put("start", 0);
    input.put("count", 10);
    if (viewUrn != null) {
      input.put("viewUrn", viewUrn);
    }
    final DataFetchingEnvironment mockEnv = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(mockEnv.getArgument(eq("urn"))).thenReturn(DATA_PRODUCT_URN.toString());
    Mockito.when(mockEnv.getArgument(eq("input"))).thenReturn(input);
    Mockito.when(mockEnv.getContext()).thenReturn(mockContext);
    return mockEnv;
  }
}
