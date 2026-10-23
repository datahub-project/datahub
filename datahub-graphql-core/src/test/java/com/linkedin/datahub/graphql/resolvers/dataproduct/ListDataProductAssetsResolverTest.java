package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.TestUtils;
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
import com.linkedin.metadata.config.DataHubAppConfiguration;
import com.linkedin.metadata.search.AggregationMetadataArray;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.metadata.search.SearchResultMetadata;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class ListDataProductAssetsResolverTest {

  private static final Urn ASSET_A = Urn.createFromTuple("dataset", "urn-a");
  private static final Urn ASSET_B = Urn.createFromTuple("dataset", "urn-b");

  @Test
  public void testGetUrnsToFilterOn_noFilters_returnsAllAssets() {
    ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(Mockito.mock(EntityClient.class));

    List<Urn> result =
        resolver.getUrnsToFilterOn(
            ImmutableList.of(ASSET_A, ASSET_B), ImmutableSet.of(ASSET_A.toString()), null);

    assertEquals(result.size(), 2);
    assertTrue(result.contains(ASSET_A));
    assertTrue(result.contains(ASSET_B));
  }

  @Test
  public void testGetUrnsToFilterOn_outputPortTrue_filtersToOutputPortUrnsOnly() {
    ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(Mockito.mock(EntityClient.class));

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
    ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(Mockito.mock(EntityClient.class));

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
    ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(Mockito.mock(EntityClient.class));

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
    ListDataProductAssetsResolver resolver =
        new ListDataProductAssetsResolver(Mockito.mock(EntityClient.class));

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

  // Assets of entity types only a newer version knows (read after a rollback) can't be searched;
  // naming their type in the search would fail the whole request.

  @Test
  public void testSearchesOnlyKnownAssetTypes() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);
    Mockito.when(
            entityClient.searchAcrossEntities(
                Mockito.any(),
                Mockito.anyList(),
                Mockito.anyString(),
                Mockito.any(),
                Mockito.anyInt(),
                Mockito.any(),
                Mockito.isNull()))
        .thenReturn(
            new SearchResult()
                .setEntities(new SearchEntityArray())
                .setNumEntities(0)
                .setFrom(0)
                .setPageSize(10)
                .setMetadata(
                    new SearchResultMetadata().setAggregations(new AggregationMetadataArray())));

    run(entityClient, List.of(ASSET_A, UrnUtils.getUrn("urn:li:entityFromNewerBuild:x")));

    ArgumentCaptor<List<String>> types = ArgumentCaptor.forClass(List.class);
    Mockito.verify(entityClient)
        .searchAcrossEntities(
            Mockito.any(),
            types.capture(),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.any(),
            Mockito.isNull());
    assertEquals(types.getValue(), List.of("dataset"));
  }

  @Test
  public void testOnlyUnknownAssetTypesGiveAnEmptyPageWithoutSearching() throws Exception {
    EntityClient entityClient = Mockito.mock(EntityClient.class);

    SearchResults results =
        run(entityClient, List.of(UrnUtils.getUrn("urn:li:entityFromNewerBuild:x")));

    assertEquals(results.getTotal(), 0);
    Mockito.verify(entityClient, Mockito.never())
        .searchAcrossEntities(
            Mockito.any(),
            Mockito.anyList(),
            Mockito.anyString(),
            Mockito.any(),
            Mockito.anyInt(),
            Mockito.any(),
            Mockito.any());
  }

  private static SearchResults run(EntityClient entityClient, List<Urn> assets) throws Exception {
    Urn product = UrnUtils.getUrn("urn:li:dataProduct:p");
    DataProductProperties properties =
        new DataProductProperties()
            .setAssets(
                new DataProductAssociationArray(
                    assets.stream()
                        .map(asset -> new DataProductAssociation().setDestinationUrn(asset))
                        .collect(Collectors.toList())));
    Mockito.when(
            entityClient.getV2(
                Mockito.any(), Mockito.eq("dataProduct"), Mockito.eq(product), Mockito.any()))
        .thenReturn(
            new EntityResponse()
                .setUrn(product)
                .setAspects(
                    new EnvelopedAspectMap(
                        Map.of(
                            "dataProductProperties",
                            new EnvelopedAspect().setValue(new Aspect(properties.data()))))));
    QueryContext context = TestUtils.getMockAllowContext();
    Mockito.when(context.getDataHubAppConfig())
        .thenReturn(Mockito.mock(DataHubAppConfiguration.class, Mockito.RETURNS_DEEP_STUBS));
    DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(context);
    Mockito.when(environment.getArgument("urn")).thenReturn(product.toString());
    Mockito.when(environment.getArgument("input")).thenReturn(Map.of("query", "*"));

    return new ListDataProductAssetsResolver(entityClient).get(environment).get();
  }
}
