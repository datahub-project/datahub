package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.graphql.generated.FacetFilterInput;
import com.linkedin.entity.client.EntityClient;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
}
