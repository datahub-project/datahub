package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BulkEntityDataProductsInput;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.EntityDataProducts;
import com.linkedin.entity.client.EntityClient;
import graphql.schema.DataFetchingEnvironment;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Covers {@link BulkEntityDataProductsResolver#buildEntityDataProducts} — folding an entity with
 * its siblings when determining data product membership and output-port status — and the input-size
 * guard in {@link BulkEntityDataProductsResolver#get}.
 */
public class BulkEntityDataProductsResolverTest {

  private static final String ENTITY = "urn:li:dataset:(urn:li:dataPlatform:mysql,db.t1,PROD)";
  private static final String SIBLING = "urn:li:dataset:(urn:li:dataPlatform:dbt,db.t1,PROD)";
  private static final String DP_1 = "urn:li:dataProduct:dp1";
  private static final String DP_2 = "urn:li:dataProduct:dp2";

  private static BulkEntityDataProductsResolver newResolver() {
    return new BulkEntityDataProductsResolver(Mockito.mock(EntityClient.class));
  }

  private static Set<String> dataProductUrns(final EntityDataProducts result) {
    return result.getDataProducts().stream().map(DataProduct::getUrn).collect(Collectors.toSet());
  }

  @Test
  public void testDirectMembershipAndOutputPort() {
    EntityDataProducts result =
        newResolver()
            .buildEntityDataProducts(
                ENTITY,
                Collections.emptySet(),
                ImmutableMap.of(ENTITY, ImmutableSet.of(DP_1)),
                ImmutableMap.of(DP_1, ImmutableSet.of(ENTITY)));

    assertEquals(result.getUrn(), ENTITY);
    assertEquals(dataProductUrns(result), ImmutableSet.of(DP_1));
    assertEquals(result.getOutputPortInDataProducts(), List.of(DP_1));
  }

  @Test
  public void testMemberButNotOutputPort() {
    EntityDataProducts result =
        newResolver()
            .buildEntityDataProducts(
                ENTITY,
                Collections.emptySet(),
                ImmutableMap.of(ENTITY, ImmutableSet.of(DP_1)),
                // DP_1 marks some other asset, not this entity, as an output port
                ImmutableMap.of(DP_1, ImmutableSet.of("urn:li:dataset:(other)")));

    assertEquals(dataProductUrns(result), ImmutableSet.of(DP_1));
    assertTrue(result.getOutputPortInDataProducts().isEmpty());
  }

  @Test
  public void testSiblingMembershipAndOutputPortFoldedIn() {
    // The entity itself is in no data product; its sibling is in DP_2 and is an output port there.
    EntityDataProducts result =
        newResolver()
            .buildEntityDataProducts(
                ENTITY,
                ImmutableSet.of(SIBLING),
                ImmutableMap.of(SIBLING, ImmutableSet.of(DP_2)),
                ImmutableMap.of(DP_2, ImmutableSet.of(SIBLING)));

    assertEquals(dataProductUrns(result), ImmutableSet.of(DP_2));
    assertEquals(result.getOutputPortInDataProducts(), List.of(DP_2));
  }

  @Test
  public void testEntityAndSiblingMembershipsAreUnioned() {
    EntityDataProducts result =
        newResolver()
            .buildEntityDataProducts(
                ENTITY,
                ImmutableSet.of(SIBLING),
                ImmutableMap.of(
                    ENTITY, ImmutableSet.of(DP_1),
                    SIBLING, ImmutableSet.of(DP_2)),
                Map.of());

    assertEquals(dataProductUrns(result), ImmutableSet.of(DP_1, DP_2));
    assertTrue(result.getOutputPortInDataProducts().isEmpty());
  }

  @Test
  public void testNoMemberships() {
    EntityDataProducts result =
        newResolver().buildEntityDataProducts(ENTITY, Collections.emptySet(), Map.of(), Map.of());

    assertTrue(result.getDataProducts().isEmpty());
    assertTrue(result.getOutputPortInDataProducts().isEmpty());
  }

  @Test
  public void testGetRejectsTooManyUrns() {
    final BulkEntityDataProductsInput input = new BulkEntityDataProductsInput();
    // One past the 100-urn batch limit enforced by the resolver.
    input.setUrns(
        IntStream.rangeClosed(1, 101)
            .mapToObj(
                i -> String.format("urn:li:dataset:(urn:li:dataPlatform:mysql,db.t%d,PROD)", i))
            .collect(Collectors.toList()));

    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(Mockito.mock(QueryContext.class));
    Mockito.when(environment.getArgument("input")).thenReturn(input);

    assertThrows(IllegalArgumentException.class, () -> newResolver().get(environment));
  }
}
