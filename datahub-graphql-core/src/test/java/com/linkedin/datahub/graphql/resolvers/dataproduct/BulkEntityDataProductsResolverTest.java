package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static com.linkedin.metadata.Constants.SIBLINGS_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.Siblings;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.data.template.SetMode;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.BulkEntityDataProductsInput;
import com.linkedin.datahub.graphql.generated.BulkEntityDataProductsResult;
import com.linkedin.datahub.graphql.generated.EntityDataProductAssociation;
import com.linkedin.datahub.graphql.generated.EntityDataProducts;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.entity.client.EntityClient;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.mockito.Mockito;
import org.testng.annotations.Test;

/**
 * Covers {@link BulkEntityDataProductsResolver#buildEntityDataProducts} — folding an entity with
 * its siblings when determining data product membership and output-port status — the input-size
 * guard in {@link BulkEntityDataProductsResolver#get}, and the end-to-end resolution flow.
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
    return result.getDataProductAssociations().stream()
        .map(association -> association.getDataProduct().getUrn())
        .collect(Collectors.toSet());
  }

  private static Set<String> outputPortDataProductUrns(final EntityDataProducts result) {
    return result.getDataProductAssociations().stream()
        .filter(EntityDataProductAssociation::getIsOutputPort)
        .map(association -> association.getDataProduct().getUrn())
        .collect(Collectors.toSet());
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
    assertEquals(outputPortDataProductUrns(result), ImmutableSet.of(DP_1));
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
    assertTrue(outputPortDataProductUrns(result).isEmpty());
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
    assertEquals(outputPortDataProductUrns(result), ImmutableSet.of(DP_2));
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
    assertTrue(outputPortDataProductUrns(result).isEmpty());
  }

  @Test
  public void testNoMemberships() {
    EntityDataProducts result =
        newResolver().buildEntityDataProducts(ENTITY, Collections.emptySet(), Map.of(), Map.of());

    assertTrue(result.getDataProductAssociations().isEmpty());
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

  /**
   * End-to-end resolution where the requested entity and its sibling each belong to a different
   * data product. Both memberships must surface on the single requested entity, with output-port
   * status folded across the sibling group.
   */
  @Test
  public void testGetReturnsBothDataProductsAcrossSiblings() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);

    // ENTITY has SIBLING; siblings are read for the input urns only.
    Mockito.when(
            entityClient.batchGetV2(
                any(), eq("dataset"), any(), eq(Collections.singleton(SIBLINGS_ASPECT_NAME))))
        .thenReturn(Map.of(UrnUtils.getUrn(ENTITY), siblingsResponse(ENTITY, SIBLING)));

    // DP_1 marks ENTITY as an output port; DP_2 marks nothing.
    Mockito.when(
            entityClient.batchGetV2(
                any(),
                eq(DATA_PRODUCT_ENTITY_NAME),
                any(),
                eq(Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(
            Map.of(
                UrnUtils.getUrn(DP_1), dataProductResponse(ENTITY),
                UrnUtils.getUrn(DP_2), dataProductResponse()));

    // DataProductContains edges: DP_1 -> ENTITY, DP_2 -> SIBLING (incoming to the entities).
    final GraphRetriever graphRetriever = Mockito.mock(GraphRetriever.class);
    Mockito.when(
            graphRetriever.scrollRelatedEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(
                2, 2, null, List.of(relatedEntity(DP_1, ENTITY), relatedEntity(DP_2, SIBLING))));

    final BulkEntityDataProductsInput input = new BulkEntityDataProductsInput();
    input.setUrns(List.of(ENTITY));

    final QueryContext queryContext = queryContext(graphRetriever);
    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(queryContext);
    Mockito.when(environment.getArgument("input")).thenReturn(input);

    final BulkEntityDataProductsResult result =
        new BulkEntityDataProductsResolver(entityClient).get(environment).get();

    assertEquals(result.getEntities().size(), 1);
    final EntityDataProducts entity = result.getEntities().get(0);
    assertEquals(entity.getUrn(), ENTITY);
    assertEquals(dataProductUrns(entity), ImmutableSet.of(DP_1, DP_2));
    // ENTITY is an output port of DP_1 only.
    assertEquals(outputPortDataProductUrns(entity), ImmutableSet.of(DP_1));
  }

  /**
   * End-to-end happy path: a single entity with no siblings that belongs to one data product and is
   * an output port of it.
   */
  @Test
  public void testGetHappyPath() throws Exception {
    final EntityClient entityClient = Mockito.mock(EntityClient.class);

    // No siblings for the requested entity.
    Mockito.when(
            entityClient.batchGetV2(
                any(), eq("dataset"), any(), eq(Collections.singleton(SIBLINGS_ASPECT_NAME))))
        .thenReturn(Map.of());

    // DP_1 contains ENTITY and marks it as an output port.
    Mockito.when(
            entityClient.batchGetV2(
                any(),
                eq(DATA_PRODUCT_ENTITY_NAME),
                any(),
                eq(Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(UrnUtils.getUrn(DP_1), dataProductResponse(ENTITY)));

    final GraphRetriever graphRetriever = Mockito.mock(GraphRetriever.class);
    Mockito.when(
            graphRetriever.scrollRelatedEntities(
                any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(
            new RelatedEntitiesScrollResult(1, 1, null, List.of(relatedEntity(DP_1, ENTITY))));

    final BulkEntityDataProductsInput input = new BulkEntityDataProductsInput();
    input.setUrns(List.of(ENTITY));

    final QueryContext queryContext = queryContext(graphRetriever);
    final DataFetchingEnvironment environment = Mockito.mock(DataFetchingEnvironment.class);
    Mockito.when(environment.getContext()).thenReturn(queryContext);
    Mockito.when(environment.getArgument("input")).thenReturn(input);

    final BulkEntityDataProductsResult result =
        new BulkEntityDataProductsResolver(entityClient).get(environment).get();

    assertEquals(result.getEntities().size(), 1);
    final EntityDataProducts entity = result.getEntities().get(0);
    assertEquals(entity.getUrn(), ENTITY);
    assertEquals(entity.getDataProductAssociations().size(), 1);
    final EntityDataProductAssociation association = entity.getDataProductAssociations().get(0);
    assertEquals(association.getDataProduct().getUrn(), DP_1);
    assertTrue(association.getIsOutputPort());
  }

  private static QueryContext queryContext(final GraphRetriever graphRetriever) {
    // System context makes canView permissive, so the resolver returns every resolved membership.
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
      final String dataProductUrn, final String entityUrn) {
    return new RelatedEntities(
        "DataProductContains", dataProductUrn, entityUrn, RelationshipDirection.INCOMING, null);
  }

  private static EntityResponse siblingsResponse(final String urn, final String sibling)
      throws Exception {
    final Siblings siblings =
        new Siblings().setSiblings(new com.linkedin.common.UrnArray(Urn.createFromString(sibling)));
    return entityResponse(urn, SIBLINGS_ASPECT_NAME, siblings);
  }

  private static EntityResponse dataProductResponse(final String... outputPortUrns) {
    final DataProductAssociationArray assets = new DataProductAssociationArray();
    for (String outputPortUrn : outputPortUrns) {
      assets.add(
          new DataProductAssociation()
              .setDestinationUrn(UrnUtils.getUrn(outputPortUrn))
              .setOutputPort(true));
    }
    final DataProductProperties properties =
        new DataProductProperties().setAssets(assets, SetMode.IGNORE_NULL);
    return entityResponse(DP_1, DATA_PRODUCT_PROPERTIES_ASPECT_NAME, properties);
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
