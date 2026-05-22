package com.linkedin.datahub.graphql.resolvers.dataproduct;

import static com.linkedin.datahub.graphql.TestUtils.getMockAllowContext;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.datahub.authorization.AuthorizationConfiguration;
import com.datahub.authorization.config.ViewAuthorizationConfiguration;
import com.linkedin.common.EntityRelationship;
import com.linkedin.common.EntityRelationshipArray;
import com.linkedin.common.EntityRelationships;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.data.template.IntegerArray;
import com.linkedin.datahub.graphql.QueryContext;
import com.linkedin.datahub.graphql.generated.DataProduct;
import com.linkedin.datahub.graphql.generated.DataProductLineageInput;
import com.linkedin.datahub.graphql.generated.DataProductLineageResult;
import com.linkedin.datahub.graphql.generated.LineageDirection;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.client.EntityClient;
import com.linkedin.metadata.graph.GraphClient;
import com.linkedin.metadata.search.LineageSearchEntity;
import com.linkedin.metadata.search.LineageSearchEntityArray;
import com.linkedin.metadata.search.LineageSearchResult;
import com.linkedin.metadata.service.DataProductService;
import graphql.schema.DataFetchingEnvironment;
import io.datahubproject.metadata.services.RestrictedService;
import java.util.HashMap;
import java.util.Map;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for {@link DataProductLineageResolver} that exercise the DataProduct-specific binding:
 * member enumeration via {@code DataProductProperties.assets} and the memberScanCap subList
 * truncation. The shared aggregation algorithm itself is covered by {@code
 * DomainLineageResolverTest}.
 */
public class DataProductLineageResolverTest {

  private static final String SOURCE_DP_URN = "urn:li:dataProduct:source";
  private static final String NEIGHBOUR_DP_URN = "urn:li:dataProduct:neighbour";
  private static final String ASSET_1 = "urn:li:dataset:(urn:li:dataPlatform:foo,asset1,PROD)";
  private static final String ASSET_2 = "urn:li:dataset:(urn:li:dataPlatform:foo,asset2,PROD)";
  private static final String ASSET_3 = "urn:li:dataset:(urn:li:dataPlatform:foo,asset3,PROD)";
  private static final String NEIGHBOUR_ASSET =
      "urn:li:dataset:(urn:li:dataPlatform:foo,nbrAsset,PROD)";

  private EntityClient entityClient;
  private GraphClient graphClient;
  private DataProductService dataProductService;
  private RestrictedService restrictedService;
  private AuthorizationConfiguration authConfig;
  private DataFetchingEnvironment env;
  private DataProductLineageResolver resolver;
  private Map<Urn, LineageSearchResult> hitsByMember;

  @BeforeMethod
  public void setUp() throws Exception {
    entityClient = mock(EntityClient.class);
    graphClient = mock(GraphClient.class);
    dataProductService = mock(DataProductService.class);
    restrictedService = mock(RestrictedService.class);

    authConfig = mock(AuthorizationConfiguration.class);
    ViewAuthorizationConfiguration viewConfig = mock(ViewAuthorizationConfiguration.class);
    when(viewConfig.isEnabled()).thenReturn(false);
    when(authConfig.getView()).thenReturn(viewConfig);

    env = mock(DataFetchingEnvironment.class);
    DataProduct source = new DataProduct();
    source.setUrn(SOURCE_DP_URN);
    QueryContext queryContext = getMockAllowContext();
    when(env.getSource()).thenReturn(source);
    when(env.getContext()).thenReturn(queryContext);

    hitsByMember = new HashMap<>();
    when(entityClient.searchAcrossLineage(
            any(),
            any(),
            any(),
            anyList(),
            anyString(),
            anyInt(),
            any(),
            anyList(),
            anyInt(),
            anyInt()))
        .thenAnswer(
            invocation -> {
              Urn memberUrn = invocation.getArgument(1);
              return hitsByMember.getOrDefault(
                  memberUrn,
                  new LineageSearchResult()
                      .setEntities(new LineageSearchEntityArray())
                      .setNumEntities(0));
            });

    resolver =
        new DataProductLineageResolver(
            entityClient, graphClient, dataProductService, restrictedService, authConfig);
  }

  @Test
  public void testNoMembersReturnsEmptyResult() throws Exception {
    when(dataProductService.getDataProductProperties(any(), eq(UrnUtils.getUrn(SOURCE_DP_URN))))
        .thenReturn(null);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DataProductLineageResult result = resolver.get(env).join();

    assertEquals(result.getTotal(), 0);
    assertEquals(result.getMemberScanCount(), 0);
    assertEquals(result.getMemberTotal(), 0);
    assertFalse(result.getIsPartial());
  }

  @Test
  public void testMembersReadFromDataProductPropertiesAssets() throws Exception {
    when(dataProductService.getDataProductProperties(any(), eq(UrnUtils.getUrn(SOURCE_DP_URN))))
        .thenReturn(buildProps(ASSET_1, ASSET_2));

    setLineageHits(ASSET_1, NEIGHBOUR_ASSET);
    setLineageHits(ASSET_2, NEIGHBOUR_ASSET);
    stubDataProductContains(NEIGHBOUR_ASSET, NEIGHBOUR_DP_URN);

    when(env.getArgument(eq("input"))).thenReturn(downstreamInput());

    DataProductLineageResult result = resolver.get(env).join();

    assertEquals(result.getMemberScanCount(), 2);
    assertEquals(result.getMemberTotal(), 2);
    assertEquals(result.getTotal(), 1);
    assertEquals(result.getRelationships().get(0).getEntity().getUrn(), NEIGHBOUR_DP_URN);
    assertEquals(result.getRelationships().get(0).getMemberMatchCount(), 2);
  }

  @Test
  public void testMemberScanCapTruncatesAssetsAndFlipsIsPartial() throws Exception {
    when(dataProductService.getDataProductProperties(any(), eq(UrnUtils.getUrn(SOURCE_DP_URN))))
        .thenReturn(buildProps(ASSET_1, ASSET_2, ASSET_3));

    setLineageHits(ASSET_1, NEIGHBOUR_ASSET);
    setLineageHits(ASSET_2, NEIGHBOUR_ASSET);
    setLineageHits(ASSET_3, NEIGHBOUR_ASSET);
    stubDataProductContains(NEIGHBOUR_ASSET, NEIGHBOUR_DP_URN);

    DataProductLineageInput input = downstreamInput();
    input.setMemberScanCap(2);
    when(env.getArgument(eq("input"))).thenReturn(input);

    DataProductLineageResult result = resolver.get(env).join();

    assertEquals(result.getMemberScanCount(), 2);
    assertEquals(result.getMemberTotal(), 3);
    assertTrue(result.getIsPartial(), "memberScanCap exceeded must flip isPartial");
    // Only the first two assets contribute; the third is dropped.
    assertEquals(result.getRelationships().get(0).getMemberMatchCount(), 2);
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private DataProductLineageInput downstreamInput() {
    DataProductLineageInput input = new DataProductLineageInput();
    input.setDirection(LineageDirection.DOWNSTREAM);
    input.setHops(1);
    input.setStart(0);
    input.setCount(25);
    input.setMemberScanCap(1000);
    input.setPerMemberCount(100);
    input.setIncludeRestricted(true);
    return input;
  }

  private DataProductProperties buildProps(String... assetUrns) {
    DataProductProperties props = new DataProductProperties();
    DataProductAssociationArray assets = new DataProductAssociationArray();
    for (String urn : assetUrns) {
      DataProductAssociation assoc = new DataProductAssociation();
      assoc.setDestinationUrn(UrnUtils.getUrn(urn));
      assets.add(assoc);
    }
    props.setAssets(assets);
    return props;
  }

  private void setLineageHits(String memberUrn, String neighbourUrn) {
    IntegerArray degrees = new IntegerArray();
    degrees.add(1);
    LineageSearchEntity entity =
        new LineageSearchEntity().setEntity(UrnUtils.getUrn(neighbourUrn)).setDegrees(degrees);
    LineageSearchEntityArray array = new LineageSearchEntityArray();
    array.add(entity);
    LineageSearchResult lsr = new LineageSearchResult().setEntities(array).setNumEntities(1);
    hitsByMember.put(UrnUtils.getUrn(memberUrn), lsr);
  }

  private void stubDataProductContains(String neighbourAssetUrn, String dpUrn) {
    EntityRelationships rels = new EntityRelationships();
    EntityRelationshipArray relArr = new EntityRelationshipArray();
    EntityRelationship rel = new EntityRelationship();
    rel.setEntity(UrnUtils.getUrn(dpUrn));
    rel.setType("DataProductContains");
    relArr.add(rel);
    rels.setRelationships(relArr);
    rels.setStart(0);
    rels.setCount(1);
    rels.setTotal(1);
    when(graphClient.getRelatedEntities(
            eq(neighbourAssetUrn), any(), any(), anyInt(), anyInt(), anyString()))
        .thenReturn(rels);
  }
}
