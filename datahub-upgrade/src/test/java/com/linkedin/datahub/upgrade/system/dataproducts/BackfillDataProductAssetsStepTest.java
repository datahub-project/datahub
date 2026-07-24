package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.metadata.Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCTS_ASPECT_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.dataproduct.DataProductAssociation;
import com.linkedin.dataproduct.DataProductAssociationArray;
import com.linkedin.dataproduct.DataProductProperties;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BackfillDataProductAssetsStepTest {

  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:ads");
  private static final Urn DATASET_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");
  private static final Urn DATASET_2 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_deleted,PROD)");

  private BackfillDataProductAssetsStep step;
  private EntityService<?> entityService;
  private SearchService searchService;
  private OperationContext opContext;
  private UpgradeContext upgradeContext;

  @BeforeMethod
  public void setUp() {
    entityService = mock(EntityService.class);
    searchService = mock(SearchService.class);
    opContext = TestOperationContexts.systemContextNoValidate();
    upgradeContext = mock(UpgradeContext.class);
    when(upgradeContext.opContext()).thenReturn(opContext);

    step = new BackfillDataProductAssetsStep(opContext, entityService, searchService, 100);
  }

  private void mockScrollReturns(SearchEntityArray entities, String nextScrollId) {
    ScrollResult scrollResult = mock(ScrollResult.class);
    when(scrollResult.getNumEntities()).thenReturn(entities.size());
    when(scrollResult.getEntities()).thenReturn(entities);
    when(scrollResult.getScrollId()).thenReturn(nextScrollId);
    when(searchService.scrollAcrossEntities(
            any(OperationContext.class), any(), any(), any(), any(), any(), any(), any(), any()))
        .thenReturn(scrollResult);
  }

  private static SearchEntityArray oneProduct() {
    SearchEntityArray entities = new SearchEntityArray();
    entities.add(new SearchEntity().setEntity(PRODUCT_URN));
    return entities;
  }

  private void mockProductWithAssets(Urn... assets) throws Exception {
    DataProductAssociationArray associations = new DataProductAssociationArray();
    for (Urn asset : assets) {
      associations.add(new DataProductAssociation().setDestinationUrn(asset));
    }
    DataProductProperties properties = new DataProductProperties().setAssets(associations);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(properties.data()));
    EntityResponse response =
        new EntityResponse()
            .setUrn(PRODUCT_URN)
            .setEntityName(PRODUCT_URN.getEntityType())
            .setAspects(
                new EnvelopedAspectMap(Map.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME, aspect)));
    when(entityService.getEntityV2(
            any(OperationContext.class),
            eq(PRODUCT_URN.getEntityType()),
            eq(PRODUCT_URN),
            eq(java.util.Collections.singleton(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(response);
  }

  /**
   * Captures the membership PATCH proposals emitted onto assets (ignores the upgrade-result MCP).
   */
  private List<MetadataChangeProposal> capturePatchProposals() {
    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, org.mockito.Mockito.atLeast(0))
        .ingestProposal(any(OperationContext.class), captor.capture(), any(), anyBoolean());
    return captor.getAllValues().stream()
        .filter(mcp -> mcp.getChangeType() == ChangeType.PATCH)
        .filter(mcp -> DATA_PRODUCTS_ASPECT_NAME.equals(mcp.getAspectName()))
        .collect(Collectors.toList());
  }

  @Test
  public void testId() {
    assertEquals(step.id(), "BackfillDataProductAssetsStep");
  }

  @Test
  public void testIsOptional() {
    assertTrue(step.isOptional());
  }

  @Test
  public void testSkipWhenPreviouslyRun() {
    when(entityService.exists(
            eq(opContext), any(Urn.class), eq(DATA_HUB_UPGRADE_RESULT_ASPECT_NAME), eq(true)))
        .thenReturn(true);

    assertTrue(step.skip(upgradeContext));
  }

  @Test
  public void testExecutableBackfillsEachMemberAsset() throws Exception {
    mockScrollReturns(oneProduct(), null);
    mockProductWithAssets(DATASET_1, DATASET_2);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    List<MetadataChangeProposal> patches = capturePatchProposals();
    assertEquals(patches.size(), 2);
    Set<Urn> targets =
        patches.stream().map(MetadataChangeProposal::getEntityUrn).collect(Collectors.toSet());
    assertEquals(targets, Set.of(DATASET_1, DATASET_2));
    for (MetadataChangeProposal mcp : patches) {
      assertEquals(mcp.getAspect().getContentType(), "application/json-patch+json");
      assertEquals(mcp.getEntityType(), Constants.DATASET_ENTITY_NAME);
    }
  }

  @Test
  public void testExecutableWithNoDataProducts() {
    mockScrollReturns(new SearchEntityArray(), null);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertTrue(capturePatchProposals().isEmpty());
  }

  @Test
  public void testExecutableSkipsProductWithoutProperties() throws Exception {
    mockScrollReturns(oneProduct(), null);
    when(entityService.getEntityV2(any(OperationContext.class), any(), any(Urn.class), any()))
        .thenReturn(null);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
    assertTrue(capturePatchProposals().isEmpty());
    verify(entityService, never())
        .ingestProposal(
            any(OperationContext.class),
            org.mockito.ArgumentMatchers.argThat(
                mcp -> mcp != null && mcp.getChangeType() == ChangeType.PATCH),
            any(),
            anyBoolean());
  }
}
