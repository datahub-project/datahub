package com.linkedin.datahub.upgrade.system.dataproducts;

import static com.linkedin.metadata.Constants.DATA_PRODUCT_ENTITY_NAME;
import static com.linkedin.metadata.Constants.DATA_PRODUCT_PROPERTIES_ASPECT_NAME;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
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
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.search.ScrollResult;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchService;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import io.datahubproject.test.metadata.context.TestOperationContexts;
import java.util.Map;
import java.util.Set;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ResyncDataProductAssetsStepTest {

  private static final Urn PRODUCT_URN = UrnUtils.getUrn("urn:li:dataProduct:ads");
  private static final Urn DATASET_1 =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hive,fct_users_created,PROD)");

  private ResyncDataProductAssetsStep step;
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

    step = new ResyncDataProductAssetsStep(opContext, entityService, searchService, true, 100);
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

  private EntityResponse propertiesResponse(Urn... assets) {
    DataProductProperties props = new DataProductProperties();
    DataProductAssociationArray associations = new DataProductAssociationArray();
    for (Urn asset : assets) {
      DataProductAssociation association = new DataProductAssociation();
      association.setDestinationUrn(asset);
      associations.add(association);
    }
    props.setAssets(associations);

    EnvelopedAspect enveloped = new EnvelopedAspect();
    enveloped.setValue(new Aspect(props.data()));
    EnvelopedAspectMap map = new EnvelopedAspectMap();
    map.put(DATA_PRODUCT_PROPERTIES_ASPECT_NAME, enveloped);

    EntityResponse response = new EntityResponse();
    response.setUrn(PRODUCT_URN);
    response.setEntityName(DATA_PRODUCT_ENTITY_NAME);
    response.setAspects(map);
    return response;
  }

  @Test
  public void testSkipFalseWhenReprocessEnabled() {
    assertFalse(step.skip(upgradeContext));
  }

  @Test
  public void testSkipTrueWhenReprocessDisabled() {
    ResyncDataProductAssetsStep disabled =
        new ResyncDataProductAssetsStep(opContext, entityService, searchService, false, 100);
    assertEquals(disabled.skip(upgradeContext), true);
  }

  @Test
  public void testRestatesDataProductPropertiesInBatch() throws Exception {
    SearchEntity searchEntity = new SearchEntity().setEntity(PRODUCT_URN);
    mockScrollReturns(new SearchEntityArray(searchEntity), null);

    when(entityService.getEntitiesV2(
            any(OperationContext.class),
            eq(DATA_PRODUCT_ENTITY_NAME),
            eq(Set.of(PRODUCT_URN)),
            eq(Set.of(DATA_PRODUCT_PROPERTIES_ASPECT_NAME))))
        .thenReturn(Map.of(PRODUCT_URN, propertiesResponse(DATASET_1)));

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, atLeastOnce())
        .ingestProposal(any(OperationContext.class), captor.capture(), any(), anyBoolean());

    MetadataChangeProposal proposal =
        captor.getAllValues().stream()
            .filter(p -> DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(p.getAspectName()))
            .findFirst()
            .orElseThrow();
    assertEquals(proposal.getEntityUrn(), PRODUCT_URN);
    assertEquals(proposal.getChangeType(), ChangeType.UPSERT);
  }

  @Test
  public void testEmptyScrollDoesNotIngestDataProductProperties() throws Exception {
    mockScrollReturns(new SearchEntityArray(), null);

    UpgradeStepResult result = step.executable().apply(upgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);

    ArgumentCaptor<MetadataChangeProposal> captor =
        ArgumentCaptor.forClass(MetadataChangeProposal.class);
    verify(entityService, atLeastOnce())
        .ingestProposal(any(OperationContext.class), captor.capture(), any(), anyBoolean());
    assertTrue(
        captor.getAllValues().stream()
            .noneMatch(p -> DATA_PRODUCT_PROPERTIES_ASPECT_NAME.equals(p.getAspectName())));
  }
}
