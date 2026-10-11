package com.linkedin.datahub.upgrade.system.dataplatforms;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.UrnArray;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.dataplatform.DataPlatformInfo;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ListUrnsResult;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class IndexDataPlatformsStepTest {

  @Mock private EntityService<?> mockEntityService;
  @Mock private EntitySearchService mockEntitySearchService;
  @Mock private UpgradeContext mockUpgradeContext;
  @Mock private OperationContext mockOpContext;
  @Mock private EntityRegistry mockEntityRegistry;
  @Mock private EntitySpec mockEntitySpec;
  @Mock private AspectSpec mockAspectSpec;

  @BeforeMethod
  public void setup() {
    MockitoAnnotations.openMocks(this);
    when(mockUpgradeContext.opContext()).thenReturn(mockOpContext);
    when(mockOpContext.getEntityRegistry()).thenReturn(mockEntityRegistry);
    when(mockEntityRegistry.getEntitySpec(any())).thenReturn(mockEntitySpec);
    when(mockEntitySpec.getAspectSpec(any())).thenReturn(mockAspectSpec);
  }

  @Test
  public void testSkipReturnsFalseWhenNoPriorRun() throws Exception {
    when(mockEntityService.getEntityV2(any(), any(), any(), any())).thenReturn(null);
    IndexDataPlatformsStep step =
        new IndexDataPlatformsStep(mockEntityService, mockEntitySearchService);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsTrueWhenAlreadyRan() throws Exception {
    DataHubUpgradeResult upgradeResult = new DataHubUpgradeResult().setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(upgradeResult.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME, aspect));
    EntityResponse response = new EntityResponse().setAspects(aspectMap);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(response);
    IndexDataPlatformsStep step =
        new IndexDataPlatformsStep(mockEntityService, mockEntitySearchService);
    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceedsWhenNoPlatforms() throws Exception {
    ListUrnsResult emptyResult =
        new ListUrnsResult().setEntities(new UrnArray()).setTotal(0).setStart(0);
    when(mockEntityService.listUrns(any(), any(), eq(0), any())).thenReturn(emptyResult);

    IndexDataPlatformsStep step =
        new IndexDataPlatformsStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testExecutableEmitsMCLForEachPlatform() throws Exception {
    Urn dpUrn = Urn.createFromString("urn:li:dataPlatform:mysql");
    DataPlatformInfo dpInfo = new DataPlatformInfo();

    ListUrnsResult listResult =
        new ListUrnsResult().setEntities(new UrnArray(dpUrn)).setTotal(1).setStart(0);
    when(mockEntityService.listUrns(any(), eq(Constants.DATA_PLATFORM_ENTITY_NAME), eq(0), any()))
        .thenReturn(listResult);

    EnvelopedAspect envelopedAspect =
        new EnvelopedAspect().setValue(new Aspect(new com.linkedin.data.DataMap()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_PLATFORM_INFO_ASPECT_NAME, envelopedAspect));
    EntityResponse entityResponse = new EntityResponse().setUrn(dpUrn).setAspects(aspectMap);
    when(mockEntityService.getEntitiesV2(
            any(), eq(Constants.DATA_PLATFORM_ENTITY_NAME), any(), any()))
        .thenReturn(Map.of(dpUrn, entityResponse));

    com.linkedin.util.Pair mockPair =
        com.linkedin.util.Pair.of(CompletableFuture.completedFuture(null), null);
    when(mockEntityService.alwaysProduceMCLAsync(
            any(),
            eq(dpUrn),
            eq(Constants.DATA_PLATFORM_ENTITY_NAME),
            eq(Constants.DATA_PLATFORM_INFO_ASPECT_NAME),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE)))
        .thenReturn(mockPair);

    IndexDataPlatformsStep step =
        new IndexDataPlatformsStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult stepResult = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService)
        .alwaysProduceMCLAsync(
            any(),
            eq(dpUrn),
            eq(Constants.DATA_PLATFORM_ENTITY_NAME),
            eq(Constants.DATA_PLATFORM_INFO_ASPECT_NAME),
            any(),
            any(),
            any(),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE));
    assertEquals(stepResult.result(), DataHubUpgradeState.SUCCEEDED);
    verify(mockEntityService)
        .ingestProposal(
            any(),
            argThat(p -> Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME.equals(p.getAspectName())),
            any(AuditStamp.class),
            eq(false));
    verify(mockEntityService)
        .ingestProposal(
            any(),
            argThat(p -> Constants.DATA_HUB_UPGRADE_RESULT_ASPECT_NAME.equals(p.getAspectName())),
            any(AuditStamp.class),
            eq(false));
  }

  @Test
  public void testExecutableReturnsFailedOnException() throws Exception {
    when(mockEntityService.listUrns(any(), any(), eq(0), any()))
        .thenThrow(new RuntimeException("db error"));

    IndexDataPlatformsStep step =
        new IndexDataPlatformsStep(mockEntityService, mockEntitySearchService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService).deleteUrn(any(), any(Urn.class));
  }
}
