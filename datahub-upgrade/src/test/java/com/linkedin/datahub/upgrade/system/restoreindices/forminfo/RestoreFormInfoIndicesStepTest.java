package com.linkedin.datahub.upgrade.system.restoreindices.forminfo;

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
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.form.FormInfo;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.upgrade.DataHubUpgradeRequest;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreFormInfoIndicesStepTest {

  @Mock private EntityService<?> mockEntityService;
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
    RestoreFormInfoIndicesStep step = new RestoreFormInfoIndicesStep(mockEntityService);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsTrueWhenAlreadyRan() throws Exception {
    DataHubUpgradeRequest upgradeRequest =
        new DataHubUpgradeRequest().setVersion("2").setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, aspect));
    EntityResponse response = new EntityResponse().setAspects(aspectMap);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(response);
    RestoreFormInfoIndicesStep step = new RestoreFormInfoIndicesStep(mockEntityService);
    assertTrue(step.skip(mockUpgradeContext));
  }

  @Test
  public void testSkipReturnsFalseWhenVersionMismatch() throws Exception {
    DataHubUpgradeRequest upgradeRequest =
        new DataHubUpgradeRequest().setVersion("1").setTimestampMs(0L);
    EnvelopedAspect aspect = new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data()));
    EnvelopedAspectMap aspectMap =
        new EnvelopedAspectMap(Map.of(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, aspect));
    EntityResponse response = new EntityResponse().setAspects(aspectMap);
    when(mockEntityService.getEntityV2(
            any(), eq(Constants.DATA_HUB_UPGRADE_ENTITY_NAME), any(), any()))
        .thenReturn(response);
    RestoreFormInfoIndicesStep step = new RestoreFormInfoIndicesStep(mockEntityService);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceedsWhenNoFormInfoAspects() {
    when(mockEntityService.listLatestAspects(any(), any(), any(), eq(0), any()))
        .thenReturn(emptyListResult());

    RestoreFormInfoIndicesStep step = new RestoreFormInfoIndicesStep(mockEntityService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testExecutableEmitsMCLForEachFormInfoRecord() throws Exception {
    Urn urn = Urn.createFromString("urn:li:form:test-form");
    FormInfo formInfo = new FormInfo();

    ExtraInfo extraInfo = new ExtraInfo().setUrn(urn);
    ListResultMetadata metadata =
        new ListResultMetadata().setExtraInfos(new ExtraInfoArray(List.of(extraInfo)));
    ListResult<com.linkedin.data.template.RecordTemplate> oneResult =
        new ListResult<>(List.of(formInfo), metadata, 0, false, 1, 0, 1000);

    when(mockEntityService.listLatestAspects(
            any(),
            eq(Constants.FORM_ENTITY_NAME),
            eq(Constants.FORM_INFO_ASPECT_NAME),
            eq(0),
            any()))
        .thenReturn(oneResult);

    com.linkedin.util.Pair mockPair =
        com.linkedin.util.Pair.of(CompletableFuture.completedFuture(null), null);
    when(mockEntityService.alwaysProduceMCLAsync(
            any(),
            eq(urn),
            eq(Constants.FORM_ENTITY_NAME),
            eq(Constants.FORM_INFO_ASPECT_NAME),
            any(),
            any(),
            eq(formInfo),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE)))
        .thenReturn(mockPair);

    RestoreFormInfoIndicesStep step = new RestoreFormInfoIndicesStep(mockEntityService);
    UpgradeStepResult stepResult = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService)
        .alwaysProduceMCLAsync(
            any(),
            eq(urn),
            eq(Constants.FORM_ENTITY_NAME),
            eq(Constants.FORM_INFO_ASPECT_NAME),
            any(),
            any(),
            eq(formInfo),
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
  public void testExecutableReturnsFailedOnException() {
    when(mockEntityService.listLatestAspects(any(), any(), any(), eq(0), any()))
        .thenThrow(new RuntimeException("db error"));

    RestoreFormInfoIndicesStep step = new RestoreFormInfoIndicesStep(mockEntityService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
    verify(mockEntityService).deleteUrn(any(), any(Urn.class));
  }

  @SuppressWarnings("unchecked")
  private static ListResult<com.linkedin.data.template.RecordTemplate> emptyListResult() {
    return new ListResult<>(
        List.of(), new ListResultMetadata().setExtraInfos(new ExtraInfoArray()), 0, false, 0, 0, 0);
  }
}
