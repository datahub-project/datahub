package com.linkedin.datahub.upgrade.system.restoreindices.columnlineage;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.metadata.query.ExtraInfoArray;
import com.linkedin.metadata.query.ListResultMetadata;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class RestoreColumnLineageIndicesStepTest {

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
  public void testSkipAlwaysFalse() {
    RestoreColumnLineageIndicesStep step = new RestoreColumnLineageIndicesStep(mockEntityService);
    assertFalse(step.skip(mockUpgradeContext));
  }

  @Test
  public void testExecutableSucceedsWhenNoAspects() {
    when(mockEntityService.listLatestAspects(any(), any(), any(), eq(0), any()))
        .thenReturn(emptyListResult());

    RestoreColumnLineageIndicesStep step = new RestoreColumnLineageIndicesStep(mockEntityService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService, never())
        .alwaysProduceMCLAsync(
            any(), any(), any(), any(), any(), any(), any(), any(), any(), any(), any());
    assertEquals(result.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void testExecutableEmitsMCLForEachUpstreamLineageRecord() throws Exception {
    Urn urn = Urn.createFromString("urn:li:dataset:(urn:li:dataPlatform:hive,db.table,PROD)");
    UpstreamLineage lineage = new UpstreamLineage();

    ExtraInfo extraInfo = new ExtraInfo().setUrn(urn);
    ListResultMetadata metadata =
        new ListResultMetadata().setExtraInfos(new ExtraInfoArray(List.of(extraInfo)));
    ListResult<com.linkedin.data.template.RecordTemplate> oneResult =
        new ListResult<>(List.of(lineage), metadata, 1, false, 0, 0, 1);

    when(mockEntityService.listLatestAspects(
            any(),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            eq(0),
            any()))
        .thenReturn(oneResult);
    when(mockEntityService.listLatestAspects(
            any(), eq(Constants.CHART_ENTITY_NAME), any(), eq(0), any()))
        .thenReturn(emptyListResult());
    when(mockEntityService.listLatestAspects(
            any(), eq(Constants.DASHBOARD_ENTITY_NAME), any(), eq(0), any()))
        .thenReturn(emptyListResult());

    com.linkedin.common.util.Pair mockPair =
        com.linkedin.common.util.Pair.of(CompletableFuture.completedFuture(null), null);
    when(mockEntityService.alwaysProduceMCLAsync(
            any(),
            eq(urn),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            any(),
            any(),
            eq(lineage),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE)))
        .thenReturn(mockPair);

    RestoreColumnLineageIndicesStep step = new RestoreColumnLineageIndicesStep(mockEntityService);
    UpgradeStepResult stepResult = step.executable().apply(mockUpgradeContext);

    verify(mockEntityService)
        .alwaysProduceMCLAsync(
            any(),
            eq(urn),
            eq(Constants.DATASET_ENTITY_NAME),
            eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            any(),
            any(),
            eq(lineage),
            any(),
            any(),
            any(AuditStamp.class),
            eq(ChangeType.RESTATE));
    assertEquals(stepResult.result(), DataHubUpgradeState.SUCCEEDED);
  }

  @Test
  public void testExecutableReturnsFailedOnException() {
    when(mockEntityService.listLatestAspects(any(), any(), any(), eq(0), any()))
        .thenThrow(new RuntimeException("db error"));

    RestoreColumnLineageIndicesStep step = new RestoreColumnLineageIndicesStep(mockEntityService);
    UpgradeStepResult result = step.executable().apply(mockUpgradeContext);

    assertEquals(result.result(), DataHubUpgradeState.FAILED);
  }

  @SuppressWarnings("unchecked")
  private static ListResult<com.linkedin.data.template.RecordTemplate> emptyListResult() {
    return new ListResult<>(
        List.of(), new ListResultMetadata().setExtraInfos(new ExtraInfoArray()), 0, false, 0, 0, 0);
  }
}
