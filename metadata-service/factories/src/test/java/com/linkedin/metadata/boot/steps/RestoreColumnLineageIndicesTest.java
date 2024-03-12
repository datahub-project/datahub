package com.linkedin.metadata.boot.steps;

import com.google.common.collect.ImmutableList;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.Aspect;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.entity.EnvelopedAspectMap;
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
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.util.Pair;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import org.mockito.Mockito;
import org.testng.annotations.Test;

public class RestoreColumnLineageIndicesTest {

  private static final String VERSION_1 = "1";
  private static final String VERSION_2 = "2";
  private static final String COLUMN_LINEAGE_UPGRADE_URN =
      String.format(
          "urn:li:%s:%s", Constants.DATA_HUB_UPGRADE_ENTITY_NAME, "restore-column-lineage-indices");
  private final Urn datasetUrn =
      UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
  private final Urn chartUrn = UrnUtils.getUrn("urn:li:chart:(looker,dashboard_elements.1)");
  private final Urn dashboardUrn =
      UrnUtils.getUrn("urn:li:dashboard:(looker,dashboards.thelook::web_analytics_overview)");

  @Test
  public void testExecuteFirstTime() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    mockGetUpgradeStep(false, VERSION_1, mockService);
    mockGetUpstreamLineage(datasetUrn, mockService);
    mockGetInputFields(chartUrn, Constants.CHART_ENTITY_NAME, mockService);
    mockGetInputFields(dashboardUrn, Constants.DASHBOARD_ENTITY_NAME, mockService);

    final AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    final RestoreColumnLineageIndices restoreIndicesStep =
        new RestoreColumnLineageIndices(mockService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DATASET_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.CHART_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DASHBOARD_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(2))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.eq(datasetUrn),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.eq(chartUrn),
            Mockito.eq(Constants.CHART_ENTITY_NAME),
            Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.eq(dashboardUrn),
            Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
            Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
  }

  @Test
  public void testExecuteWithNewVersion() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    mockGetUpgradeStep(true, VERSION_2, mockService);
    mockGetUpstreamLineage(datasetUrn, mockService);
    mockGetInputFields(chartUrn, Constants.CHART_ENTITY_NAME, mockService);
    mockGetInputFields(dashboardUrn, Constants.DASHBOARD_ENTITY_NAME, mockService);

    final AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    final RestoreColumnLineageIndices restoreIndicesStep =
        new RestoreColumnLineageIndices(mockService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DATASET_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.CHART_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DASHBOARD_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(2))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.eq(datasetUrn),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.eq(chartUrn),
            Mockito.eq(Constants.CHART_ENTITY_NAME),
            Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
    Mockito.verify(mockService, Mockito.times(1))
        .alwaysProduceMCLAsync(
            Mockito.eq(dashboardUrn),
            Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
            Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
  }

  @Test
  public void testDoesNotExecuteWithSameVersion() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    mockGetUpgradeStep(true, VERSION_1, mockService);
    mockGetUpstreamLineage(datasetUrn, mockService);
    mockGetInputFields(chartUrn, Constants.CHART_ENTITY_NAME, mockService);
    mockGetInputFields(dashboardUrn, Constants.DASHBOARD_ENTITY_NAME, mockService);

    final AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    final RestoreColumnLineageIndices restoreIndicesStep =
        new RestoreColumnLineageIndices(mockService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.DATASET_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.CHART_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.DASHBOARD_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(0))
        .ingestProposal(
            Mockito.any(MetadataChangeProposal.class),
            Mockito.any(AuditStamp.class),
            Mockito.eq(false));
    Mockito.verify(mockService, Mockito.times(0))
        .alwaysProduceMCLAsync(
            Mockito.eq(datasetUrn),
            Mockito.eq(Constants.DATASET_ENTITY_NAME),
            Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
    Mockito.verify(mockService, Mockito.times(0))
        .alwaysProduceMCLAsync(
            Mockito.eq(chartUrn),
            Mockito.eq(Constants.CHART_ENTITY_NAME),
            Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
    Mockito.verify(mockService, Mockito.times(0))
        .alwaysProduceMCLAsync(
            Mockito.eq(dashboardUrn),
            Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
            Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
            Mockito.eq(aspectSpec),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(null),
            Mockito.eq(null),
            Mockito.any(),
            Mockito.eq(ChangeType.RESTATE));
  }

  private void mockGetUpstreamLineage(@Nonnull Urn datasetUrn, @Nonnull EntityService mockService) {
    final List<ExtraInfo> extraInfos =
        ImmutableList.of(
            new ExtraInfo()
                .setUrn(datasetUrn)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp()
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                        .setTime(0L)));

    Mockito.when(
            mockService.alwaysProduceMCLAsync(
                Mockito.any(Urn.class),
                Mockito.anyString(),
                Mockito.anyString(),
                Mockito.any(AspectSpec.class),
                Mockito.eq(null),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(),
                Mockito.any(ChangeType.class)))
        .thenReturn(Pair.of(Mockito.mock(Future.class), false));

    Mockito.when(
            mockService.listLatestAspects(
                Mockito.eq(Constants.DATASET_ENTITY_NAME),
                Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new ListResult<>(
                ImmutableList.of(new UpstreamLineage()),
                new ListResultMetadata().setExtraInfos(new ExtraInfoArray(extraInfos)),
                1,
                false,
                1,
                1,
                1));
  }

  private void mockGetInputFields(
      @Nonnull Urn entityUrn, @Nonnull String entityName, @Nonnull EntityService mockService) {
    final List<ExtraInfo> extraInfos =
        ImmutableList.of(
            new ExtraInfo()
                .setUrn(entityUrn)
                .setVersion(0L)
                .setAudit(
                    new AuditStamp()
                        .setActor(UrnUtils.getUrn("urn:li:corpuser:test"))
                        .setTime(0L)));

    Mockito.when(
            mockService.listLatestAspects(
                Mockito.eq(entityName),
                Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
                Mockito.eq(0),
                Mockito.eq(1000)))
        .thenReturn(
            new ListResult<>(
                ImmutableList.of(new InputFields()),
                new ListResultMetadata().setExtraInfos(new ExtraInfoArray(extraInfos)),
                1,
                false,
                1,
                1,
                1));
  }

  private AspectSpec mockAspectSpecs(@Nonnull EntityRegistry mockRegistry) {
    final EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    final AspectSpec aspectSpec = Mockito.mock(AspectSpec.class);
    //  Mock for upstreamLineage
    Mockito.when(mockRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME))
        .thenReturn(aspectSpec);
    //  Mock inputFields for charts
    Mockito.when(mockRegistry.getEntitySpec(Constants.CHART_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME))
        .thenReturn(aspectSpec);
    //  Mock inputFields for dashboards
    Mockito.when(mockRegistry.getEntitySpec(Constants.DASHBOARD_ENTITY_NAME))
        .thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME))
        .thenReturn(aspectSpec);

    return aspectSpec;
  }

  private void mockGetUpgradeStep(
      boolean shouldReturnResponse, @Nonnull String version, @Nonnull EntityService mockService)
      throws Exception {

    final Urn upgradeEntityUrn = UrnUtils.getUrn(COLUMN_LINEAGE_UPGRADE_URN);
    final com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest =
        new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(version);
    final Map<String, EnvelopedAspect> upgradeRequestAspects = new HashMap<>();
    upgradeRequestAspects.put(
        Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME,
        new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data())));
    final EntityResponse response =
        new EntityResponse().setAspects(new EnvelopedAspectMap(upgradeRequestAspects));
    Mockito.when(
            mockService.getEntityV2(
                Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
                upgradeEntityUrn,
                Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)))
        .thenReturn(shouldReturnResponse ? response : null);
  }
}
