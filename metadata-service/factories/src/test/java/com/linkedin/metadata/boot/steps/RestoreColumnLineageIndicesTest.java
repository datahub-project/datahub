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
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.EntitySpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchEntityArray;
import com.linkedin.metadata.search.SearchResult;
import com.linkedin.mxe.MetadataChangeProposal;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

public class RestoreColumnLineageIndicesTest {

  private static final String VERSION_1 = "1";
  private static final String VERSION_2 = "2";
  private static final String COLUMN_LINEAGE_UPGRADE_URN =
      String.format("urn:li:%s:%s", Constants.DATA_HUB_UPGRADE_ENTITY_NAME, "restore-column-lineage-indices");
  private final Urn datasetUrn = UrnUtils.getUrn("urn:li:dataset:(urn:li:dataPlatform:hdfs,SampleHdfsDataset,PROD)");
  private final Urn chartUrn = UrnUtils.getUrn("urn:li:chart:(looker,dashboard_elements.1)");
  private final Urn dashboardUrn = UrnUtils.getUrn("urn:li:dashboard:(looker,dashboards.thelook::web_analytics_overview)");

  @Test
  public void testExecuteFirstTime() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    mockGetUpgradeStep(false, VERSION_1, mockService);
    mockGetUpstreamLineage(datasetUrn, mockSearchService, mockService);
    mockGetInputFields(chartUrn, Constants.CHART_ENTITY_NAME, mockSearchService, mockService);
    mockGetInputFields(dashboardUrn, Constants.DASHBOARD_ENTITY_NAME, mockSearchService, mockService);

    AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    RestoreColumnLineageIndices restoreIndicesStep = new RestoreColumnLineageIndices(mockService, mockSearchService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DATASET_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.CHART_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DASHBOARD_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(2)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(datasetUrn),
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(chartUrn),
        Mockito.eq(Constants.CHART_ENTITY_NAME),
        Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(dashboardUrn),
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
  }

  @Test
  public void testExecuteWithNewVersion() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    mockGetUpgradeStep(true, VERSION_2, mockService);
    mockGetUpstreamLineage(datasetUrn, mockSearchService, mockService);
    mockGetInputFields(chartUrn, Constants.CHART_ENTITY_NAME, mockSearchService, mockService);
    mockGetInputFields(dashboardUrn, Constants.DASHBOARD_ENTITY_NAME, mockSearchService, mockService);

    AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    RestoreColumnLineageIndices restoreIndicesStep = new RestoreColumnLineageIndices(mockService, mockSearchService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DATASET_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.CHART_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(1)).getEntitySpec(Constants.DASHBOARD_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(2)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(datasetUrn),
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(chartUrn),
        Mockito.eq(Constants.CHART_ENTITY_NAME),
        Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(1)).produceMetadataChangeLog(
        Mockito.eq(dashboardUrn),
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
  }

  @Test
  public void testDoesNotExecuteWithSameVersion() throws Exception {
    final EntityService mockService = Mockito.mock(EntityService.class);
    final EntitySearchService mockSearchService = Mockito.mock(EntitySearchService.class);
    final EntityRegistry mockRegistry = Mockito.mock(EntityRegistry.class);

    mockGetUpgradeStep(true, VERSION_1, mockService);
    mockGetUpstreamLineage(datasetUrn, mockSearchService, mockService);
    mockGetInputFields(chartUrn, Constants.CHART_ENTITY_NAME, mockSearchService, mockService);
    mockGetInputFields(dashboardUrn, Constants.DASHBOARD_ENTITY_NAME, mockSearchService, mockService);

    AspectSpec aspectSpec = mockAspectSpecs(mockRegistry);

    RestoreColumnLineageIndices restoreIndicesStep = new RestoreColumnLineageIndices(mockService, mockSearchService, mockRegistry);
    restoreIndicesStep.execute();

    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.DATASET_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.CHART_ENTITY_NAME);
    Mockito.verify(mockRegistry, Mockito.times(0)).getEntitySpec(Constants.DASHBOARD_ENTITY_NAME);
    // creates upgradeRequest and upgradeResult aspects
    Mockito.verify(mockService, Mockito.times(0)).ingestProposal(
        Mockito.any(MetadataChangeProposal.class),
        Mockito.any(AuditStamp.class),
        Mockito.eq(false)
    );
    Mockito.verify(mockService, Mockito.times(0)).produceMetadataChangeLog(
        Mockito.eq(datasetUrn),
        Mockito.eq(Constants.DATASET_ENTITY_NAME),
        Mockito.eq(Constants.UPSTREAM_LINEAGE_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(0)).produceMetadataChangeLog(
        Mockito.eq(chartUrn),
        Mockito.eq(Constants.CHART_ENTITY_NAME),
        Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
    Mockito.verify(mockService, Mockito.times(0)).produceMetadataChangeLog(
        Mockito.eq(dashboardUrn),
        Mockito.eq(Constants.DASHBOARD_ENTITY_NAME),
        Mockito.eq(Constants.INPUT_FIELDS_ASPECT_NAME),
        Mockito.eq(aspectSpec),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(null),
        Mockito.eq(null),
        Mockito.any(),
        Mockito.eq(ChangeType.RESTATE)
    );
  }

  private void mockGetUpstreamLineage(Urn datasetUrn, EntitySearchService mockSearchService, EntityService mockService) throws Exception {
    Map<String, EnvelopedAspect> upstreamLineageAspects = new HashMap<>();
    upstreamLineageAspects.put(Constants.UPSTREAM_LINEAGE_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(new UpstreamLineage().data())));
    Map<Urn, EntityResponse> upstreamLineageResponses = new HashMap<>();
    upstreamLineageResponses.put(datasetUrn, new EntityResponse().setUrn(datasetUrn).setAspects(new EnvelopedAspectMap(upstreamLineageAspects)));
    Mockito.when(mockSearchService.search(Constants.DATASET_ENTITY_NAME, "", null, null, 0, 1000))
        .thenReturn(new SearchResult().setNumEntities(1).setEntities(new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(datasetUrn)))));
    Mockito.when(mockService.getEntitiesV2(
            Constants.DATASET_ENTITY_NAME,
            new HashSet<>(Collections.singleton(datasetUrn)),
            Collections.singleton(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)))
        .thenReturn(upstreamLineageResponses);
  }

  private void mockGetInputFields(Urn entityUrn, String entityName, EntitySearchService mockSearchService, EntityService mockService) throws Exception {
    Map<String, EnvelopedAspect> inputFieldsAspects = new HashMap<>();
    inputFieldsAspects.put(Constants.INPUT_FIELDS_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(new InputFields().data())));
    Map<Urn, EntityResponse> inputFieldsResponses = new HashMap<>();
    inputFieldsResponses.put(entityUrn, new EntityResponse().setUrn(entityUrn).setAspects(new EnvelopedAspectMap(inputFieldsAspects)));
    Mockito.when(mockSearchService.search(entityName, "", null, null, 0, 1000))
        .thenReturn(new SearchResult().setNumEntities(1).setEntities(new SearchEntityArray(ImmutableList.of(new SearchEntity().setEntity(entityUrn)))));
    Mockito.when(mockService.getEntitiesV2(
            entityName,
            new HashSet<>(Collections.singleton(entityUrn)),
            Collections.singleton(Constants.INPUT_FIELDS_ASPECT_NAME)
        ))
        .thenReturn(inputFieldsResponses);
  }

  private AspectSpec mockAspectSpecs(EntityRegistry mockRegistry) {
    EntitySpec entitySpec = Mockito.mock(EntitySpec.class);
    AspectSpec aspectSpec = Mockito.mock(AspectSpec.class);
    //  Mock for upstreamLineage
    Mockito.when(mockRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)).thenReturn(aspectSpec);
    //  Mock inputFields for charts
    Mockito.when(mockRegistry.getEntitySpec(Constants.CHART_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME)).thenReturn(aspectSpec);
    //  Mock inputFields for dashboards
    Mockito.when(mockRegistry.getEntitySpec(Constants.DASHBOARD_ENTITY_NAME)).thenReturn(entitySpec);
    Mockito.when(entitySpec.getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME)).thenReturn(aspectSpec);

    return aspectSpec;
  }

  private void mockGetUpgradeStep(boolean shouldReturnResponse, String version, EntityService mockService) throws Exception {

    final Urn upgradeEntityUrn = UrnUtils.getUrn(COLUMN_LINEAGE_UPGRADE_URN);
    com.linkedin.upgrade.DataHubUpgradeRequest upgradeRequest = new com.linkedin.upgrade.DataHubUpgradeRequest().setVersion(version);
    Map<String, EnvelopedAspect> upgradeRequestAspects = new HashMap<>();
    upgradeRequestAspects.put(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME, new EnvelopedAspect().setValue(new Aspect(upgradeRequest.data())));
    EntityResponse response = new EntityResponse().setAspects(new EnvelopedAspectMap(upgradeRequestAspects));
    Mockito.when(mockService.getEntityV2(
        Constants.DATA_HUB_UPGRADE_ENTITY_NAME,
        upgradeEntityUrn,
        Collections.singleton(Constants.DATA_HUB_UPGRADE_REQUEST_ASPECT_NAME)
    )).thenReturn(shouldReturnResponse ? response : null);
  }
}
