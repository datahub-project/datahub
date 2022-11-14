package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.entity.EntityResponse;
import com.linkedin.entity.EnvelopedAspectMap;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.search.EntitySearchService;
import com.linkedin.metadata.search.SearchEntity;
import com.linkedin.metadata.search.SearchResult;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.linkedin.metadata.Constants.UPSTREAM_LINEAGE_ASPECT_NAME;

@Slf4j
public class RestoreColumnLineageIndices extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "restore-column-lineage-indices";
  private static final Integer BATCH_SIZE = 1000;

  private final EntitySearchService _entitySearchService;
  private final EntityRegistry _entityRegistry;

  public RestoreColumnLineageIndices(EntityService entityService, EntitySearchService entitySearchService,
                                EntityRegistry entityRegistry) {
    super(entityService, VERSION, UPGRADE_ID);
    _entitySearchService = entitySearchService;
    _entityRegistry = entityRegistry;
  }

  @Override
  public void upgrade() throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp().setActor(Urn.createFromString(Constants.SYSTEM_ACTOR)).setTime(System.currentTimeMillis());

    final int totalUpstreamLineageCount = getAndRestoreUpstreamLineageIndices(0, auditStamp);
    int upstreamLineageCount = BATCH_SIZE;
    while (upstreamLineageCount < totalUpstreamLineageCount) {
      getAndRestoreUpstreamLineageIndices(upstreamLineageCount, auditStamp);
      upstreamLineageCount += BATCH_SIZE;
    }

    final int totalChartInputFieldsCount = getAndRestoreInputFieldsIndices(Constants.CHART_ENTITY_NAME, 0, auditStamp);
    int chartInputFieldsCount = BATCH_SIZE;
    while (chartInputFieldsCount < totalChartInputFieldsCount) {
      getAndRestoreInputFieldsIndices(Constants.CHART_ENTITY_NAME, chartInputFieldsCount, auditStamp);
      chartInputFieldsCount += BATCH_SIZE;
    }

    final int totalDashboardInputFieldsCount = getAndRestoreInputFieldsIndices(Constants.DASHBOARD_ENTITY_NAME, 0, auditStamp);
    int dashboardInputFieldsCount = BATCH_SIZE;
    while (dashboardInputFieldsCount < totalDashboardInputFieldsCount) {
      getAndRestoreInputFieldsIndices(Constants.DASHBOARD_ENTITY_NAME, dashboardInputFieldsCount, auditStamp);
      dashboardInputFieldsCount += BATCH_SIZE;
    }
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private int getAndRestoreUpstreamLineageIndices(int start, AuditStamp auditStamp)
      throws Exception {
    final AspectSpec upstreamLineageAspectSpec = _entityRegistry.getEntitySpec(Constants.DATASET_ENTITY_NAME)
        .getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);
    SearchResult datasetResult =
        _entitySearchService.search(Constants.DATASET_ENTITY_NAME, "", null, null, start, BATCH_SIZE);
    List<Urn> datasetUrns = datasetResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
    if (datasetUrns.size() == 0) {
      return 0;
    }
    final Map<Urn, EntityResponse> upstreamLineageResponses =
        _entityService.getEntitiesV2(Constants.DATASET_ENTITY_NAME, new HashSet<>(datasetUrns),
            Collections.singleton(Constants.UPSTREAM_LINEAGE_ASPECT_NAME)
        );

    //  Loop over Datasets and produce changelog
    for (Urn datasetUrn : datasetUrns) {
      EntityResponse upstreamLineageResponse = upstreamLineageResponses.get(datasetUrn);
      if (upstreamLineageResponse == null) {
        log.warn("Dataset not in set of entity responses {}", datasetUrn);
        continue;
      }
      UpstreamLineage upstreamLineage = mapUpstreamLineage(upstreamLineageResponse);
      if (upstreamLineage == null) {
        log.warn("Received null upstreamLineage for urn {}", datasetUrn);
        continue;
      }

      _entityService.produceMetadataChangeLog(
          datasetUrn,
          Constants.DATASET_ENTITY_NAME,
          Constants.UPSTREAM_LINEAGE_ASPECT_NAME,
          upstreamLineageAspectSpec,
          null,
          upstreamLineage,
          null,
          null,
          auditStamp,
          ChangeType.RESTATE);
    }

    return datasetResult.getNumEntities();
  }

  private int getAndRestoreInputFieldsIndices(String entityName, int start, AuditStamp auditStamp) throws Exception {
    final AspectSpec inputFieldsAspectSpec = _entityRegistry.getEntitySpec(entityName)
        .getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME);
    SearchResult entityResult =
        _entitySearchService.search(entityName, "", null, null, start, BATCH_SIZE);
    List<Urn> entityUrns = entityResult.getEntities().stream().map(SearchEntity::getEntity).collect(Collectors.toList());
    if (entityUrns.size() == 0) {
      return 0;
    }
    final Map<Urn, EntityResponse> inputFieldsResponses =
        _entityService.getEntitiesV2(entityName, new HashSet<>(entityUrns),
            Collections.singleton(Constants.INPUT_FIELDS_ASPECT_NAME)
        );

    //  Loop over Datasets and produce changelog
    for (Urn entityUrn : entityUrns) {
      EntityResponse inputFieldsResponse = inputFieldsResponses.get(entityUrn);
      if (inputFieldsResponse == null) {
        log.warn("Entity not in set of entity responses {}", entityUrn);
        continue;
      }
      InputFields inputFields = mapInputFields(inputFieldsResponse);
      if (inputFields == null) {
        log.warn("Received null inputFields for urn {}", entityUrn);
        continue;
      }

      _entityService.produceMetadataChangeLog(
          entityUrn,
          entityName,
          Constants.INPUT_FIELDS_ASPECT_NAME,
          inputFieldsAspectSpec,
          null,
          inputFields,
          null,
          null,
          auditStamp,
          ChangeType.RESTATE);
    }

    return entityResult.getNumEntities();
  }

  private UpstreamLineage mapUpstreamLineage(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(UPSTREAM_LINEAGE_ASPECT_NAME)) {
      return null;
    }

    return new UpstreamLineage(aspectMap.get(Constants.UPSTREAM_LINEAGE_ASPECT_NAME).getValue().data());
  }

  private InputFields mapInputFields(EntityResponse entityResponse) {
    EnvelopedAspectMap aspectMap = entityResponse.getAspects();
    if (!aspectMap.containsKey(Constants.INPUT_FIELDS_ASPECT_NAME)) {
      return null;
    }

    return new InputFields(aspectMap.get(Constants.INPUT_FIELDS_ASPECT_NAME).getValue().data());
  }
}
