package com.linkedin.metadata.boot.steps;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.boot.UpgradeStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.query.ExtraInfo;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreColumnLineageIndices extends UpgradeStep {
  private static final String VERSION = "1";
  private static final String UPGRADE_ID = "restore-column-lineage-indices";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityRegistry _entityRegistry;

  public RestoreColumnLineageIndices(
      @Nonnull final EntityService entityService, @Nonnull final EntityRegistry entityRegistry) {
    super(entityService, VERSION, UPGRADE_ID);
    _entityRegistry = Objects.requireNonNull(entityRegistry, "entityRegistry must not be null");
  }

  @Override
  public void upgrade() throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    final int totalUpstreamLineageCount = getAndRestoreUpstreamLineageIndices(0, auditStamp);
    int upstreamLineageCount = BATCH_SIZE;
    while (upstreamLineageCount < totalUpstreamLineageCount) {
      getAndRestoreUpstreamLineageIndices(upstreamLineageCount, auditStamp);
      upstreamLineageCount += BATCH_SIZE;
    }

    final int totalChartInputFieldsCount =
        getAndRestoreInputFieldsIndices(Constants.CHART_ENTITY_NAME, 0, auditStamp);
    int chartInputFieldsCount = BATCH_SIZE;
    while (chartInputFieldsCount < totalChartInputFieldsCount) {
      getAndRestoreInputFieldsIndices(
          Constants.CHART_ENTITY_NAME, chartInputFieldsCount, auditStamp);
      chartInputFieldsCount += BATCH_SIZE;
    }

    final int totalDashboardInputFieldsCount =
        getAndRestoreInputFieldsIndices(Constants.DASHBOARD_ENTITY_NAME, 0, auditStamp);
    int dashboardInputFieldsCount = BATCH_SIZE;
    while (dashboardInputFieldsCount < totalDashboardInputFieldsCount) {
      getAndRestoreInputFieldsIndices(
          Constants.DASHBOARD_ENTITY_NAME, dashboardInputFieldsCount, auditStamp);
      dashboardInputFieldsCount += BATCH_SIZE;
    }
  }

  @Nonnull
  @Override
  public ExecutionMode getExecutionMode() {
    return ExecutionMode.ASYNC;
  }

  private int getAndRestoreUpstreamLineageIndices(int start, AuditStamp auditStamp) {
    final AspectSpec upstreamLineageAspectSpec =
        _entityRegistry
            .getEntitySpec(Constants.DATASET_ENTITY_NAME)
            .getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        _entityService.listLatestAspects(
            Constants.DATASET_ENTITY_NAME,
            Constants.UPSTREAM_LINEAGE_ASPECT_NAME,
            start,
            BATCH_SIZE);

    if (latestAspects.getTotalCount() == 0
        || latestAspects.getValues() == null
        || latestAspects.getMetadata() == null) {
      log.debug("Found 0 upstreamLineage aspects for datasets. Skipping migration.");
      return 0;
    }

    if (latestAspects.getValues().size() != latestAspects.getMetadata().getExtraInfos().size()) {
      // Bad result -- we should log that we cannot migrate this batch of upstreamLineages.
      log.warn(
          "Failed to match upstreamLineage aspects with corresponding urns. Found mismatched length between aspects ({})"
              + "and metadata ({}) for metadata {}",
          latestAspects.getValues().size(),
          latestAspects.getMetadata().getExtraInfos().size(),
          latestAspects.getMetadata());
      return latestAspects.getTotalCount();
    }

    List<Future<?>> futures = new LinkedList<>();
    for (int i = 0; i < latestAspects.getValues().size(); i++) {
      ExtraInfo info = latestAspects.getMetadata().getExtraInfos().get(i);
      RecordTemplate upstreamLineageRecord = latestAspects.getValues().get(i);
      Urn urn = info.getUrn();
      UpstreamLineage upstreamLineage = (UpstreamLineage) upstreamLineageRecord;
      if (upstreamLineage == null) {
        log.warn("Received null upstreamLineage for urn {}", urn);
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  urn,
                  Constants.DATASET_ENTITY_NAME,
                  Constants.UPSTREAM_LINEAGE_ASPECT_NAME,
                  upstreamLineageAspectSpec,
                  null,
                  upstreamLineage,
                  null,
                  null,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    return latestAspects.getTotalCount();
  }

  private int getAndRestoreInputFieldsIndices(String entityName, int start, AuditStamp auditStamp)
      throws Exception {
    final AspectSpec inputFieldsAspectSpec =
        _entityRegistry.getEntitySpec(entityName).getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        _entityService.listLatestAspects(
            entityName, Constants.INPUT_FIELDS_ASPECT_NAME, start, BATCH_SIZE);

    if (latestAspects.getTotalCount() == 0
        || latestAspects.getValues() == null
        || latestAspects.getMetadata() == null) {
      log.debug("Found 0 inputFields aspects. Skipping migration.");
      return 0;
    }

    if (latestAspects.getValues().size() != latestAspects.getMetadata().getExtraInfos().size()) {
      // Bad result -- we should log that we cannot migrate this batch of inputFields.
      log.warn(
          "Failed to match inputFields aspects with corresponding urns. Found mismatched length between aspects ({})"
              + "and metadata ({}) for metadata {}",
          latestAspects.getValues().size(),
          latestAspects.getMetadata().getExtraInfos().size(),
          latestAspects.getMetadata());
      return latestAspects.getTotalCount();
    }

    List<Future<?>> futures = new LinkedList<>();
    for (int i = 0; i < latestAspects.getValues().size(); i++) {
      ExtraInfo info = latestAspects.getMetadata().getExtraInfos().get(i);
      RecordTemplate inputFieldsRecord = latestAspects.getValues().get(i);
      Urn urn = info.getUrn();
      InputFields inputFields = (InputFields) inputFieldsRecord;
      if (inputFields == null) {
        log.warn("Received null inputFields for urn {}", urn);
        continue;
      }

      futures.add(
          _entityService
              .alwaysProduceMCLAsync(
                  urn,
                  entityName,
                  Constants.INPUT_FIELDS_ASPECT_NAME,
                  inputFieldsAspectSpec,
                  null,
                  inputFields,
                  null,
                  null,
                  auditStamp,
                  ChangeType.RESTATE)
              .getFirst());
    }

    futures.stream()
        .filter(Objects::nonNull)
        .forEach(
            f -> {
              try {
                f.get();
              } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
              }
            });

    return latestAspects.getTotalCount();
  }
}
