package com.linkedin.datahub.upgrade.system.restoreindices.columnlineage;

import com.linkedin.common.AuditStamp;
import com.linkedin.common.InputFields;
import com.linkedin.common.urn.Urn;
import com.linkedin.data.template.RecordTemplate;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.dataset.UpstreamLineage;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.ListResult;
import com.linkedin.metadata.models.AspectSpec;
import com.linkedin.metadata.query.ExtraInfo;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RestoreColumnLineageIndicesStep implements UpgradeStep {

  private static final String STEP_ID = "restore-column-lineage-indices";
  private static final Integer BATCH_SIZE = 1000;

  private final EntityService<?> _entityService;

  public RestoreColumnLineageIndicesStep(@Nonnull final EntityService<?> entityService) {
    _entityService = entityService;
  }

  @Override
  public String id() {
    return STEP_ID;
  }

  @Override
  public boolean isOptional() {
    return true;
  }

  @Override
  public boolean skip(final UpgradeContext context) {
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return context -> {
      try {
        execute(context.opContext());
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
      } catch (Exception e) {
        log.error("Failed to restore column lineage indices", e);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }
    };
  }

  private void execute(@Nonnull final OperationContext systemOperationContext) throws Exception {
    final AuditStamp auditStamp =
        new AuditStamp()
            .setActor(Urn.createFromString(Constants.SYSTEM_ACTOR))
            .setTime(System.currentTimeMillis());

    final int totalUpstreamLineageCount =
        getAndRestoreUpstreamLineageIndices(systemOperationContext, 0, auditStamp);
    int upstreamLineageCount = BATCH_SIZE;
    while (upstreamLineageCount < totalUpstreamLineageCount) {
      getAndRestoreUpstreamLineageIndices(systemOperationContext, upstreamLineageCount, auditStamp);
      upstreamLineageCount += BATCH_SIZE;
    }

    final int totalChartInputFieldsCount =
        getAndRestoreInputFieldsIndices(
            systemOperationContext, Constants.CHART_ENTITY_NAME, 0, auditStamp);
    int chartInputFieldsCount = BATCH_SIZE;
    while (chartInputFieldsCount < totalChartInputFieldsCount) {
      getAndRestoreInputFieldsIndices(
          systemOperationContext, Constants.CHART_ENTITY_NAME, chartInputFieldsCount, auditStamp);
      chartInputFieldsCount += BATCH_SIZE;
    }

    final int totalDashboardInputFieldsCount =
        getAndRestoreInputFieldsIndices(
            systemOperationContext, Constants.DASHBOARD_ENTITY_NAME, 0, auditStamp);
    int dashboardInputFieldsCount = BATCH_SIZE;
    while (dashboardInputFieldsCount < totalDashboardInputFieldsCount) {
      getAndRestoreInputFieldsIndices(
          systemOperationContext,
          Constants.DASHBOARD_ENTITY_NAME,
          dashboardInputFieldsCount,
          auditStamp);
      dashboardInputFieldsCount += BATCH_SIZE;
    }
  }

  private int getAndRestoreUpstreamLineageIndices(
      @Nonnull OperationContext systemOperationContext, int start, AuditStamp auditStamp) {
    final AspectSpec upstreamLineageAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(Constants.DATASET_ENTITY_NAME)
            .getAspectSpec(Constants.UPSTREAM_LINEAGE_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        _entityService.listLatestAspects(
            systemOperationContext,
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
                  systemOperationContext,
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

  private int getAndRestoreInputFieldsIndices(
      @Nonnull OperationContext systemOperationContext,
      String entityName,
      int start,
      AuditStamp auditStamp) {
    final AspectSpec inputFieldsAspectSpec =
        systemOperationContext
            .getEntityRegistry()
            .getEntitySpec(entityName)
            .getAspectSpec(Constants.INPUT_FIELDS_ASPECT_NAME);

    final ListResult<RecordTemplate> latestAspects =
        _entityService.listLatestAspects(
            systemOperationContext,
            entityName,
            Constants.INPUT_FIELDS_ASPECT_NAME,
            start,
            BATCH_SIZE);

    if (latestAspects.getTotalCount() == 0
        || latestAspects.getValues() == null
        || latestAspects.getMetadata() == null) {
      log.debug("Found 0 inputFields aspects. Skipping migration.");
      return 0;
    }

    if (latestAspects.getValues().size() != latestAspects.getMetadata().getExtraInfos().size()) {
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
                  systemOperationContext,
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
