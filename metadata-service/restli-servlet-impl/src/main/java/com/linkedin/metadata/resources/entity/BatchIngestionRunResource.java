package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.run.UnsafeEntityInfo;
import com.linkedin.metadata.run.UnsafeEntityInfoArray;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.IngestionRunSummary;
import com.linkedin.metadata.run.IngestionRunSummaryArray;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import io.opentelemetry.extension.annotations.WithSpan;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * resource for showing information and rolling back runs
 */
@Slf4j
@RestLiCollection(name = "runs", namespace = "com.linkedin.entity")
public class BatchIngestionRunResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {

  private static final Integer DEFAULT_OFFSET = 0;
  private static final Integer DEFAULT_PAGE_SIZE = 100;
  private static final Integer DEFAULT_UNSAFE_ENTITIES_PAGE_SIZE = 1000000;
  private static final boolean DEFAULT_INCLUDE_SOFT_DELETED = false;
  private static final boolean DEFAULT_HARD_DELETE = false;
  private static final Integer ELASTIC_MAX_PAGE_SIZE = 10000;
  private static final Integer ELASTIC_BATCH_DELETE_SLEEP_SEC = 5;

  @Inject
  @Named("systemMetadataService")
  private SystemMetadataService _systemMetadataService;

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  /**
   * Rolls back an ingestion run
   */
  @Action(name = "rollback")
  @Nonnull
  @WithSpan
  public Task<RollbackResponse> rollback(@ActionParam("runId") @Nonnull String runId,
                                         @ActionParam("dryRun") @Optional Boolean dryRun,
                                         @Deprecated @ActionParam("hardDelete") @Optional Boolean hardDelete,
                                         @ActionParam("safe") @Optional Boolean safe) {
    log.info("ROLLBACK RUN runId: {} dry run: {}", runId, dryRun);

    boolean doHardDelete = safe != null ? !safe : hardDelete != null ? hardDelete : DEFAULT_HARD_DELETE;

    if (safe != null && hardDelete != null) {
      log.warn("Both Safe & hardDelete flags were defined, honouring safe flag as hardDelete is deprecated");
    }

    return RestliUtil.toTask(() -> {
      if (runId.equals(EntityService.DEFAULT_RUN_ID)) {
        throw new IllegalArgumentException(String.format(
            "%s is a default run-id provided for non labeled ingestion runs. You cannot delete using this reserved run-id",
            runId));
      }
      RollbackResponse response = new RollbackResponse();
      List<AspectRowSummary> aspectRowsToDelete;
      aspectRowsToDelete = _systemMetadataService.findByRunId(runId, doHardDelete);

      log.info("found {} rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
      if (dryRun) {

        final Map<Boolean, List<AspectRowSummary>> aspectsSplitByIsKeyAspects = aspectRowsToDelete.stream()
                .collect(Collectors.partitioningBy(AspectRowSummary::isKeyAspect));

        final List<AspectRowSummary> keyAspects = aspectsSplitByIsKeyAspects.get(true);

        long entitiesDeleted = keyAspects.size();
        long aspectsReverted = aspectRowsToDelete.size();

        final long affectedEntities = aspectRowsToDelete.stream()
                .collect(Collectors.groupingBy(AspectRowSummary::getUrn)).keySet().size();

        final AspectRowSummaryArray rowSummaries = new AspectRowSummaryArray(
                aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size())));

        // If we are soft deleting, remove key aspects from count of aspects being deleted
        if (!doHardDelete) {
          aspectsReverted -= keyAspects.size();
          rowSummaries.removeIf(AspectRowSummary::isKeyAspect);
        }
        // Compute the aspects that exist referencing the key aspects we are deleting
        final List<AspectRowSummary> affectedAspectsList = keyAspects.stream()
                .map((AspectRowSummary urn) -> _systemMetadataService.findByUrn(urn.getUrn(), false))
                .flatMap(List::stream)
                .filter(row -> !row.getRunId().equals(runId) && !row.isKeyAspect()
                        && !row.getAspectName().equals(Constants.STATUS_ASPECT_NAME))
                .collect(Collectors.toList());

        long affectedAspects = affectedAspectsList.size();
        long unsafeEntitiesCount = affectedAspectsList.stream()
                .collect(Collectors.groupingBy(AspectRowSummary::getUrn)).keySet().size();

        final List<UnsafeEntityInfo> unsafeEntityInfos = affectedAspectsList.stream().map(AspectRowSummary::getUrn)
                .distinct()
                .map(urn -> {
                  UnsafeEntityInfo unsafeEntityInfo = new UnsafeEntityInfo();
                  unsafeEntityInfo.setUrn(urn);
                  return unsafeEntityInfo;
                })
                // Return at most 1 million rows
                .limit(DEFAULT_UNSAFE_ENTITIES_PAGE_SIZE)
                .collect(Collectors.toList());

        return response.setAspectsAffected(affectedAspects)
                .setAspectsReverted(aspectsReverted)
                .setEntitiesAffected(affectedEntities)
                .setEntitiesDeleted(entitiesDeleted)
                .setUnsafeEntitiesCount(unsafeEntitiesCount)
                .setUnsafeEntities(new UnsafeEntityInfoArray(unsafeEntityInfos))
                .setAspectRowSummaries(rowSummaries);
      }

      RollbackRunResult rollbackRunResult = _entityService.rollbackRun(aspectRowsToDelete, runId, doHardDelete);
      final List<AspectRowSummary> deletedRows = rollbackRunResult.getRowsRolledBack();
      int rowsDeletedFromEntityDeletion = rollbackRunResult.getRowsDeletedFromEntityDeletion();

      // since elastic limits how many rows we can access at once, we need to iteratively delete
      while (aspectRowsToDelete.size() >= ELASTIC_MAX_PAGE_SIZE) {
        sleep(ELASTIC_BATCH_DELETE_SLEEP_SEC);
        aspectRowsToDelete = _systemMetadataService.findByRunId(runId, doHardDelete);
        log.info("{} remaining rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
        log.info("deleting...");
        rollbackRunResult = _entityService.rollbackRun(aspectRowsToDelete, runId, doHardDelete);
        deletedRows.addAll(rollbackRunResult.getRowsRolledBack());
        rowsDeletedFromEntityDeletion += rollbackRunResult.getRowsDeletedFromEntityDeletion();
      }

      log.info("finished deleting {} rows", deletedRows.size());
      int aspectsReverted = deletedRows.size() + rowsDeletedFromEntityDeletion;

      final Map<Boolean, List<AspectRowSummary>> aspectsSplitByIsKeyAspects = aspectRowsToDelete.stream()
              .collect(Collectors.partitioningBy(AspectRowSummary::isKeyAspect));

      final List<AspectRowSummary> keyAspects = aspectsSplitByIsKeyAspects.get(true);

      final long entitiesDeleted = keyAspects.size();
      final long affectedEntities = deletedRows.stream()
              .collect(Collectors.groupingBy(AspectRowSummary::getUrn)).keySet().size();

      final AspectRowSummaryArray rowSummaries = new AspectRowSummaryArray(
              aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size())));

      log.info("computing aspects affected by this rollback...");
      // Compute the aspects that exist referencing the key aspects we are deleting
      final List<AspectRowSummary> affectedAspectsList = keyAspects.stream()
              .map((AspectRowSummary urn) -> _systemMetadataService.findByUrn(urn.getUrn(), false))
              .flatMap(List::stream)
              .filter(row -> !row.getRunId().equals(runId) && !row.isKeyAspect()
                      && !row.getAspectName().equals(Constants.STATUS_ASPECT_NAME))
              .collect(Collectors.toList());

      long affectedAspects = affectedAspectsList.size();
      long unsafeEntitiesCount = affectedAspectsList.stream()
              .collect(Collectors.groupingBy(AspectRowSummary::getUrn)).keySet().size();

      final List<UnsafeEntityInfo> unsafeEntityInfos = affectedAspectsList.stream().map(AspectRowSummary::getUrn)
              .distinct()
              .map(urn -> {
                UnsafeEntityInfo unsafeEntityInfo = new UnsafeEntityInfo();
                unsafeEntityInfo.setUrn(urn);
                return unsafeEntityInfo;
              })
              // Return at most 1 million rows
              .limit(DEFAULT_UNSAFE_ENTITIES_PAGE_SIZE)
              .collect(Collectors.toList());

      log.info("calculation done.");

      return response.setAspectsAffected(affectedAspects)
              .setAspectsReverted(aspectsReverted)
              .setEntitiesAffected(affectedEntities)
              .setEntitiesDeleted(entitiesDeleted)
              .setUnsafeEntitiesCount(unsafeEntitiesCount)
              .setUnsafeEntities(new UnsafeEntityInfoArray(unsafeEntityInfos))
              .setAspectRowSummaries(rowSummaries);
    }, MetricRegistry.name(this.getClass(), "rollback"));
  }

  private String stringifyRowCount(int size) {
    if (size < ELASTIC_MAX_PAGE_SIZE) {
      return String.valueOf(size);
    } else {
      return "at least " + size;
    }
  }

  private void sleep(Integer seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @Action(name = "list")
  @Nonnull
  @WithSpan
  public Task<IngestionRunSummaryArray> list(@ActionParam("pageOffset") @Optional @Nullable Integer pageOffset,
      @ActionParam("pageSize") @Optional @Nullable Integer pageSize,
      @ActionParam("includeSoft") @Optional @Nullable Boolean includeSoft) {
    log.info("LIST RUNS offset: {} size: {}", pageOffset, pageSize);

    return RestliUtil.toTask(() -> {
              List<IngestionRunSummary> summaries = _systemMetadataService.listRuns(
                      pageOffset != null ? pageOffset : DEFAULT_OFFSET,
                      pageSize != null ? pageSize : DEFAULT_PAGE_SIZE,
                      includeSoft != null ? includeSoft : DEFAULT_INCLUDE_SOFT_DELETED);

              return new IngestionRunSummaryArray(summaries);
            }, MetricRegistry.name(this.getClass(), "list"));
  }
}
