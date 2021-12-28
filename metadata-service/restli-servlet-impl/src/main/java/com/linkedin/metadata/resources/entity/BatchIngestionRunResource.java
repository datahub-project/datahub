package com.linkedin.metadata.resources.entity;

import com.codahale.metrics.MetricRegistry;
import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.restli.RestliUtil;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
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
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.inject.Inject;
import javax.inject.Named;
import lombok.extern.slf4j.Slf4j;


/**
 * resource for showing information and rolling back runs
 */
@Slf4j
@RestLiCollection(name = "runs", namespace = "com.linkedin.entity")
public class BatchIngestionRunResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {

  private static final Integer DEFAULT_OFFSET = 0;
  private static final Integer DEFAULT_PAGE_SIZE = 100;
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
      @ActionParam("dryRun") @Optional @Nullable Boolean dryRun) {
    log.info("ROLLBACK RUN runId: {} dry run: {}", runId, dryRun);
    return RestliUtil.toTask(() -> {
      if (runId.equals(EntityService.DEFAULT_RUN_ID)) {
        throw new IllegalArgumentException(String.format(
            "%s is a default run-id provided for non labeled ingestion runs. You cannot delete using this reserved run-id",
            runId));
      }
      RollbackResponse response = new RollbackResponse();
      List<AspectRowSummary> aspectRowsToDelete;
      aspectRowsToDelete = _systemMetadataService.findByRunId(runId);

      log.info("found {} rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
      if (dryRun) {
        response.setAspectsAffected(aspectRowsToDelete.size());
        response.setEntitiesAffected(
            aspectRowsToDelete.stream().collect(Collectors.groupingBy(AspectRowSummary::getUrn)).keySet().size());
        response.setEntitiesDeleted(aspectRowsToDelete.stream().filter(row -> row.isKeyAspect()).count());
        response.setAspectRowSummaries(
            new AspectRowSummaryArray(aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size()))));
        return response;
      }

      RollbackRunResult rollbackRunResult = _entityService.rollbackRun(aspectRowsToDelete, runId);
      List<AspectRowSummary> deletedRows = rollbackRunResult.getRowsRolledBack();
      Integer rowsDeletedFromEntityDeletion = rollbackRunResult.getRowsDeletedFromEntityDeletion();

      // since elastic limits how many rows we can access at once, we need to iteratively delete
      while (aspectRowsToDelete.size() >= ELASTIC_MAX_PAGE_SIZE) {
        sleep(ELASTIC_BATCH_DELETE_SLEEP_SEC);
        aspectRowsToDelete = _systemMetadataService.findByRunId(runId);
        log.info("{} remaining rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
        log.info("deleting...");
        rollbackRunResult = _entityService.rollbackRun(aspectRowsToDelete, runId);
        deletedRows.addAll(rollbackRunResult.getRowsRolledBack());
        rowsDeletedFromEntityDeletion += rollbackRunResult.getRowsDeletedFromEntityDeletion();
      }

      log.info("finished deleting {} rows", deletedRows.size());
      response.setAspectsAffected(deletedRows.size() + rowsDeletedFromEntityDeletion);
      response.setEntitiesAffected(deletedRows.stream().filter(row -> row.isKeyAspect()).count());
      response.setAspectRowSummaries(
          new AspectRowSummaryArray(deletedRows.subList(0, Math.min(100, deletedRows.size()))));
      return response;
    }, MetricRegistry.name(this.getClass(), "rollback"));
  }

  private String stringifyRowCount(int size) {
    if (size < ELASTIC_MAX_PAGE_SIZE) {
      return String.valueOf(size);
    } else {
      return "at least " + String.valueOf(size);
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
      @ActionParam("pageSize") @Optional @Nullable Integer pageSize) {
    log.info("LIST RUNS offset: {} size: {}", pageOffset, pageSize);

    return RestliUtil.toTask(() -> new IngestionRunSummaryArray(
        _systemMetadataService.listRuns(pageOffset != null ? pageOffset : DEFAULT_OFFSET,
            pageSize != null ? pageSize : DEFAULT_PAGE_SIZE)), MetricRegistry.name(this.getClass(), "list"));
  }
}
