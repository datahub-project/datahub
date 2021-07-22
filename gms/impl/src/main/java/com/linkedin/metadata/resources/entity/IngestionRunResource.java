package com.linkedin.metadata.resources.entity;

import com.linkedin.metadata.aspect.VersionedAspect;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.restli.RestliUtils;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.IngestionRunSummaryArray;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.systemMetadata.SystemMetadataService;
import com.linkedin.parseq.Task;
import com.linkedin.restli.server.annotations.Action;
import com.linkedin.restli.server.annotations.ActionParam;
import com.linkedin.restli.server.annotations.Optional;
import com.linkedin.restli.server.annotations.RestLiCollection;
import com.linkedin.restli.server.resources.CollectionResourceTaskTemplate;
import java.util.List;
import java.util.concurrent.TimeUnit;
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
public class IngestionRunResource extends CollectionResourceTaskTemplate<String, VersionedAspect> {

  private static final Integer DEFAULT_OFFSET = 0;
  private static final Integer DEFAULT_PAGE_SIZE = 100;
  private static final Integer ELASTIC_MAX_PAGE_SIZE = 10000;

  @Inject
  @Named("systemMetadataService")
  private SystemMetadataService _systemMetadataService;

  @Inject
  @Named("entityService")
  private EntityService _entityService;

  /**
   * Retrieves the value for an entity that is made up of latest versions of specified aspects.
   */
  @Action(name = "rollback")
  @Nonnull
  public Task<RollbackResponse> rollback(
      @ActionParam("runId") @Nonnull String runId,
      @ActionParam("dryRun") @Optional @Nullable Boolean dryRun
  ) {
    log.info("ROLLBACK RUN runId: {} dry run: {}", runId, dryRun);
    return RestliUtils.toTask(() -> {
      RollbackResponse response = new RollbackResponse();
      List<AspectRowSummary> aspectRowsToDelete;
      aspectRowsToDelete = _systemMetadataService.findByRunId(runId);

      log.info("found {} rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
      if (dryRun) {
        response.setTotal(aspectRowsToDelete.size());
        response.setAspectRowSummaries(
            new AspectRowSummaryArray(aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size())))
        );
        return response;
      }

      log.info("deleting...");
      List<AspectRowSummary> deletedRows = _entityService.rollbackRun(aspectRowsToDelete, runId);
      while (aspectRowsToDelete.size() >= ELASTIC_MAX_PAGE_SIZE) {
        sleep(5);
        aspectRowsToDelete = _systemMetadataService.findByRunId(runId);
        log.info("{} remaining rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
        log.info("deleting...");
        deletedRows.addAll(_entityService.rollbackRun(aspectRowsToDelete, runId));
      }

      log.info("finished deleting {} rows", deletedRows.size());
      response.setTotal(deletedRows.size());
      response.setAspectRowSummaries(
          new AspectRowSummaryArray(deletedRows.subList(0, Math.min(100, deletedRows.size())))
      );
      return response;
    });
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
  public Task<IngestionRunSummaryArray> list(
      @ActionParam("pageOffset") @Optional @Nullable Integer pageOffset,
      @ActionParam("pageSize") @Optional @Nullable Integer pageSize
  ) {
    log.info("LIST RUNS min size: {} max size: {} offset: {} size: {}");

    return RestliUtils.toTask(() ->
      new IngestionRunSummaryArray(_systemMetadataService.listRuns(
          pageOffset != null ? pageOffset : DEFAULT_OFFSET,
          pageSize != null ? pageSize : DEFAULT_PAGE_SIZE
      ))
    );
  }
}
