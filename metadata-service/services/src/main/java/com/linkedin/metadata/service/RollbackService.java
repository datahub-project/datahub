package com.linkedin.metadata.service;

import static com.linkedin.metadata.Constants.DEFAULT_RUN_ID;
import static com.linkedin.metadata.authorization.ApiOperation.DELETE;
import static com.linkedin.metadata.utils.metrics.LongRunningOperationMetrics.TAG_OPERATION;
import static com.linkedin.metadata.utils.metrics.LongRunningOperationMetrics.TAG_PHASE;

import com.datahub.authentication.AuthenticationException;
import com.datahub.authorization.AuthUtil;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.linkedin.common.AuditStamp;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.entity.EnvelopedAspect;
import com.linkedin.events.metadata.ChangeType;
import com.linkedin.execution.ExecutionRequestResult;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.RollbackRunResult;
import com.linkedin.metadata.key.ExecutionRequestKey;
import com.linkedin.metadata.run.AspectRowSummary;
import com.linkedin.metadata.run.AspectRowSummaryArray;
import com.linkedin.metadata.run.RollbackResponse;
import com.linkedin.metadata.run.UnsafeEntityInfo;
import com.linkedin.metadata.run.UnsafeEntityInfoArray;
import com.linkedin.metadata.systemmetadata.SystemMetadataService;
import com.linkedin.metadata.timeseries.TimeseriesAspectService;
import com.linkedin.metadata.utils.EntityKeyUtils;
import com.linkedin.metadata.utils.GenericRecordUtils;
import com.linkedin.metadata.utils.metrics.LongRunningOperationMetrics;
import com.linkedin.mxe.MetadataChangeProposal;
import com.linkedin.timeseries.DeleteAspectValuesResult;
import io.datahubproject.metadata.context.OperationContext;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/** Extracts logic historically in the Restli service which acts across multiple services */
@Slf4j
@AllArgsConstructor
public class RollbackService {
  public static final String ROLLING_BACK_STATUS = "ROLLING_BACK";
  public static final String ROLLED_BACK_STATUS = "ROLLED_BACK";
  public static final String ROLLBACK_FAILED_STATUS = "ROLLBACK_FAILED";

  public static final int DEFAULT_UNSAFE_ENTITIES_PAGE_SIZE = 1000000;
  public static final int ELASTIC_BATCH_DELETE_SLEEP_SEC = 5;

  static final String METRIC_PREFIX = "datahub.ingestion.rollback";
  static final String OPERATION_TYPE = "ingestionRollback";
  static final String PHASE_DRY_RUN = "dry_run";
  static final String PHASE_EXECUTE = "execute";

  private final EntityService<?> entityService;
  private final SystemMetadataService systemMetadataService;
  private final TimeseriesAspectService timeseriesAspectService;
  private final SystemMetadataServiceConfig systemMetadataServiceConfig;

  public List<AspectRowSummary> rollbackTargetAspects(
      @Nonnull OperationContext opContext, @Nonnull String runId, boolean hardDelete) {
    return systemMetadataService.findByRunId(
        opContext,
        runId,
        hardDelete,
        0,
        systemMetadataServiceConfig.getLimit().getResults().getApiDefault());
  }

  public RollbackResponse rollbackIngestion(
      @Nonnull OperationContext opContext,
      @Nonnull String runId,
      boolean dryRun,
      boolean hardDelete,
      Authorizer authorizer)
      throws AuthenticationException {

    if (runId.equals(DEFAULT_RUN_ID)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is a default run-id provided for non labeled ingestion runs. You cannot delete using this reserved run-id",
              runId));
    }

    final LongRunningOperationMetrics metrics =
        LongRunningOperationMetrics.begin(
            opContext.getMetricUtils().orElse(null),
            METRIC_PREFIX,
            Tags.of(
                TAG_OPERATION, OPERATION_TYPE, TAG_PHASE, dryRun ? PHASE_DRY_RUN : PHASE_EXECUTE));

    try {
      if (!dryRun) {
        updateExecutionRequestStatus(opContext, runId, ROLLING_BACK_STATUS);
      }

      List<AspectRowSummary> aspectRowsToDelete =
          rollbackTargetAspects(opContext, runId, hardDelete);
      if (!isAuthorized(opContext, aspectRowsToDelete)) {
        throw new AuthenticationException("User is NOT unauthorized to delete entities.");
      }

      log.info("found {} rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
      if (dryRun) {

        final Map<Boolean, List<AspectRowSummary>> aspectsSplitByIsKeyAspects =
            aspectRowsToDelete.stream()
                .collect(Collectors.partitioningBy(AspectRowSummary::isKeyAspect));

        final List<AspectRowSummary> keyAspects = aspectsSplitByIsKeyAspects.get(true);

        long entitiesDeleted = keyAspects.size();
        long aspectsReverted = aspectRowsToDelete.size();

        final long affectedEntities =
            aspectRowsToDelete.stream()
                .collect(Collectors.groupingBy(AspectRowSummary::getUrn))
                .keySet()
                .size();

        final AspectRowSummaryArray rowSummaries =
            new AspectRowSummaryArray(
                aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size())));

        // If we are soft deleting, remove key aspects from count of aspects being deleted
        if (!hardDelete) {
          aspectsReverted -= keyAspects.size();
          rowSummaries.removeIf(AspectRowSummary::isKeyAspect);
        }
        // Compute the aspects that exist referencing the key aspects we are deleting
        final List<AspectRowSummary> affectedAspectsList =
            keyAspects.stream()
                .map(
                    (AspectRowSummary urn) ->
                        systemMetadataService.findByUrn(
                            opContext,
                            urn.getUrn(),
                            false,
                            0,
                            systemMetadataServiceConfig.getLimit().getResults().getApiDefault()))
                .flatMap(List::stream)
                .filter(
                    row ->
                        !row.getRunId().equals(runId)
                            && !row.isKeyAspect()
                            && !row.getAspectName().equals(Constants.STATUS_ASPECT_NAME))
                .collect(Collectors.toList());

        long unsafeEntitiesCount =
            affectedAspectsList.stream()
                .collect(Collectors.groupingBy(AspectRowSummary::getUrn))
                .keySet()
                .size();

        final List<UnsafeEntityInfo> unsafeEntityInfos =
            affectedAspectsList.stream()
                .map(AspectRowSummary::getUrn)
                .distinct()
                .map(
                    urn -> {
                      UnsafeEntityInfo unsafeEntityInfo = new UnsafeEntityInfo();
                      unsafeEntityInfo.setUrn(urn);
                      return unsafeEntityInfo;
                    })
                // Return at most 1 million rows
                .limit(DEFAULT_UNSAFE_ENTITIES_PAGE_SIZE)
                .collect(Collectors.toList());

        metrics.recordRows(aspectRowsToDelete.size());
        metrics.recordPage();

        return new RollbackResponse()
            .setAspectsReverted(aspectsReverted)
            .setEntitiesAffected(affectedEntities)
            .setEntitiesDeleted(entitiesDeleted)
            .setUnsafeEntitiesCount(unsafeEntitiesCount)
            .setUnsafeEntities(new UnsafeEntityInfoArray(unsafeEntityInfos))
            .setAspectRowSummaries(rowSummaries);
      }

      RollbackRunResult rollbackRunResult =
          entityService.rollbackRun(opContext, aspectRowsToDelete, runId, hardDelete);
      final List<AspectRowSummary> deletedRows = rollbackRunResult.getRowsRolledBack();
      int rowsDeletedFromEntityDeletion = rollbackRunResult.getRowsDeletedFromEntityDeletion();
      recordRollbackPage(metrics, rollbackRunResult);

      // Accumulate key aspects across all pages. Master only kept the last page, which undercounted
      // entitiesDeleted and made affected/unsafe entity math depend on the final page only.
      final List<AspectRowSummary> allKeyAspects = new ArrayList<>();
      aspectRowsToDelete.stream().filter(AspectRowSummary::isKeyAspect).forEach(allKeyAspects::add);

      // since elastic limits how many rows we can access at once, we need to iteratively
      // delete
      while (aspectRowsToDelete.size()
          >= systemMetadataServiceConfig.getLimit().getResults().getApiDefault()) {
        sleep(ELASTIC_BATCH_DELETE_SLEEP_SEC);
        aspectRowsToDelete =
            systemMetadataService.findByRunId(
                opContext,
                runId,
                hardDelete,
                0,
                systemMetadataServiceConfig.getLimit().getResults().getApiDefault());
        log.info("{} remaining rows to delete...", stringifyRowCount(aspectRowsToDelete.size()));
        log.info("deleting...");
        rollbackRunResult =
            entityService.rollbackRun(opContext, aspectRowsToDelete, runId, hardDelete);
        deletedRows.addAll(rollbackRunResult.getRowsRolledBack());
        rowsDeletedFromEntityDeletion += rollbackRunResult.getRowsDeletedFromEntityDeletion();
        aspectRowsToDelete.stream()
            .filter(AspectRowSummary::isKeyAspect)
            .forEach(allKeyAspects::add);
        recordRollbackPage(metrics, rollbackRunResult);
      }

      // Rollback timeseries aspects
      DeleteAspectValuesResult timeseriesRollbackResult =
          timeseriesAspectService.rollbackTimeseriesAspects(opContext, runId);
      int timeseriesDeleted = timeseriesRollbackResult.getNumDocsDeleted().intValue();
      rowsDeletedFromEntityDeletion += timeseriesDeleted;
      if (timeseriesDeleted > 0) {
        metrics.recordRows(timeseriesDeleted);
        metrics.recordPage();
      }

      log.info("finished deleting {} rows", deletedRows.size());
      // Execute path: aspectsReverted counts what rollbackRun actually reverted. Master did not
      // subtract key aspects here (soft delete already excludes them from rowsRolledBack), and
      // subtracting again would double-count to 0. Dry-run keeps the subtract below.
      int aspectsReverted = deletedRows.size() + rowsDeletedFromEntityDeletion;

      final long entitiesDeleted = allKeyAspects.size();
      final long affectedEntities =
          deletedRows.stream()
              .collect(Collectors.groupingBy(AspectRowSummary::getUrn))
              .keySet()
              .size();

      final AspectRowSummaryArray rowSummaries =
          new AspectRowSummaryArray(
              aspectRowsToDelete.subList(0, Math.min(100, aspectRowsToDelete.size())));

      log.info("computing aspects affected by this rollback...");
      // Compute the aspects that exist referencing the key aspects we are deleting, across all
      // pages visited.
      final List<AspectRowSummary> affectedAspectsList =
          allKeyAspects.stream()
              .map(
                  (AspectRowSummary urn) ->
                      systemMetadataService.findByUrn(
                          opContext,
                          urn.getUrn(),
                          false,
                          0,
                          systemMetadataServiceConfig.getLimit().getResults().getApiDefault()))
              .flatMap(List::stream)
              .filter(
                  row ->
                      !row.getRunId().equals(runId)
                          && !row.isKeyAspect()
                          && !row.getAspectName().equals(Constants.STATUS_ASPECT_NAME))
              .collect(Collectors.toList());

      long affectedAspects = affectedAspectsList.size();
      long unsafeEntitiesCount =
          affectedAspectsList.stream()
              .collect(Collectors.groupingBy(AspectRowSummary::getUrn))
              .keySet()
              .size();

      final List<UnsafeEntityInfo> unsafeEntityInfos =
          affectedAspectsList.stream()
              .map(AspectRowSummary::getUrn)
              .distinct()
              .map(
                  urn -> {
                    UnsafeEntityInfo unsafeEntityInfo = new UnsafeEntityInfo();
                    unsafeEntityInfo.setUrn(urn);
                    return unsafeEntityInfo;
                  })
              // Return at most 1 million rows
              .limit(DEFAULT_UNSAFE_ENTITIES_PAGE_SIZE)
              .collect(Collectors.toList());

      log.info("calculation done.");

      updateExecutionRequestStatus(opContext, runId, ROLLED_BACK_STATUS);

      return new RollbackResponse()
          .setAspectsAffected(affectedAspects)
          .setAspectsReverted(aspectsReverted)
          .setEntitiesAffected(affectedEntities)
          .setEntitiesDeleted(entitiesDeleted)
          .setUnsafeEntitiesCount(unsafeEntitiesCount)
          .setUnsafeEntities(new UnsafeEntityInfoArray(unsafeEntityInfos))
          .setAspectRowSummaries(rowSummaries);
    } catch (AuthenticationException e) {
      metrics.failed("unauthorized");
      throw e;
    } catch (Throwable e) {
      metrics.failed("unexpected");
      throw e;
    } finally {
      metrics.finish();
    }
  }

  private static void recordRollbackPage(
      final LongRunningOperationMetrics metrics, final RollbackRunResult result) {
    int processed = result.getRowsRolledBack().size() + result.getRowsDeletedFromEntityDeletion();
    // Always count the page — it completed regardless of whether rows were deleted. Volume is
    // conditional so a zero-row page does not inflate entities_processed / rows_processed.
    metrics.recordPage();
    if (processed > 0) {
      metrics.recordRows(processed);
    }
  }

  public void updateExecutionRequestStatus(
      @Nonnull OperationContext opContext, @Nonnull String runId, @Nonnull String status) {
    try {
      final Urn executionRequestUrn =
          EntityKeyUtils.convertEntityKeyToUrn(
              new ExecutionRequestKey().setId(runId), Constants.EXECUTION_REQUEST_ENTITY_NAME);
      EnvelopedAspect aspect =
          entityService.getLatestEnvelopedAspect(
              opContext,
              executionRequestUrn.getEntityType(),
              executionRequestUrn,
              Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
      if (aspect == null) {
        log.warn("Aspect for execution request with runId {} not found", runId);
      } else {
        final MetadataChangeProposal proposal = new MetadataChangeProposal();
        ExecutionRequestResult requestResult = new ExecutionRequestResult(aspect.getValue().data());
        requestResult.setStatus(status);
        proposal.setEntityUrn(executionRequestUrn);
        proposal.setEntityType(Constants.EXECUTION_REQUEST_ENTITY_NAME);
        proposal.setAspectName(Constants.EXECUTION_REQUEST_RESULT_ASPECT_NAME);
        proposal.setAspect(GenericRecordUtils.serializeAspect(requestResult));
        proposal.setChangeType(ChangeType.UPSERT);

        entityService.ingestProposal(
            opContext,
            proposal,
            new AuditStamp()
                .setActor(UrnUtils.getUrn(Constants.SYSTEM_ACTOR))
                .setTime(System.currentTimeMillis()),
            false);
      }
    } catch (Exception e) {
      log.error(
          String.format(
              "Not able to update execution result aspect with runId %s and new status %s.",
              runId, status),
          e);
    }
  }

  private boolean isAuthorized(
      @Nonnull OperationContext opContext, @Nonnull List<AspectRowSummary> rowSummaries) {

    return AuthUtil.isAPIAuthorizedEntityUrns(
        opContext,
        DELETE,
        rowSummaries.stream()
            .map(AspectRowSummary::getUrn)
            .map(UrnUtils::getUrn)
            .collect(Collectors.toSet()));
  }

  private String stringifyRowCount(int size) {
    if (size < systemMetadataServiceConfig.getLimit().getResults().getMax()) {
      return String.valueOf(size);
    } else {
      return "at least " + size;
    }
  }

  private static void sleep(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      log.error("Rollback sleep exception", e);
    }
  }
}
