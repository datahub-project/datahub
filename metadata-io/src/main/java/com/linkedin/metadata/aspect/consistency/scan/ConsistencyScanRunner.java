package com.linkedin.metadata.aspect.consistency.scan;

import com.linkedin.metadata.aspect.consistency.ConsistencyService;
import com.linkedin.metadata.aspect.consistency.check.CheckBatchRequest;
import com.linkedin.metadata.aspect.consistency.check.CheckContext;
import com.linkedin.metadata.aspect.consistency.check.CheckResult;
import com.linkedin.metadata.utils.progress.ProgressSnapshot;
import com.linkedin.metadata.utils.progress.ProgressTracker;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;

/**
 * Multi-batch entity-check scan over a single entity type.
 *
 * <p>Owns counting, the {@code checkBatch} loop, progress tracking, and callback dispatch. Does
 * <b>not</b> log INFO itself — adapters decide logging vs silent persistence.
 *
 * <p>Designed for upgrade full-scans. OpenAPI stays page-oriented and should call {@link
 * ConsistencyService#checkBatch} / {@link ConsistencyService#countMatching} directly.
 */
@RequiredArgsConstructor
public class ConsistencyScanRunner {

  @Nonnull private final ConsistencyService consistencyService;

  /**
   * Run a full scan for one entity type until scroll is exhausted, limit is hit, or {@code
   * shouldStop} returns true.
   */
  @Nonnull
  public ConsistencyScanResult run(
      @Nonnull OperationContext opContext, @Nonnull ConsistencyScanRequest request) {

    CheckBatchRequest countRequest =
        CheckBatchRequest.builder()
            .entityType(request.getEntityType())
            .checkIds(request.getCheckIds())
            .filter(request.getFilter())
            .batchSize(request.getBatchSize())
            .build();
    Optional<Long> counted = consistencyService.countMatching(opContext, countRequest);
    Optional<Long> count = counted != null ? counted : Optional.empty();

    Long totalForTracker = resolveTrackerTotal(request, count);

    if (request.getOnStart() != null) {
      request
          .getOnStart()
          .accept(
              ConsistencyScanStart.builder()
                  .entityType(request.getEntityType())
                  .totalEstimate(request.isEntityEtaEligible() ? totalForTracker : null)
                  .etaEnabled(totalForTracker != null)
                  .build());
    }

    ProgressTracker tracker =
        ProgressTracker.builder()
            .label("entity-check[" + request.getEntityType() + "]")
            .total(totalForTracker)
            .initialProcessed(request.getInitialProcessed())
            .reportIntervalMs(request.getProgressLogIntervalMs())
            .warmupMs(request.getProgressWarmupMs())
            .build();

    long scanned = request.getInitialProcessed();
    int issues = request.getInitialIssues();
    int fixed = request.getInitialFixed();
    int failed = request.getInitialFailed();
    String scrollId = request.getScrollId();
    boolean cancelled = false;

    do {
      if (request.getShouldStop() != null && request.getShouldStop().getAsBoolean()) {
        break;
      }

      if (request.getLimit() > 0 && scanned >= request.getLimit()) {
        break;
      }

      int batchSize = request.getBatchSize();
      if (request.getLimit() > 0) {
        long remaining = request.getLimit() - scanned;
        batchSize = (int) Math.min(batchSize, remaining);
      }

      CheckContext batchContext = request.getCheckContext();
      if (batchContext != null) {
        batchContext.clearOrphanUrns(request.getEntityType());
      }

      CheckResult checkResult =
          consistencyService.checkBatch(
              opContext,
              CheckBatchRequest.builder()
                  .entityType(request.getEntityType())
                  .checkIds(request.getCheckIds())
                  .batchSize(batchSize)
                  .scrollId(scrollId)
                  .filter(request.getFilter())
                  .build(),
              batchContext);

      if (checkResult.getEntitiesScanned() == 0) {
        break;
      }

      scanned += checkResult.getEntitiesScanned();
      issues += checkResult.getIssuesFound();
      tracker.record(checkResult.getEntitiesScanned());

      if (request.getOnBatch() != null) {
        BatchHandleResult handleResult = request.getOnBatch().handle(checkResult);
        fixed += handleResult.getFixed();
        failed += handleResult.getFailed();
      }

      scrollId = checkResult.getScrollId();

      ProgressSnapshot snap = tracker.snapshot();
      if (request.getOnCheckpoint() != null) {
        request
            .getOnCheckpoint()
            .accept(
                ConsistencyScanCheckpoint.builder()
                    .entityType(request.getEntityType())
                    .scrollId(scrollId)
                    .entitiesScanned(scanned)
                    .issuesFound(issues)
                    .issuesFixed(fixed)
                    .issuesFailed(failed)
                    .progress(snap)
                    .build());
      }

      if (request.getOnProgress() != null) {
        tracker.maybeReport(request.getOnProgress());
      }

      if (request.getLimit() > 0 && scanned >= request.getLimit()) {
        break;
      }

      if (request.getDelayMs() > 0 && scrollId != null) {
        if (!applyDelay(request)) {
          cancelled = true;
          break;
        }
      }

    } while (scrollId != null);

    ProgressSnapshot finalSnap = tracker.snapshot();
    ConsistencyScanResult result =
        ConsistencyScanResult.builder()
            .entityType(request.getEntityType())
            .entitiesScanned(scanned)
            .issuesFound(issues)
            .issuesFixed(fixed)
            .issuesFailed(failed)
            .totalEstimate(totalForTracker)
            .finalProgress(finalSnap)
            .cancelled(cancelled)
            .build();

    if (!cancelled && request.getOnComplete() != null) {
      request.getOnComplete().accept(result);
    }

    return result;
  }

  @javax.annotation.Nullable
  private static Long resolveTrackerTotal(
      @Nonnull ConsistencyScanRequest request, @Nonnull Optional<Long> count) {
    if (!request.isEntityEtaEligible() || count.isEmpty()) {
      return null;
    }
    long counted = count.get();
    if (request.getLimit() > 0) {
      return Math.min(counted, (long) request.getLimit());
    }
    return counted;
  }

  /**
   * @return false when interrupted — caller should stop the scan loop
   */
  private boolean applyDelay(@Nonnull ConsistencyScanRequest request) {
    if (request.getDelayHook() != null) {
      request.getDelayHook().run();
      return true;
    }
    try {
      Thread.sleep(request.getDelayMs());
      return true;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }
}
