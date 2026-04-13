package com.linkedin.metadata.search.elasticsearch.indexbuilder;

import com.linkedin.metadata.config.search.BuildIndicesConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.exceptions.*;
import com.linkedin.metadata.search.utils.RetryConfigUtils;
import io.github.resilience4j.retry.Retry;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.common.settings.Settings;

/**
 * Orchestrates parallel reindex operations with simple queue-based concurrency control. Maintains a
 * maximum number of concurrent ES reindex operations to avoid cluster overload.
 *
 * <p>Passive design: Receives state change notifications from CircuitBreakerState instead of
 * polling cluster health directly. When state changes occur, rethrottles all active tasks to the
 * new tier's RPS limit.
 */
@Slf4j
public class ParallelReindexOrchestrator {

  /**
   * JVM-local index-level locks prevent concurrent reindex operations on the same index WITHIN THIS
   * PROCESS. These are NOT distributed locks and NOT safe for multi-pod deployments. Designed for
   * single-pod upgrade jobs only. Instance-based to avoid hidden static dependencies.
   */
  private final ConcurrentHashMap<String, Object> indexLocks;

  private final ESIndexBuilder indexBuilder;
  private final BuildIndicesConfiguration config;
  private final CircuitBreakerState circuitBreakerState;

  /**
   * Channel for state change notifications. Non-blocking queue allowing HealthCheckPoller thread to
   * signal state changes. Main orchestration thread drains this queue at each iteration to detect
   * and respond to throttle tier changes.
   */

  /**
   * Track indices that failed cleanup. Used to provide actionable error messages with exact DELETE
   * commands needed.
   */
  private final Set<String> failedCleanupIndices = Collections.synchronizedSet(new HashSet<>());

  /**
   * Thread pool for async finalization to prevent blocking monitoring loop. Allows multiple indices
   * to finalize concurrently without blocking task monitoring. FixedThreadPool already controls
   * concurrency via thread count, so no separate semaphore needed.
   */
  private final ExecutorService finalizationExecutor;

  /**
   * Shared thread pool for rethrottling operations. Reused across all health state changes to avoid
   * the overhead of creating and destroying ExecutorService instances on every state transition.
   * Pool size configured via rethrottleExecutorPoolSize to prevent overwhelming the cluster with
   * simultaneous task updates.
   */
  private final ExecutorService rethrottleExecutor;

  /**
   * Retry instance for document count validation during finalization. Uses exponential backoff to
   * allow ES time to flush segments and complete index refresh operations. Configured via
   * BuildIndicesConfiguration (docCountValidationRetryCount, docCountValidationRetrySleepMs).
   */
  private Retry docCountValidationRetry;

  /**
   * Constructor using BuildIndicesConfiguration.
   *
   * <p>All configuration (cost thresholds, concurrency limits, health check settings) comes from
   * {@link BuildIndicesConfiguration} which can be customized via environment variables or YAML.
   *
   * <p>Registers this instance as a listener on CircuitBreakerState to receive state change
   * notifications from the HealthCheckPoller thread.
   *
   * @param indexBuilder ESIndexBuilder for index operations
   * @param config BuildIndicesConfiguration with all settings
   * @param circuitBreakerState CircuitBreakerState for receiving health state changes
   */
  public ParallelReindexOrchestrator(
      ESIndexBuilder indexBuilder,
      BuildIndicesConfiguration config,
      CircuitBreakerState circuitBreakerState) {
    this.indexBuilder = indexBuilder;
    this.config = config;
    this.circuitBreakerState = circuitBreakerState;
    this.indexLocks = new ConcurrentHashMap<>();
    this.finalizationExecutor =
        Executors.newFixedThreadPool(config.getMaxConcurrentFinalizations());
    this.rethrottleExecutor = Executors.newFixedThreadPool(config.getRethrottleExecutorPoolSize());
    this.docCountValidationRetry =
        Retry.of(
            "doc-count-validation",
            RetryConfigUtils.constructDocValidationRetry(
                config.getDocCountValidationRetrySleepMs(),
                config.getDocCountValidationRetryCount()));
  }

  /**
   * Reindex multiple indices with controlled concurrency using a simple queue-based approach.
   *
   * @param configs List of reindex configurations
   * @return Map of index name to reindex result
   * @throws InterruptedException if interrupted during monitoring
   */
  public Map<String, ReindexResult> reindexAll(@Nonnull List<ReindexConfig> configs)
      throws InterruptedException {

    // Filter to indices that need reindexing
    List<ReindexConfig> toReindex =
        configs.stream().filter(ReindexConfig::requiresReindex).collect(Collectors.toList());

    if (toReindex.isEmpty()) {
      log.info("No indices require reindexing");
      return Collections.emptyMap();
    }

    log.info("Starting parallel reindex for {} indices", toReindex.size());

    failedCleanupIndices.clear();

    Map<IndexCostEstimator.CostTier, List<ReindexConfig>> tieredConfigs =
        classifyIndicesByCost(toReindex);

    List<ReindexConfig> largeIndices =
        tieredConfigs.getOrDefault(IndexCostEstimator.CostTier.LARGE, Collections.emptyList());
    List<ReindexConfig> normalIndices =
        tieredConfigs.getOrDefault(IndexCostEstimator.CostTier.NORMAL, Collections.emptyList());

    log.info(
        "Reindex distribution: {} LARGE indices (serialize, max={} concurrent), {} NORMAL indices (batch, max={} concurrent)",
        largeIndices.size(),
        config.getMaxConcurrentLargeReindex(),
        normalIndices.size(),
        config.getMaxConcurrentNormalReindex());

    // Acquire index-level locks to prevent concurrent reindex on same index
    List<String> lockedIndices = new ArrayList<>();
    try {
      for (ReindexConfig config : toReindex) {
        if (indexLocks.putIfAbsent(config.name(), new Object()) != null) {
          throw new IllegalStateException(
              "Reindex already in progress for index: " + config.name());
        }
        lockedIndices.add(config.name());
      }

      // Execute with cost-aware concurrency limits
      return executeReindexWithCostAwareness(largeIndices, normalIndices);

    } finally {
      // Always release index locks
      int releasedCount = 0;
      for (String indexName : lockedIndices) {
        if (indexLocks.remove(indexName) != null) {
          releasedCount++;
        }
      }
      if (releasedCount > 0) {
        log.info("Released reindex locks for {} indices", releasedCount);
      }
      if (!failedCleanupIndices.isEmpty()) {
        String errorMsg = buildCleanupErrorMessage(failedCleanupIndices);
        log.error(errorMsg);
        // Log cleanup failure but don't fail the overall operation
        // Cleanup failures are non-blocking - reindex completed, just orphaned some temp indices
      }
    }
  }

  /**
   * Check if the overall reindex operation has exceeded the maximum allowed time. Marks both active
   * and pending tasks as failed to prevent silent data loss when timeout occurs.
   *
   * @param startTime Time when reindex operation started (milliseconds)
   * @param timeoutMs Maximum allowed time (milliseconds)
   * @param activeTasks Currently active tasks to clean up on timeout
   * @param pendingConfigs Pending configs waiting to be submitted
   * @param results Map to store timeout results
   * @return true if timeout occurred, false otherwise
   */
  private boolean checkForTimeout(
      long startTime,
      long timeoutMs,
      Map<String, ReindexTaskInfo> activeTasks,
      Queue<ReindexConfig> pendingConfigs,
      Map<String, ReindexResult> results) {

    if (System.currentTimeMillis() - startTime > timeoutMs) {
      log.error(
          "Reindex timeout! {} tasks still active, {} pending after {} hours",
          activeTasks.size(),
          pendingConfigs.size(),
          config.getMaxReindexHours());
      // Mark active tasks as timeout and cleanup
      List<String> cleanupFailedIndices = new ArrayList<>();
      for (ReindexTaskInfo taskInfo : activeTasks.values()) {
        results.put(taskInfo.getIndexAlias(), ReindexResult.FAILED_TIMEOUT);
        try {
          log.error(
              "Reindex timeout for {}: {} active tasks, {} pending after {} hours",
              taskInfo.getIndexAlias(),
              activeTasks.size(),
              pendingConfigs.size(),
              config.getMaxReindexHours());
          cleanupIndex(
              taskInfo.indexAlias,
              taskInfo.tempIndexName,
              "timeout exceeded",
              "timeout of reindexing task exceeded");
        } catch (Exception e) {
          log.error(
              "Cleanup failed for {} during timeout: {}", taskInfo.getIndexAlias(), e.getMessage());
          cleanupFailedIndices.add(taskInfo.getIndexAlias());
        }
      }
      // Mark pending configs as timeout too to prevent silent loss
      for (ReindexConfig config : pendingConfigs) {
        results.put(config.name(), ReindexResult.FAILED_TIMEOUT);
        log.warn(
            "Pending index {} never submitted due to timeout - marking as failed", config.name());
      }
      if (!cleanupFailedIndices.isEmpty()) {
        log.error(
            "CRITICAL: {} indices failed cleanup during timeout: {}",
            cleanupFailedIndices.size(),
            cleanupFailedIndices);
      }
      return true;
    }
    return false;
  }

  /**
   * Submit new reindex tasks to fill available concurrency slots.
   *
   * @param pendingConfigs Queue of configs waiting to be submitted
   * @param activeTasks Map of currently active tasks (task ID -> task info)
   * @param results Map to store results for completed/failed submissions
   * @param failedCount Counter for failed submissions
   * @param maxConcurrency Maximum concurrent tasks for this tier
   * @param currentHealthState
   */
  private void submitPendingTasks(
      Queue<ReindexConfig> pendingConfigs,
      Map<String, ReindexTaskInfo> activeTasks,
      Map<String, ReindexResult> results,
      AtomicInteger failedCount,
      int maxConcurrency,
      CircuitBreakerState.HealthState currentHealthState) {

    while (activeTasks.size() < maxConcurrency && !pendingConfigs.isEmpty()) {
      ReindexConfig config = pendingConfigs.poll();
      try {
        ReindexTaskInfo taskInfo = submitReindexTask(config, currentHealthState);
        if (taskInfo == null) {
          // Empty index - skip
          log.info("Index {} has 0 documents, skipping reindex", config.name());
          results.put(config.name(), ReindexResult.REINDEXED_SKIPPED_0DOCS);
        } else {
          activeTasks.put(taskInfo.getTaskId(), taskInfo);
          log.info(
              "Submitted reindex for {} (task: {}) - {}/{} active",
              config.name(),
              taskInfo.getTaskId(),
              activeTasks.size(),
              maxConcurrency);
        }
      } catch (Exception e) {
        log.error("Failed to submit reindex for {}: {}", config.name(), e.getMessage(), e);
        results.put(config.name(), classifyException(e));
        failedCount.incrementAndGet();
      }
    }
  }

  /**
   * Monitor active reindex tasks and finalize completed ones.
   *
   * @param activeTasks Map of currently active tasks (task ID -> task info)
   * @param finalizingTasks Set of task IDs currently being finalized asynchronously
   * @param results Map to store results for completed tasks
   * @param completedCount Counter for successfully completed tasks
   * @param failedCount Counter for failed tasks
   */
  private void monitorAndCompleteActiveTasks(
      Map<String, ReindexTaskInfo> activeTasks,
      Set<String> finalizingTasks,
      Map<String, ReindexResult> results,
      AtomicInteger completedCount,
      AtomicInteger failedCount) {

    if (activeTasks.isEmpty()) {
      return;
    }

    // Batch fetch all task statuses
    ESIndexBuilder.TaskStatusResult statusResult =
        indexBuilder.getTaskStatusMultiple(activeTasks.keySet());
    Map<String, GetTaskResponse> taskResponses = statusResult.getResponses();
    Set<String> failedTaskIds = statusResult.getFailedTaskIds();

    Iterator<Map.Entry<String, ReindexTaskInfo>> iterator = activeTasks.entrySet().iterator();
    while (iterator.hasNext()) {
      Map.Entry<String, ReindexTaskInfo> entry = iterator.next();
      String taskId = entry.getKey();
      ReindexTaskInfo taskInfo = entry.getValue();

      try {
        // If we had network errors, the task may still be running - do NOT count as null
        if (failedTaskIds.contains(taskId)) {
          taskInfo.consecutiveNullResponses = 0; // Reset counter - don't count network errors
          log.debug(
              "Task {} for {} had transient fetch error - resetting null counter (may still be running)",
              taskId,
              taskInfo.getIndexAlias());
          continue;
        }

        GetTaskResponse taskResponse = taskResponses.get(taskId);

        // If task response is missing (not in failed set, not in responses), it's legitimately not
        // found
        // This means task completed and ES removed it from active task list
        if (taskResponse == null) {
          taskInfo.consecutiveNullResponses++;
          // After 5 consecutive legitimately-missing responses, task is truly completed
          if (taskInfo.consecutiveNullResponses >= 5) {
            log.info(
                "Task {} for {} legitimately not found after {} consecutive checks (task completed and removed from ES)",
                taskId,
                taskInfo.getIndexAlias(),
                taskInfo.consecutiveNullResponses);
            iterator.remove();
            finalizingTasks.add(taskId);
            submitAsyncFinalization(taskInfo, results, completedCount, failedCount)
                .whenComplete((result, ex) -> finalizingTasks.remove(taskId));
          }
          continue;
        }

        // Reset null counter when we get a valid response
        taskInfo.consecutiveNullResponses = 0;

        // Check if task is completed
        if (taskResponse.isCompleted()) {
          log.info("Task {} for {} is completed", taskId, taskInfo.getIndexAlias());
          iterator.remove();
          finalizingTasks.add(taskId);
          submitAsyncFinalization(taskInfo, results, completedCount, failedCount)
              .whenComplete((result, ex) -> finalizingTasks.remove(taskId));
        }
      } catch (Exception e) {
        log.error(
            "Error processing task {} for {}: {}",
            taskId,
            taskInfo.getIndexAlias(),
            e.getMessage(),
            e);
        results.put(taskInfo.getIndexAlias(), ReindexResult.FAILED_MONITORING_ERROR);
        failedCount.incrementAndGet();
        iterator.remove();
        try {
          cleanupIndex(
              taskInfo.getIndexAlias(),
              taskInfo.getTempIndexName(),
              "Monitoring error",
              "manual cleanup may be required");
        } catch (Exception cleanupError) {
          log.error(
              "Cleanup failed for {} during monitoring error: {}",
              taskInfo.getIndexAlias(),
              cleanupError.getMessage());
          failedCleanupIndices.add(taskInfo.getIndexAlias());
        }
      }
    }
  }

  private ReindexResult classifyException(Exception e) {
    if (e instanceof BaseReindexException) {
      return ((BaseReindexException) e).getFailureResult();
    }

    // Standard exception types
    if (e instanceof TimeoutException) {
      return ReindexResult.FAILED_TIMEOUT;
    }
    if (e instanceof ExecutionException) {
      Throwable cause = e.getCause();
      if (cause instanceof IOException) {
        return ReindexResult.FAILED_SUBMISSION_IO;
      }
      return ReindexResult.FAILED_SUBMISSION;
    }
    if (e instanceof IOException) {
      return ReindexResult.FAILED_SUBMISSION_IO;
    }

    // Walk cause chain for legacy patterns and wrapped exceptions
    Throwable current = e;
    while (current != null) {
      if (current instanceof IOException) {
        return ReindexResult.FAILED_SUBMISSION_IO;
      }
      if (current.getMessage() != null && current.getMessage().contains("Doc count mismatch")) {
        return ReindexResult.FAILED_DOC_COUNT_MISMATCH;
      }
      current = current.getCause();
    }

    return ReindexResult.FAILED_SUBMISSION;
  }

  /** Submit a reindex task for a single index with tier-specific scroll size */
  private ReindexTaskInfo submitReindexTask(
      ReindexConfig indexConfig, CircuitBreakerState.HealthState currentHealthState)
      throws Exception {
    // Log estimated doc count (without refresh for cost estimation)
    long estimatedSourceCount = indexBuilder.getCountWithoutRefresh(indexConfig.name());

    log.info(
        "Would reindex approximately {} documents from {}",
        estimatedSourceCount,
        indexConfig.name());

    // Skip reindex if index is empty
    if (estimatedSourceCount == 0) {
      return null;
    }

    long startTime = System.currentTimeMillis();
    String tempIndexName = ESIndexBuilder.getNextIndexName(indexConfig.name(), startTime);

    try {
      indexBuilder.createIndex(tempIndexName, indexConfig);

      // Optimize destination index for reindex (reduce ES overhead)
      // This MUST be restored in finalizeReindex or on failure
      DestinationIndexOptimizer optimizer =
          new DestinationIndexOptimizer(indexBuilder, indexBuilder.getJvminfo());
      DestinationIndexOptimizer.OriginalSettings originalSettings =
          optimizer.optimizeForReindex(tempIndexName, currentHealthState, this.config);
      float requestsPerSecond = currentHealthState.getRequestsPerSecond(this.config);
      if (requestsPerSecond == -1.0f) {
        requestsPerSecond = Float.POSITIVE_INFINITY;
      }
      Map<String, Object> reindexInfo =
          indexBuilder.submitReindexInternal(
              new String[] {indexConfig.name()}, tempIndexName, indexConfig, requestsPerSecond);

      if (reindexInfo == null) {
        throw new IllegalStateException(
            "Reindex submission for "
                + indexConfig.name()
                + " failed: null response from submitReindexInternal()");
      }

      String taskId = (String) reindexInfo.get("taskId");
      if (taskId == null || taskId.isEmpty()) {
        throw new IllegalStateException(
            "Reindex submission for "
                + indexConfig.name()
                + " failed: taskId not returned from submitReindexInternal()");
      }

      return ReindexTaskInfo.builder()
          .taskId(taskId)
          .indexAlias(indexConfig.name())
          .tempIndexName(tempIndexName)
          .indexConfig(indexConfig)
          .startTime(startTime)
          .destinationOriginalSettings(originalSettings)
          .optimizer(optimizer)
          .build();

    } catch (IOException e) {
      log.error("Failed to submit reindex task for {} - IO error", indexConfig.name(), e);
      cleanupIndex(
          indexConfig.name(), tempIndexName, "after failure", "manual cleanup may be required");
      throw new ReindexIOException(
          "Failed to submit reindex task for index " + indexConfig.name(), e);
    } catch (Exception e) {
      log.error(
          "Failed to submit reindex task for {}, cleaning up temp index", indexConfig.name(), e);
      cleanupIndex(
          indexConfig.name(), tempIndexName, "after failure", "manual cleanup may be required");
      throw new ReindexSubmissionException(
          "Failed to submit reindex task for index " + indexConfig.name(), e);
    }
  }

  /**
   * Transactional restoration of destination index settings.
   *
   * <p>CRITICAL: Ensures settings are always restored to original state after reindex operations,
   * whether successful or failed. This maintains transactional semantics where destination index
   * settings are never left in an inconsistent state.
   *
   * @param taskInfo Reindex task info containing index name, temp index, settings, and optimizer
   * @throws IOException If unable to restore settings - failure must be propagated to caller
   */
  private void restoreDestinationSettings(ReindexTaskInfo taskInfo) throws IOException {
    if (taskInfo.getDestinationOriginalSettings() == null) {
      return;
    }
    taskInfo
        .getOptimizer()
        .restoreOriginalSettings(
            taskInfo.getTempIndexName(), taskInfo.getDestinationOriginalSettings());
  }

  /** Finalize reindex after task completes */
  private ReindexResult finalizeReindex(ReindexTaskInfo taskInfo) throws Exception {

    log.info("Finalizing reindex for {}", taskInfo.getIndexAlias());

    try {
      docCountValidationRetry.executeSupplier(
          () -> {
            try {
              long source = indexBuilder.getCount(taskInfo.getIndexAlias());
              long dest = indexBuilder.getCount(taskInfo.getTempIndexName());

              if (source != dest) {
                log.warn(
                    "Document count mismatch for {}: source={}, dest={} (retry via resilience4j exponential backoff)",
                    taskInfo.getIndexAlias(),
                    source,
                    dest);

                // Throw custom exception with metadata for proper classification
                throw new DocCountMismatchException(
                    String.format("Doc count mismatch: source=%d, dest=%d", source, dest),
                    source,
                    dest);
              }

              log.info(
                  "Document count match confirmed for {}: count={}",
                  taskInfo.getIndexAlias(),
                  source);
              return source;
            } catch (IOException e) {
              throw new ReindexIOException(
                  "Failed to verify document count for index " + taskInfo.getIndexAlias(), e);
            }
          });
    } catch (Exception e) {
      log.error("Doc count validation failed: {}", e.getMessage(), e);
      cleanupIndex(
          taskInfo.getIndexAlias(),
          taskInfo.getTempIndexName(),
          "Doc count validation failure",
          "manual cleanup may be required");
      // Use exception classification instead of hardcoding result
      return classifyException(e);
    }

    // CRITICAL: Restore minimal replicas BEFORE health check to prevent data loss
    // Ensures index has configured minimum replicas synced before alias swap
    try {
      // Reuse optimizer instance from taskInfo instead of creating new one
      taskInfo
          .getOptimizer()
          .restoreToMinimalReplicas(
              taskInfo.getTempIndexName(), config.getMinimumReplicasForPromotion());
    } catch (Exception e) {
      log.error(
          "Failed to restore minimal replicas for {} - blocking promotion to prevent data loss: {}",
          taskInfo.getTempIndexName(),
          e.getMessage(),
          e);
      cleanupIndex(
          taskInfo.getIndexAlias(),
          taskInfo.getTempIndexName(),
          "Could not sync minimum replica's",
          "manual cleanup may be required");
      throw new ReplicaHealthException(
          "Failed to restore minimum replicas for index " + taskInfo.getTempIndexName(), e);
    }

    // CRITICAL: Ensure replicas are synced before promoting index
    // This prevents data loss if the cluster is unhealthy
    try {
      String originalReplicas = taskInfo.getDestinationOriginalSettings().getNumberOfReplicas();
      int timeoutSeconds = config.getReplicaSyncTimeoutMinutes() * 60;
      log.info(
          "Checking replica health for {} before promotion (replicas=1->{}, timeout={}s)",
          taskInfo.getTempIndexName(),
          originalReplicas != null ? originalReplicas : "unknown",
          timeoutSeconds);
      indexBuilder.waitForIndexGreenHealth(taskInfo.getTempIndexName(), timeoutSeconds);
      log.info("Replica health check passed for {}", taskInfo.getTempIndexName());
    } catch (Exception e) {
      log.error(
          "Replica health check failed for {} - blocking promotion: {}",
          taskInfo.getTempIndexName(),
          e.getMessage(),
          e);
      cleanupIndex(
          taskInfo.getIndexAlias(),
          taskInfo.getTempIndexName(),
          "Could not sync minimum replica's",
          "manual cleanup may be required");
      throw new ReplicaHealthException(
          "Replica health check failed for index " + taskInfo.getTempIndexName(), e);
    }

    // SUCCESS: Swap aliases (which deletes old indices and points alias to new index)
    // CRITICAL: Pass null for pattern instead of wildcard to avoid circuit breaker on large
    // clusters.
    // The wildcard pattern (e.g., "datasetindex_v2*") forces ES to scan all 70+ matching indices
    // and load metadata for all of them, creating responses >972MB on 10M+ doc clusters. Using
    // null
    // leverages the alias index directly for O(1) lookup of the specific alias, avoiding metadata
    // bloat.
    try {
      indexBuilder.swapAliases(taskInfo.getIndexAlias(), null, taskInfo.getTempIndexName());
    } catch (Exception e) {
      log.error(
          "Alias swap failed for {} - temp index must be cleaned up: {}",
          taskInfo.getIndexAlias(),
          e.getMessage());
      cleanupIndex(
          taskInfo.getIndexAlias(),
          taskInfo.getTempIndexName(),
          "Alias swap failed",
          "manual cleanup may be required");
      return ReindexResult.FAILED_MONITORING_ERROR;
    }

    // Restore settings to new destination (temp index is now the destination)
    // CRITICAL: Must wrap in try-catch after alias swap succeeds. If restoration fails after
    // alias swap, the active index is left with degraded settings. This is a data integrity issue.
    // NOTE: updateIndexSettings declares throws IOException but actually throws RuntimeException
    // on retry exhaustion (retry wraps IOException and never unwraps). Catch both to ensure
    // critical error path is taken.
    try {
      restoreDestinationSettings(taskInfo);
    } catch (IOException | RuntimeException e) {
      log.error(
          "CRITICAL: Failed to restore settings after alias swap for {} - index left with degraded settings (refresh_interval=-1, replicas=0): {}",
          taskInfo.getIndexAlias(),
          e.getMessage());
      // NOTE: Do NOT cleanup index - alias swap already succeeded, index is active now.
      // Cleanup would delete the live index. Operator must manually restore settings.
      return ReindexResult.FAILED_MONITORING_ERROR;
    }

    log.info("Successfully finalized reindex for {}", taskInfo.getIndexAlias());
    return ReindexResult.REINDEXED;
  }

  /**
   * Submit finalization asynchronously to prevent blocking monitoring loop.
   *
   * <p>Returns CompletableFuture so caller can chain cleanup (e.g., removing from finalizingTasks).
   * All exceptions are caught and handled internally - future always completes normally.
   *
   * @param taskInfo Task info for the completed reindex
   * @param results Map to store finalization result
   * @param completedCount Counter to increment on success
   * @param failedCount Counter to increment on failure
   * @return CompletableFuture that completes when finalization is done (success or failure)
   */
  private CompletableFuture<Void> submitAsyncFinalization(
      ReindexTaskInfo taskInfo,
      Map<String, ReindexResult> results,
      AtomicInteger completedCount,
      AtomicInteger failedCount) {
    return CompletableFuture.runAsync(
        () -> {
          try {
            ReindexResult result = finalizeReindex(taskInfo);
            results.put(taskInfo.getIndexAlias(), result);
            // Increment appropriate counter based on result status
            if (isSuccessfulResult(result)) {
              completedCount.incrementAndGet();
              log.info(
                  "Reindex finalized asynchronously for {} (result: {})",
                  taskInfo.getIndexAlias(),
                  result);
            } else {
              failedCount.incrementAndGet();
              log.warn(
                  "Reindex finalization failed for {} (result: {})",
                  taskInfo.getIndexAlias(),
                  result);
            }
          } catch (Exception e) {
            // Classify the exception to determine appropriate failure result
            ReindexResult failureResult = classifyException(e);
            log.error(
                "Finalization failed for {}: {} (classified as {})",
                taskInfo.getIndexAlias(),
                e.getMessage(),
                failureResult);
            results.put(taskInfo.getIndexAlias(), failureResult);
            failedCount.incrementAndGet();
          }
        },
        finalizationExecutor);
  }

  /**
   * Determine if a reindex result indicates success. Success includes both complete success
   * (REINDEXED) and partial success scenarios (REINDEXED_WITH_FAILURES, or skipped/not-required
   * cases).
   *
   * @param result The reindex result to evaluate
   * @return true if the result indicates successful completion (not a FAILED_* status)
   */
  private boolean isSuccessfulResult(@Nonnull ReindexResult result) {
    return !result.name().startsWith("FAILED_");
  }

  /** Common cleanup logic with retry support for index deletion */
  private void cleanupIndex(
      String aliasName, String tempIndexName, String context, String failureMessage) {
    try {
      // CRITICAL SAFETY CHECK: Verify alias is not pointing to this index before deletion
      // If alias swap already happened during finalization, the temp index is now live
      // and must not be deleted
      boolean aliasPointsToTemp = indexBuilder.aliasPointsToIndex(aliasName, tempIndexName);
      if (aliasPointsToTemp) {
        log.warn(
            "SKIPPING cleanup of {} - alias {} currently points to it (finalization may have completed, alias already swapped)",
            tempIndexName,
            aliasName);
        return;
      }

      log.debug(
          "Alias {} does not point to {} - safe to delete temp index", aliasName, tempIndexName);

      Retry.of("index-cleanup", RetryConfigUtils.COST_ESTIMATION)
          .executeRunnable(
              () -> {
                try {
                  log.info("Cleaning up temp index {} {}", tempIndexName, context);
                  indexBuilder.deleteIndex(tempIndexName);
                  log.info("Successfully deleted temp index {}", tempIndexName);
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      failedCleanupIndices.add(tempIndexName);
      log.error(
          "Failed to cleanup temp index {} after retries - {}", tempIndexName, failureMessage);
    }
  }

  /**
   * Build an error message with actionable cleanup commands for failed indices.
   *
   * @param failedIndices Set of index names that failed cleanup
   * @return Error message with exact DELETE commands needed
   */
  private String buildCleanupErrorMessage(Set<String> failedIndices) {
    StringBuilder msg = new StringBuilder();
    msg.append("Reindex completed but failed to cleanup ")
        .append(failedIndices.size())
        .append(" temporary indices.\n");
    msg.append("MANUAL CLEANUP REQUIRED. Execute these commands in Elasticsearch:\n\n");

    for (String indexName : failedIndices) {
      msg.append("DELETE /").append(indexName).append("\n");
    }

    msg.append("\nOr delete by pattern:\n");
    msg.append("DELETE /*_")
        .append(System.currentTimeMillis() / 1000 / 86400)
        .append("*/ (approximate)\n");
    msg.append("\nIndices: ").append(failedIndices);

    return msg.toString();
  }

  /**
   * Classify indices by cost tier for smart scheduling (P=2). Uses resilience4j retry pattern for
   * cost estimation with exponential backoff. Falls back to sequential mode on failure.
   *
   * @param configs Reindex configurations to classify
   * @return Map from CostTier to list of configs in that tier
   */
  private Map<IndexCostEstimator.CostTier, List<ReindexConfig>> classifyIndicesByCost(
      List<ReindexConfig> configs) {
    try {
      return Retry.of("cost-estimation", RetryConfigUtils.COST_ESTIMATION)
          .executeSupplier(
              () -> {
                try {
                  IndexCostEstimator estimator =
                      new IndexCostEstimator(indexBuilder, config.getNormalIndexCostThreshold());

                  // Extract index names from configs
                  List<String> indexNames =
                      configs.stream().map(ReindexConfig::name).collect(Collectors.toList());

                  // Estimate costs
                  List<IndexCostEstimator.IndexCostInfo> costs =
                      estimator.estimateIndexCosts(indexNames);

                  // Create mapping from name to cost tier
                  Map<String, IndexCostEstimator.CostTier> tierMap = new HashMap<>();
                  for (IndexCostEstimator.IndexCostInfo cost : costs) {
                    tierMap.put(cost.getIndexName(), cost.getTier());
                  }

                  // Group configs by tier
                  return configs.stream()
                      .collect(
                          Collectors.groupingBy(
                              cfg ->
                                  tierMap.getOrDefault(
                                      cfg.name(),
                                      IndexCostEstimator.CostTier
                                          .NORMAL))); // Default to NORMAL if not found
                } catch (IOException e) {
                  throw new RuntimeException(e);
                }
              });
    } catch (Exception e) {
      log.error(
          "Cost estimation failed after retries - falling back to SEQUENTIAL mode: {}",
          e.getMessage());
      return Map.of(IndexCostEstimator.CostTier.LARGE, configs);
    }
  }

  /**
   * Execute reindex with cost-aware concurrency limits (P=2).
   *
   * <p>Execution strategy: - Monitor and throttle large and normal indices separately
   *
   * @param largeIndices LARGE tier indices (serialize)
   * @param normalIndices NORMAL tier indices (parallelize)
   * @return Map of index name to reindex result
   * @throws InterruptedException if interrupted during monitoring
   */
  private Map<String, ReindexResult> executeReindexWithCostAwareness(
      List<ReindexConfig> largeIndices, List<ReindexConfig> normalIndices) {

    Map<String, ReindexResult> allResults = new ConcurrentHashMap<>();

    try {
      if (!largeIndices.isEmpty()) {
        log.info("Starting execution of {} LARGE tier indices (serialized)", largeIndices.size());
        executeReindexTiered(largeIndices, config.getMaxConcurrentLargeReindex(), allResults);
      }

      if (!normalIndices.isEmpty()) {
        log.info(
            "Starting execution of {} NORMAL tier indices (max {} concurrent)",
            normalIndices.size(),
            config.getMaxConcurrentNormalReindex());
        executeReindexTiered(normalIndices, config.getMaxConcurrentNormalReindex(), allResults);
      }
    } finally {
      // Shutdown finalizationExecutor AFTER both tiers complete (not inside executeReindexTiered)
      log.info("Shutting down finalization executor after all tiers complete");
      finalizationExecutor.shutdown();
      try {
        if (!finalizationExecutor.awaitTermination(config.getMaxReindexHours(), TimeUnit.HOURS)) {
          log.error(
              "Finalization executor did not terminate within {} hours - some tasks may be incomplete",
              config.getMaxReindexHours());
          List<Runnable> remainingTasks = finalizationExecutor.shutdownNow();
          log.error("Force-shutdown {} remaining finalization tasks", remainingTasks.size());
        }
      } catch (InterruptedException e) {
        log.error("Interrupted while waiting for finalization tasks to complete");
        finalizationExecutor.shutdownNow();
        Thread.currentThread().interrupt();
      }
    }

    return allResults;
  }

  /**
   * Execute reindex for a group of configs with a specific concurrency limit.
   *
   * <p>Main loop is now passive - reads state changes from queue instead of polling health checks
   * directly. When state changes occur, rethrottles all active tasks to the new tier's RPS limit.
   *
   * @param configs Configs to process in this batch
   * @param maxConcurrency Maximum concurrent tasks for this tier
   * @param results Map to accumulate results
   */
  private void executeReindexTiered(
      List<ReindexConfig> configs, int maxConcurrency, Map<String, ReindexResult> results) {

    Queue<ReindexConfig> pendingConfigs = new ArrayDeque<>(configs);
    Map<String, ReindexTaskInfo> activeTasks = new ConcurrentHashMap<>();
    Set<String> finalizingTasks = ConcurrentHashMap.newKeySet();

    long startTime = System.currentTimeMillis();
    long timeoutMs = config.getMaxReindexHours() * 60L * 60L * 1000L;
    long checkIntervalMs = config.getTaskCheckIntervalSeconds() * 1000L;

    AtomicInteger completedCount = new AtomicInteger(0);
    AtomicInteger failedCount = new AtomicInteger(0);

    // Initialize health tracking from current circuit breaker state
    CircuitBreakerState.HealthState currentHealthState = circuitBreakerState.getCurrentState();
    log.info("Starting reindex with initial health state: {}", currentHealthState);

    // Main loop: submit, monitor, refill
    // Continue while ANY work remains: pending configs, active reindex tasks, or pending
    // finalizations
    while (!pendingConfigs.isEmpty() || !activeTasks.isEmpty() || !finalizingTasks.isEmpty()) {

      // Check for overall timeout
      if (checkForTimeout(startTime, timeoutMs, activeTasks, pendingConfigs, results)) {
        break;
      }

      // Phase 1: Check for health state changes and rethrottle active tasks if needed
      CircuitBreakerState.HealthState newHealthState = circuitBreakerState.getCurrentState();
      if (newHealthState != currentHealthState) {
        handleHealthStateChange(newHealthState, currentHealthState, activeTasks);
        currentHealthState = newHealthState;
      }

      // Phase 2: Fill active slots with health-state-aware scroll size
      // In RED state, this skips submissions to let existing tasks finish
      fillActiveSlots(
          pendingConfigs, activeTasks, results, failedCount, maxConcurrency, currentHealthState);

      // Phase 3: Monitor active tasks (always run to detect completions)
      if (!activeTasks.isEmpty()) {
        monitorAndCompleteActiveTasks(
            activeTasks, finalizingTasks, results, completedCount, failedCount);
      }

      // Phase 4: Sleep before next check
      MonitoringContext monitoringContext =
          new MonitoringContext(
              checkIntervalMs,
              activeTasks,
              results,
              failedCount,
              pendingConfigs,
              currentHealthState,
              completedCount,
              finalizingTasks);
      if (!sleepBetweenChecks(monitoringContext)) {
        break; // Interrupted
      }
    }

    log.info(
        "Tier execution complete: {} succeeded, {} failed for {} indices",
        completedCount.get(),
        failedCount.get(),
        configs.size());
  }

  /**
   * Fill active slots with pending tasks using health-state-appropriate scroll size.
   *
   * <p>In RED state, prevents new task submissions to allow existing tasks to complete without
   * cluster overload. In YELLOW and GREEN states, submits new tasks with appropriate batch sizes.
   *
   * @param pendingConfigs Queue of pending reindex configs
   * @param activeTasks Map of active reindex tasks
   * @param results Map to accumulate results
   * @param failedCount Atomic counter of failed tasks
   * @param maxConcurrency Maximum concurrent tasks allowed
   * @param currentHealthState Current cluster health state
   */
  private void fillActiveSlots(
      Queue<ReindexConfig> pendingConfigs,
      Map<String, ReindexTaskInfo> activeTasks,
      Map<String, ReindexResult> results,
      AtomicInteger failedCount,
      int maxConcurrency,
      CircuitBreakerState.HealthState currentHealthState) {
    // In RED state, do NOT submit new tasks - let running tasks finish
    if (currentHealthState == CircuitBreakerState.HealthState.RED) {
      log.debug(
          "Cluster in RED state - not submitting new tasks, letting {} active tasks finish",
          activeTasks.size());
      return;
    }

    if (!pendingConfigs.isEmpty()) {
      submitPendingTasks(
          pendingConfigs, activeTasks, results, failedCount, maxConcurrency, currentHealthState);
    } else {
      log.debug("No pending tasks to submit");
    }
  }

  /** Context object for sleep/monitoring operations between reindex iterations */
  private static class MonitoringContext {
    final long checkIntervalMs;
    final Map<String, ReindexTaskInfo> activeTasks;
    final Map<String, ReindexResult> results;
    final AtomicInteger failedCount;
    final Queue<ReindexConfig> pendingConfigs;
    final CircuitBreakerState.HealthState currentHealthState;
    final AtomicInteger completedCount;
    final Set<String> finalizingTasks;

    MonitoringContext(
        long checkIntervalMs,
        Map<String, ReindexTaskInfo> activeTasks,
        Map<String, ReindexResult> results,
        AtomicInteger failedCount,
        Queue<ReindexConfig> pendingConfigs,
        CircuitBreakerState.HealthState currentHealthState,
        AtomicInteger completedCount,
        Set<String> finalizingTasks) {
      this.checkIntervalMs = checkIntervalMs;
      this.activeTasks = activeTasks;
      this.results = results;
      this.failedCount = failedCount;
      this.pendingConfigs = pendingConfigs;
      this.currentHealthState = currentHealthState;
      this.completedCount = completedCount;
      this.finalizingTasks = finalizingTasks;
    }
  }

  /**
   * Sleep between iteration checks with graceful handling of interruptions.
   *
   * @param context Monitoring context containing all necessary state
   * @return true if sleep completed normally, false if interrupted
   */
  private boolean sleepBetweenChecks(MonitoringContext context) {
    if (!context.activeTasks.isEmpty()
        || !context.pendingConfigs.isEmpty()
        || !context.finalizingTasks.isEmpty()) {
      log.debug(
          "Tier monitoring: {} active, {} pending, {} finalizing, {} completed, {} failed (current health status: {})",
          context.activeTasks.size(),
          context.pendingConfigs.size(),
          context.finalizingTasks.size(),
          context.completedCount.get(),
          context.failedCount.get(),
          context.currentHealthState);
      try {
        Thread.sleep(context.checkIntervalMs);
        return true;
      } catch (InterruptedException e) {
        log.warn(
            "Reindex monitoring interrupted - cleaning up {} active tasks",
            context.activeTasks.size());
        int cleanupFailures = 0;
        for (ReindexTaskInfo taskInfo : context.activeTasks.values()) {
          context.results.put(taskInfo.getIndexAlias(), ReindexResult.FAILED_MONITORING_ERROR);
          context.failedCount.incrementAndGet();

          // Restore destination index settings to original state
          try {
            restoreDestinationSettings(taskInfo);
          } catch (Exception restoreError) {
            log.error(
                "Failed to restore settings for {} during interruption: {} - index may be left with degraded settings",
                taskInfo.getIndexAlias(),
                restoreError.getMessage());
            cleanupFailures++;
          }

          // Delete temporary index created during reindex
          try {
            cleanupIndex(
                taskInfo.getIndexAlias(),
                taskInfo.tempIndexName,
                "(during interruption handler)",
                "Interruption");
          } catch (Exception cleanupError) {
            log.error(
                "Cleanup failed for {} during interruption: {}",
                taskInfo.getIndexAlias(),
                cleanupError.getMessage());
            cleanupFailures++;
          }
        }
        if (cleanupFailures > 0) {
          log.error(
              "CRITICAL: {} indices failed cleanup/restore during interruption - "
                  + "temp indices may be orphaned or settings may need manual restoration",
              cleanupFailures);
          failedCleanupIndices.addAll(
              context.activeTasks.values().stream()
                  .map(ReindexTaskInfo::getIndexAlias)
                  .collect(java.util.stream.Collectors.toSet()));
        }
        Thread.currentThread().interrupt();
        return false;
      }
    }
    return true;
  }

  /**
   * Handle health state change by rethrottling all active tasks and updating refresh_interval.
   *
   * <p>Executes two parallel operations: 1. Rethrottles all active reindex tasks to new RPS limit
   * (up to 8 parallel) 2. Bulk updates refresh_interval on all destination indices
   *
   * @param newHealthState New cluster health state
   * @param previousHealthState Previous cluster health state (for logging)
   * @param activeTasks Map of active reindex tasks to update
   */
  private void handleHealthStateChange(
      @Nonnull final CircuitBreakerState.HealthState newHealthState,
      @Nonnull final CircuitBreakerState.HealthState previousHealthState,
      @Nonnull final Map<String, ReindexTaskInfo> activeTasks) {
    log.info(
        "Health state changed: {} -> {}. Rethrottling {} active tasks.",
        previousHealthState,
        newHealthState,
        activeTasks.size());

    // Get new RPS for the new health state
    float newRps = newHealthState.getRequestsPerSecond(config);

    // Parallel rethrottle of all active tasks to reduce latency
    if (!activeTasks.isEmpty()) {
      List<String> failedTaskIds = Collections.synchronizedList(new ArrayList<>());

      // Submit all rethrottle operations in parallel using CompletableFuture
      List<CompletableFuture<Void>> futures =
          activeTasks.keySet().stream()
              .map(
                  taskId ->
                      CompletableFuture.runAsync(
                          () -> {
                            try {
                              indexBuilder.rethrottleTask(taskId, newRps);
                              log.debug(
                                  "Rethrottled task {} to health state {} (RPS: {})",
                                  taskId,
                                  newHealthState,
                                  newRps > 0 ? newRps : "unlimited");
                            } catch (Exception e) {
                              failedTaskIds.add(taskId);
                              log.debug("Failed to rethrottle task {}: {}", taskId, e.getMessage());
                            }
                          },
                          rethrottleExecutor))
              .toList();

      // Wait for all rethrottle operations to complete with 5 second timeout
      try {
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get(5, TimeUnit.SECONDS);

        // Log summary
        if (!failedTaskIds.isEmpty()) {
          log.warn(
              "Rethrottled {} of {} tasks to {} (RPS: {}). Failed: {}",
              activeTasks.size() - failedTaskIds.size(),
              activeTasks.size(),
              newHealthState,
              newRps > 0 ? newRps : "unlimited",
              failedTaskIds.size());
        } else {
          log.info(
              "Rethrottled {} tasks to {} (RPS: {})",
              activeTasks.size(),
              newHealthState,
              newRps > 0 ? newRps : "unlimited");
        }
      } catch (TimeoutException e) {
        log.warn(
            "Rethrottle timed out after 5 seconds, continuing anyway. "
                + "RPS will be updated for {} active tasks.",
            activeTasks.size());
      } catch (Exception e) {
        log.warn("Rethrottle interrupted: {}", e.getMessage());
      }
    }

    // Bulk update refresh_interval for all destination indices based on new health state
    if (!activeTasks.isEmpty()) {
      String newRefreshInterval = newHealthState.getRefreshInterval(config);
      String[] destinationIndices =
          activeTasks.values().stream()
              .map(ReindexTaskInfo::getTempIndexName)
              .toArray(String[]::new);
      try {
        Settings settings =
            Settings.builder()
                .put(ESIndexBuilder.INDEX_REFRESH_INTERVAL, newRefreshInterval)
                .build();
        indexBuilder.updateIndexSettings(destinationIndices, settings);
        log.info(
            "Bulk updated refresh_interval on {} destination indices → {} (state: {})",
            activeTasks.size(),
            newRefreshInterval,
            newHealthState);
      } catch (Exception e) {
        log.warn(
            "Failed to bulk update refresh_interval on {} indices: {}",
            activeTasks.size(),
            e.getMessage());
      }
    }
  }

  /**
   * Gracefully shutdown rethrottle executor. Finalization executor is shut down in
   * executeReindexWithCostAwareness() finally block after all tiers complete, ensuring all
   * finalization tasks have time to finish before exiting.
   */
  public void shutdown() {
    rethrottleExecutor.shutdown();
    try {
      // Wait for executor to finish before returning
      if (!rethrottleExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
        log.warn("Rethrottle executor did not terminate within 30 seconds");
        rethrottleExecutor.shutdownNow();
      }
    } catch (InterruptedException e) {
      log.warn("Interrupted while waiting for rethrottle executor to shutdown");
      rethrottleExecutor.shutdownNow();
      Thread.currentThread().interrupt();
    }
  }

  /** Container for reindex task information */
  @Builder
  @Data
  private static class ReindexTaskInfo {
    String taskId;
    String indexAlias;
    String tempIndexName;
    ReindexConfig indexConfig;
    long startTime;
    // Transactional destination optimization
    DestinationIndexOptimizer.OriginalSettings destinationOriginalSettings;
    // Optimizer instance reused across all optimization operations (submit, restore, failure)
    DestinationIndexOptimizer optimizer;
    // Track consecutive null responses (if task doesn't exist in ES after 5 checks)
    @Builder.Default int consecutiveNullResponses = 0;
  }
}
