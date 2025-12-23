package com.linkedin.datahub.upgrade.system.semanticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.tasks.TaskInfo;

/**
 * Upgrade step that copies documents from base entity indices to semantic search indices.
 *
 * <p>Note: This step only copies document metadata. Embeddings are populated separately via the
 * SemanticContent aspect, which is emitted by ingestion connectors.
 */
@Slf4j
public class CopyDocumentsToSemanticIndexStep implements UpgradeStep {

  private static final String UPGRADE_ID_PREFIX = "CopyDocumentsToSemanticIndex";
  private static final long TASK_POLL_INTERVAL_MS = 5000; // 5 seconds
  private static final long TASK_TIMEOUT_MS = 3600000; // 1 hour

  private final OperationContext opContext;
  private final String entityName;
  private final String upgradeId;
  private final Urn upgradeIdUrn;
  private final SearchClientShim<?> searchClient;
  private final EntityService<?> entityService;
  private final IndexConvention indexConvention;

  public CopyDocumentsToSemanticIndexStep(
      OperationContext opContext,
      String entityName,
      SearchClientShim<?> searchClient,
      EntityService<?> entityService,
      IndexConvention indexConvention) {
    this.opContext = opContext;
    this.entityName = entityName;
    this.searchClient = searchClient;
    this.entityService = entityService;
    this.indexConvention = indexConvention;

    upgradeId = UPGRADE_ID_PREFIX + "_" + entityName;
    upgradeIdUrn = BootstrapStep.getUpgradeUrn(upgradeId);
  }

  @Override
  public String id() {
    return upgradeId;
  }

  private UpgradeStepResult execute(UpgradeContext context) {
    try {
      String baseIndexName = indexConvention.getEntityIndexName(entityName);
      String semanticIndexName = indexConvention.getEntityIndexNameSemantic(entityName);
      log.info(
          "Starting document copy for entity '{}': {} -> {}",
          entityName,
          baseIndexName,
          semanticIndexName);

      // Check if semantic index exists
      GetIndexRequest getIndexRequest = new GetIndexRequest(semanticIndexName);
      if (!searchClient.indexExists(getIndexRequest, RequestOptions.DEFAULT)) {
        log.error("Semantic index '{}' does not exist. Skipping.", semanticIndexName);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      // Submit reindex task
      ReindexRequest reindexRequest =
          new ReindexRequest()
              .setSourceIndices(baseIndexName)
              .setDestIndex(semanticIndexName)
              .setMaxRetries(3)
              .setAbortOnVersionConflict(false);

      String taskId = searchClient.submitReindexTask(reindexRequest, RequestOptions.DEFAULT);
      log.info("Document copy task submitted for entity '{}'. Task ID: {}", entityName, taskId);

      // Wait for the reindex task to complete
      if (!waitForTaskCompletion(taskId)) {
        log.error("Reindex task {} failed or timed out for entity '{}'", taskId, entityName);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      log.info("Document copy completed successfully for entity '{}'", entityName);
      BootstrapStep.setUpgradeResult(opContext, upgradeIdUrn, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    } catch (Exception e) {
      log.error("Failed to copy documents for entity: {}", entityName, e);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
  }

  /**
   * Wait for an OpenSearch task to complete.
   *
   * @param taskId The task ID in format "nodeId:taskId"
   * @return true if task completed successfully, false otherwise
   */
  private boolean waitForTaskCompletion(String taskId) {
    String[] parts = taskId.split(":");
    if (parts.length != 2) {
      log.error("Invalid task ID format: {}", taskId);
      return false;
    }

    GetTaskRequest taskRequest = new GetTaskRequest(parts[0], Long.parseLong(parts[1]));
    long startTime = System.currentTimeMillis();

    while (System.currentTimeMillis() - startTime < TASK_TIMEOUT_MS) {
      try {
        Optional<GetTaskResponse> responseOpt =
            searchClient.getTask(taskRequest, RequestOptions.DEFAULT);

        if (responseOpt.isEmpty()) {
          // Task not found - may have completed and been cleaned up
          log.warn("Task {} not found, assuming completed", taskId);
          return true;
        }

        GetTaskResponse response = responseOpt.get();
        if (response.isCompleted()) {
          TaskInfo taskInfo = response.getTaskInfo();
          if (taskInfo.isCancelled()) {
            log.error("Task {} was cancelled", taskId);
            return false;
          }
          log.info("Task {} completed successfully", taskId);
          return true;
        }

        log.debug("Task {} still running...", taskId);

        Thread.sleep(TASK_POLL_INTERVAL_MS);
      } catch (InterruptedException e) {
        // Restore interrupt flag - likely caused by JVM shutdown (e.g., container restart
        // during deployment). Returning false leaves upgrade as non-SUCCEEDED so it retries
        // on next startup.
        Thread.currentThread().interrupt();
        log.error("Interrupted while waiting for task {}", taskId);
        return false;
      } catch (Exception e) {
        log.error("Error checking task status for {}", taskId, e);
        return false;
      }
    }

    log.error("Task {} timed out after {} ms", taskId, TASK_TIMEOUT_MS);
    return false;
  }

  @Override
  public Function<UpgradeContext, UpgradeStepResult> executable() {
    return this::execute;
  }

  @Override
  public boolean skip(UpgradeContext context) {
    // Check if this upgrade has already completed successfully
    Optional<DataHubUpgradeResult> prevResult =
        context.upgrade().getUpgradeResult(opContext, upgradeIdUrn, entityService);

    boolean previousRunSucceeded =
        prevResult
            .filter(result -> DataHubUpgradeState.SUCCEEDED.equals(result.getState()))
            .isPresent();

    if (previousRunSucceeded) {
      log.info("{} was already completed successfully. Skipping.", id());
    }
    return previousRunSucceeded;
  }

  @Override
  public boolean isOptional() {
    // This is optional - don't block other upgrades if this fails
    return true;
  }
}
