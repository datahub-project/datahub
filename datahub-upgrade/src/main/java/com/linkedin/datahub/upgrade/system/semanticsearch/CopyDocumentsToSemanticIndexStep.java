package com.linkedin.datahub.upgrade.system.semanticsearch;

import com.linkedin.common.urn.Urn;
import com.linkedin.datahub.upgrade.UpgradeContext;
import com.linkedin.datahub.upgrade.UpgradeStep;
import com.linkedin.datahub.upgrade.UpgradeStepResult;
import com.linkedin.datahub.upgrade.impl.DefaultUpgradeStepResult;
import com.linkedin.metadata.boot.BootstrapStep;
import com.linkedin.metadata.config.search.SearchComponent;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.SearchClusterAccess;
import com.linkedin.upgrade.DataHubUpgradeResult;
import com.linkedin.upgrade.DataHubUpgradeState;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Optional;
import java.util.function.Function;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.ReindexRequest;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.SortOrder;
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

  private static final int CROSS_CLUSTER_PAGE_SIZE = 500;

  private final OperationContext opContext;
  private final String entityName;
  private final String upgradeId;
  private final Urn upgradeIdUrn;
  private final EntityService<?> entityService;
  private final IndexConvention indexConvention;

  public CopyDocumentsToSemanticIndexStep(
      OperationContext opContext,
      String entityName,
      EntityService<?> entityService,
      IndexConvention indexConvention) {
    this.opContext = opContext;
    this.entityName = entityName;
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
      String baseIndexName = indexConvention.getEntityIndexName(opContext, entityName);
      String semanticIndexName = indexConvention.getEntityIndexNameSemantic(opContext, entityName);
      log.info(
          "Starting document copy for entity '{}': {} -> {}",
          entityName,
          baseIndexName,
          semanticIndexName);

      SearchClusterAccess access = opContext.getSearchContext().requireSearchClusterAccess();
      SearchClientShim<?> sourceClient = access.clientFor(SearchComponent.SEARCH_V2);
      SearchClientShim<?> destClient = access.clientFor(SearchComponent.SEMANTIC);

      GetIndexRequest getIndexRequest = new GetIndexRequest(semanticIndexName);
      if (!destClient.indexExists(opContext, getIndexRequest, RequestOptions.DEFAULT)) {
        log.error("Semantic index '{}' does not exist. Skipping.", semanticIndexName);
        return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
      }

      if (sourceClient == destClient) {
        if (!copySameCluster(sourceClient, baseIndexName, semanticIndexName)) {
          return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
        }
      } else {
        copyAcrossClusters(sourceClient, destClient, baseIndexName, semanticIndexName);
      }

      log.info("Document copy completed successfully for entity '{}'", entityName);
      BootstrapStep.setUpgradeResult(opContext, upgradeIdUrn, entityService);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.SUCCEEDED);
    } catch (Exception e) {
      log.error("Failed to copy documents for entity: {}", entityName, e);
      return new DefaultUpgradeStepResult(id(), DataHubUpgradeState.FAILED);
    }
  }

  private boolean copySameCluster(
      SearchClientShim<?> searchClient, String baseIndexName, String semanticIndexName)
      throws IOException {
    ReindexRequest reindexRequest =
        new ReindexRequest()
            .setSourceIndices(baseIndexName)
            .setDestIndex(semanticIndexName)
            .setMaxRetries(3)
            .setAbortOnVersionConflict(false);

    String taskId =
        searchClient.submitReindexTask(opContext, reindexRequest, RequestOptions.DEFAULT);
    log.info("Document copy task submitted for entity '{}'. Task ID: {}", entityName, taskId);

    if (!waitForTaskCompletion(searchClient, taskId)) {
      log.error("Reindex task {} failed or timed out for entity '{}'", taskId, entityName);
      return false;
    }
    return true;
  }

  private void copyAcrossClusters(
      SearchClientShim<?> sourceClient,
      SearchClientShim<?> destClient,
      String baseIndexName,
      String semanticIndexName)
      throws IOException {
    SearchSourceBuilder source = new SearchSourceBuilder();
    source.query(QueryBuilders.matchAllQuery());
    source.size(CROSS_CLUSTER_PAGE_SIZE);
    // `_id` is not a sortable field on OpenSearch/ES 7+. V2 entity documents have unique `urn`.
    source.sort("urn", SortOrder.ASC);

    SearchRequest searchRequest = new SearchRequest(baseIndexName);
    searchRequest.source(source);

    SearchResponse searchResponse =
        sourceClient.search(opContext, searchRequest, RequestOptions.DEFAULT);
    SearchHit[] hits = searchResponse.getHits().getHits();
    while (hits.length > 0) {
      for (SearchHit hit : hits) {
        IndexRequest indexRequest =
            new IndexRequest(semanticIndexName)
                .id(hit.getId())
                .source(hit.getSourceAsString(), XContentType.JSON);
        destClient.indexDocument(opContext, indexRequest, RequestOptions.DEFAULT);
      }
      Object[] sortValues = hits[hits.length - 1].getSortValues();
      source.searchAfter(sortValues);
      searchResponse = sourceClient.search(opContext, searchRequest, RequestOptions.DEFAULT);
      hits = searchResponse.getHits().getHits();
    }
  }

  /**
   * Wait for an OpenSearch task to complete.
   *
   * @param taskId The task ID in format "nodeId:taskId"
   * @return true if task completed successfully, false otherwise
   */
  private boolean waitForTaskCompletion(SearchClientShim<?> searchClient, String taskId) {
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
