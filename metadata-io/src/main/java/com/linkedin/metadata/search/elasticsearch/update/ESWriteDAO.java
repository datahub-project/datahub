package com.linkedin.metadata.search.elasticsearch.update;

import static org.opensearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES;
import static org.opensearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES_VALUE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.client.tasks.TaskSubmissionResponse;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

@Slf4j
@RequiredArgsConstructor
public class ESWriteDAO {
  private final ElasticSearchConfiguration config;
  private final RestHighLevelClient searchClient;
  private final ESBulkProcessor bulkProcessor;

  /** Result of a delete by query operation */
  @Data
  @Builder
  public static class DeleteByQueryResult {
    private final long timeTaken;
    private final boolean success;
    private final String failureReason;
    private final long remainingDocuments;
    private final int retryAttempts;
    private final TaskId taskId;
  }

  /**
   * Updates or inserts the given search document.
   *
   * @param entityName name of the entity
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String document,
      @Nonnull String docId) {
    final UpdateRequest updateRequest =
        new UpdateRequest(toIndexName(opContext, entityName), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(config.getBulkProcessor().getNumRetries());

    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the document with the given document ID from the index.
   *
   * @param entityName name of the entity
   * @param docId the ID of the document to delete
   */
  public void deleteDocument(
      @Nonnull OperationContext opContext, @Nonnull String entityName, @Nonnull String docId) {
    bulkProcessor.add(new DeleteRequest(toIndexName(opContext, entityName)).id(docId));
  }

  /** Applies a script to a particular document */
  public void applyScriptUpdate(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String docId,
      @Nonnull String scriptSource,
      @Nonnull Map<String, Object> scriptParams,
      Map<String, Object> upsert) {
    // Create a properly parameterized script
    Script script =
        new Script(
            ScriptType.INLINE, // Use INLINE for scripts provided in the request
            "painless", // Specify the script language as painless
            scriptSource, // The script source code
            scriptParams // The parameters map
            );
    UpdateRequest updateRequest =
        new UpdateRequest(toIndexName(opContext, entityName), docId)
            .detectNoop(false)
            .scriptedUpsert(true)
            .retryOnConflict(config.getBulkProcessor().getNumRetries())
            .script(script)
            .upsert(upsert);
    bulkProcessor.add(updateRequest);
  }

  /** Clear all documents in all the indices */
  public void clear(@Nonnull OperationContext opContext) {
    String[] indices =
        getIndices(opContext.getSearchContext().getIndexConvention().getAllEntityIndicesPattern());
    bulkProcessor.deleteByQuery(QueryBuilders.matchAllQuery(), indices);
  }

  private String[] getIndices(String pattern) {
    try {
      GetIndexResponse response =
          searchClient.indices().get(new GetIndexRequest(pattern), RequestOptions.DEFAULT);
      return response.getIndices();
    } catch (IOException e) {
      log.error("Failed to get indices using pattern {}", pattern);
      return new String[] {};
    }
  }

  /**
   * Performs an async delete by query operation (non-blocking) Returns immediately with a
   * CompletableFuture containing the task ID
   */
  @Nonnull
  public CompletableFuture<TaskSubmissionResponse> deleteByQueryAsync(
      @Nonnull String indexName,
      @Nonnull QueryBuilder query,
      @Nullable BulkDeleteConfiguration overrideConfig) {

    final BulkDeleteConfiguration finalConfig =
        overrideConfig != null ? overrideConfig : config.getBulkDelete();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            DeleteByQueryRequest request = buildDeleteByQueryRequest(indexName, query, finalConfig);

            // Submit the task asynchronously
            TaskSubmissionResponse taskId =
                searchClient.submitDeleteByQueryTask(request, RequestOptions.DEFAULT);

            log.info("Started async delete by query task: {} for index: {}", taskId, indexName);

            return taskId;
          } catch (IOException e) {
            log.error("Failed to start async delete by query for index: {}", indexName, e);
            throw new RuntimeException("Failed to start async delete by query", e);
          }
        });
  }

  /**
   * Performs asynchronous delete by query operation with monitoring Blocks until completion and
   * retries if documents remain
   */
  @Nonnull
  public DeleteByQueryResult deleteByQuerySync(
      @Nonnull String indexName,
      @Nonnull QueryBuilder query,
      @Nullable BulkDeleteConfiguration overrideConfig) {

    final BulkDeleteConfiguration finalConfig =
        overrideConfig != null ? overrideConfig : config.getBulkDelete();

    long startTime = System.currentTimeMillis();
    long totalDeleted = 0;
    int retryAttempts = 0;
    TaskId lastTaskId = null;

    try {
      // Get initial document count
      long initialCount = countDocuments(indexName, query);
      if (initialCount == 0) {
        return DeleteByQueryResult.builder()
            .timeTaken(System.currentTimeMillis() - startTime)
            .success(true)
            .remainingDocuments(0)
            .retryAttempts(0)
            .build();
      }

      log.info("Starting delete by query for index: {} with {} documents", indexName, initialCount);

      long remainingDocs = initialCount;
      long previousRemainingDocs = initialCount;

      while (remainingDocs > 0 && retryAttempts < finalConfig.getNumRetries()) {
        long countBeforeDelete = remainingDocs;

        // Submit delete by query task
        DeleteByQueryRequest request = buildDeleteByQueryRequest(indexName, query, finalConfig);

        TaskSubmissionResponse taskSubmission =
            searchClient.submitDeleteByQueryTask(request, RequestOptions.DEFAULT);
        TaskId taskId = parseTaskId(taskSubmission.getTask());
        lastTaskId = taskId;

        log.info("Submitted delete by query task: {} for index: {}", taskId, indexName);

        // Monitor the task with context for proper tracking
        DeleteByQueryResult iterationResult =
            monitorDeleteByQueryTask(taskId, finalConfig.getTimeoutDuration(), indexName, query);

        // Calculate deleted count based on document count change
        remainingDocs = iterationResult.getRemainingDocuments();
        long deleted = 0;

        if (remainingDocs >= 0 && previousRemainingDocs >= 0) {
          deleted = previousRemainingDocs - remainingDocs;
          if (deleted > 0) {
            totalDeleted += deleted;
          }
        }

        previousRemainingDocs = remainingDocs;

        log.info(
            "Delete by query iteration completed. Deleted: {}, Remaining: {}, Total deleted: {}",
            deleted,
            remainingDocs,
            totalDeleted);

        // Check if we made progress
        if (!iterationResult.isSuccess()) {
          // Task failed or timed out
          return DeleteByQueryResult.builder()
              .timeTaken(System.currentTimeMillis() - startTime)
              .success(false)
              .failureReason(iterationResult.getFailureReason())
              .remainingDocuments(remainingDocs)
              .retryAttempts(retryAttempts)
              .taskId(lastTaskId)
              .build();
        } else if (deleted == 0 && remainingDocs == countBeforeDelete && remainingDocs > 0) {
          log.warn("Delete by query made no progress. Documents remaining: {}", remainingDocs);
          if (retryAttempts < finalConfig.getNumRetries() - 1) {
            retryAttempts++;
            // Add a small delay before retry
            Thread.sleep(finalConfig.getPollDuration().toMillis());
          } else {
            // Final attempt made no progress
            return DeleteByQueryResult.builder()
                .timeTaken(System.currentTimeMillis() - startTime)
                .success(false)
                .failureReason(
                    "Delete operation made no progress after " + (retryAttempts + 1) + " attempts")
                .remainingDocuments(remainingDocs)
                .retryAttempts(retryAttempts)
                .taskId(lastTaskId)
                .build();
          }
        } else if (remainingDocs > 0) {
          retryAttempts++;
          log.info(
              "Retrying delete by query. Attempt {} of {}",
              retryAttempts + 1,
              finalConfig.getNumRetries());
        }
      }

      return DeleteByQueryResult.builder()
          .timeTaken(System.currentTimeMillis() - startTime)
          .success(remainingDocs == 0)
          .failureReason(remainingDocs > 0 ? "Documents still remaining after max retries" : null)
          .remainingDocuments(remainingDocs)
          .retryAttempts(retryAttempts)
          .taskId(lastTaskId)
          .build();

    } catch (Exception e) {
      log.error("Delete by query failed for index: {}", indexName, e);
      return DeleteByQueryResult.builder()
          .timeTaken(System.currentTimeMillis() - startTime)
          .success(false)
          .failureReason("Exception: " + e.getMessage())
          .remainingDocuments(-1) // Unknown
          .retryAttempts(retryAttempts)
          .taskId(lastTaskId)
          .build();
    }
  }

  /**
   * Monitor the status of an async delete by query task For internal use with context to properly
   * track deletions
   */
  @VisibleForTesting
  @Nonnull
  public DeleteByQueryResult monitorDeleteByQueryTask(
      @Nonnull TaskId taskId,
      @Nullable Duration timeout,
      @Nonnull String indexName,
      @Nonnull QueryBuilder query) {

    Duration finalTimeout = timeout != null ? timeout : config.getBulkDelete().getTimeoutDuration();
    long startTime = System.currentTimeMillis();

    try {
      GetTaskRequest getTaskRequest = new GetTaskRequest(taskId.getNodeId(), taskId.getId());
      getTaskRequest.setWaitForCompletion(true);
      getTaskRequest.setTimeout(TimeValue.timeValueMillis(finalTimeout.toMillis()));

      Optional<GetTaskResponse> taskResponse =
          searchClient.tasks().get(getTaskRequest, RequestOptions.DEFAULT);

      if (taskResponse.isEmpty() || !taskResponse.get().isCompleted()) {
        // Count remaining documents to determine if any progress was made
        long remainingDocs = countDocuments(indexName, query);

        return DeleteByQueryResult.builder()
            .timeTaken(System.currentTimeMillis() - startTime)
            .success(false)
            .failureReason("Task not completed within timeout")
            .remainingDocuments(remainingDocs)
            .retryAttempts(0)
            .taskId(taskId)
            .build();
      }

      // Task completed - count remaining documents to determine success
      long remainingDocs = countDocuments(indexName, query);

      // We can't get exact delete count from task API, but we can infer success
      // from whether documents remain
      return DeleteByQueryResult.builder()
          .timeTaken(System.currentTimeMillis() - startTime)
          .success(true) // Task completed successfully
          .failureReason(null)
          .remainingDocuments(remainingDocs)
          .retryAttempts(0)
          .taskId(taskId)
          .build();

    } catch (Exception e) {
      log.error("Failed to monitor delete by query task: {}", taskId, e);

      // Try to get remaining count even on error
      long remainingDocs = -1;
      try {
        remainingDocs = countDocuments(indexName, query);
      } catch (Exception countError) {
        log.error("Failed to count remaining documents", countError);
      }

      return DeleteByQueryResult.builder()
          .timeTaken(System.currentTimeMillis() - startTime)
          .success(false)
          .failureReason("Monitoring failed: " + e.getMessage())
          .remainingDocuments(remainingDocs)
          .retryAttempts(0)
          .taskId(taskId)
          .build();
    }
  }

  private static String toIndexName(
      @Nonnull OperationContext opContext, @Nonnull String entityName) {
    return opContext
        .getSearchContext()
        .getIndexConvention()
        .getIndexName(opContext.getEntityRegistry().getEntitySpec(entityName));
  }

  private DeleteByQueryRequest buildDeleteByQueryRequest(
      @Nonnull String indexName,
      @Nonnull QueryBuilder query,
      @Nonnull BulkDeleteConfiguration config) {

    DeleteByQueryRequest request = new DeleteByQueryRequest(indexName);
    request.setQuery(query);
    request.setBatchSize(config.getBatchSize());

    // Handle slices configuration - can be "auto" or a number
    if (!AUTO_SLICES_VALUE.equalsIgnoreCase(config.getSlices())) {
      try {
        int sliceCount = Integer.parseInt(config.getSlices());
        request.setSlices(sliceCount);
      } catch (NumberFormatException e) {
        log.warn("Invalid slices value '{}', defaulting to 'auto'", config.getSlices());
        request.setSlices(AUTO_SLICES);
      }
    } else {
      request.setSlices(AUTO_SLICES);
    }

    request.setTimeout(TimeValue.timeValueMillis(config.getTimeoutDuration().toMillis()));

    // Set conflicts strategy to proceed (skip conflicting documents)
    request.setConflicts("proceed");

    return request;
  }

  private long countDocuments(@Nonnull String indexName, @Nonnull QueryBuilder query)
      throws IOException {

    CountRequest countRequest = new CountRequest(indexName);
    countRequest.query(query);
    CountResponse countResponse = searchClient.count(countRequest, RequestOptions.DEFAULT);
    return countResponse.getCount();
  }

  /** Parse task ID from the task string format "nodeId:taskId" */
  private TaskId parseTaskId(String taskString) {
    if (taskString == null || !taskString.contains(":")) {
      throw new IllegalArgumentException("Invalid task string format: " + taskString);
    }
    String[] parts = taskString.split(":", 2);
    return new TaskId(parts[0], Long.parseLong(parts[1]));
  }
}
