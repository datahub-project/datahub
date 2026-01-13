package com.linkedin.metadata.search.elasticsearch.update;

import static com.linkedin.metadata.Constants.READ_ONLY_LOG;
import static org.opensearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES;
import static org.opensearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES_VALUE;

import com.google.common.annotations.VisibleForTesting;
import com.linkedin.metadata.config.search.BulkDeleteConfiguration;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.search.elasticsearch.indexbuilder.IndexDeletionUtils;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import com.linkedin.metadata.utils.elasticsearch.responses.GetIndexResponse;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.client.tasks.TaskId;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.reindex.DeleteByQueryRequest;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

@Slf4j
public class ESWriteDAO {
  private final ElasticSearchConfiguration config;
  private final SearchClientShim<?> searchClient;
  @Getter private final ESBulkProcessor bulkProcessor;
  private boolean canWrite = true;

  public ESWriteDAO(
      ElasticSearchConfiguration config,
      SearchClientShim<?> searchClient,
      ESBulkProcessor bulkProcessor) {
    this.config = config;
    this.searchClient = searchClient;
    this.bulkProcessor = bulkProcessor;
  }

  public void setWritable(boolean writable) {
    canWrite = writable;
  }

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
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    final UpdateRequest updateRequest =
        new UpdateRequest(toIndexName(opContext, entityName), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(config.getBulkProcessor().getNumRetries());

    bulkProcessor.add(updateRequest);
  }

  /**
   * Updates or inserts the given search document in the specified index. This method works directly
   * with index names, useful for V3 multi-entity indices.
   *
   * @param indexName name of the index
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocumentByIndexName(
      @Nonnull String indexName, @Nonnull String document, @Nonnull String docId) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    final UpdateRequest updateRequest =
        new UpdateRequest(indexName, docId)
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
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    bulkProcessor.add(new DeleteRequest(toIndexName(opContext, entityName)).id(docId));
  }

  /**
   * Deletes the document with the given document ID from the specified index. This method works
   * directly with index names, useful for V3 multi-entity indices.
   *
   * @param indexName name of the index
   * @param docId the ID of the document to delete
   */
  public void deleteDocumentByIndexName(@Nonnull String indexName, @Nonnull String docId) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    bulkProcessor.add(new DeleteRequest(indexName).id(docId));
  }

  /**
   * Checks if the given index exists in OpenSearch.
   *
   * @param indexName name of the index to check
   * @return true if the index exists, false otherwise
   */
  public boolean indexExists(@Nonnull String indexName) {
    try {
      return searchClient.indexExists(new GetIndexRequest(indexName), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.warn("Error checking if index {} exists: {}", indexName, e.getMessage());
      return false;
    }
  }

  /**
   * Updates or inserts the given search document in the V3 index for the specified search group.
   * This method uses the index convention to properly construct the V3 index name.
   *
   * @param opContext the operation context
   * @param searchGroup the search group name
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocumentBySearchGroup(
      @Nonnull OperationContext opContext,
      @Nonnull String searchGroup,
      @Nonnull String document,
      @Nonnull String docId) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    final UpdateRequest updateRequest =
        new UpdateRequest(toIndexNameV3(opContext, searchGroup), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(config.getBulkProcessor().getNumRetries());

    // Use URN-aware routing for entity document consistency
    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the document with the given document ID from the V3 index for the specified search
   * group. This method uses the index convention to properly construct the V3 index name.
   *
   * @param opContext the operation context
   * @param searchGroup the search group name
   * @param docId the ID of the document to delete
   */
  public void deleteDocumentBySearchGroup(
      @Nonnull OperationContext opContext, @Nonnull String searchGroup, @Nonnull String docId) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    // Use URN-aware routing for entity document consistency
    bulkProcessor.add(new DeleteRequest(toIndexNameV3(opContext, searchGroup)).id(docId));
  }

  /** Applies a script to a particular document */
  public void applyScriptUpdate(
      @Nonnull OperationContext opContext,
      @Nonnull String entityName,
      @Nonnull String docId,
      @Nonnull String scriptSource,
      @Nonnull Map<String, Object> scriptParams,
      Map<String, Object> upsert) {
    applyScriptUpdateByIndexName(
        toIndexName(opContext, entityName), docId, scriptSource, scriptParams, upsert);
  }

  /**
   * Applies a script to a particular document in a specific index. This method works directly with
   * index names, useful for applying script updates to semantic indices.
   *
   * @param indexName the name of the index
   * @param docId the document ID
   * @param scriptSource the script source code
   * @param scriptParams the script parameters
   * @param upsert the document to upsert if it doesn't exist
   */
  public void applyScriptUpdateByIndexName(
      @Nonnull String indexName,
      @Nonnull String docId,
      @Nonnull String scriptSource,
      @Nonnull Map<String, Object> scriptParams,
      Map<String, Object> upsert) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return;
    }
    // Create a properly parameterized script
    Script script =
        new Script(
            ScriptType.INLINE, // Use INLINE for scripts provided in the request
            "painless", // Specify the script language as painless
            scriptSource, // The script source code
            scriptParams // The parameters map
            );
    UpdateRequest updateRequest =
        new UpdateRequest(indexName, docId)
            .detectNoop(false)
            .scriptedUpsert(true)
            .retryOnConflict(config.getBulkProcessor().getNumRetries())
            .script(script)
            .upsert(upsert);
    bulkProcessor.add(updateRequest);
  }

  /**
   * Delete the index
   *
   * @param opContext the operation context
   * @return set of index names that were deleted
   */
  @Nonnull
  public Set<String> clear(@Nonnull OperationContext opContext) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return Collections.emptySet();
    }
    List<String> patterns =
        opContext
            .getSearchContext()
            .getIndexConvention()
            .getEntityIndicesCleanupPatterns(config.getEntityIndex());
    List<String> allIndices = new ArrayList<>();
    for (String pattern : patterns) {
      allIndices.addAll(Arrays.asList(getIndices(pattern)));
    }

    // Track which indices (aliases or concrete) were deleted so the caller can recreate them
    Set<String> deletedIndexNames = new HashSet<>();

    // Instead of deleting all documents (inefficient), delete the indices themselves
    for (String indexName : allIndices) {
      try {
        String nameToTrack = IndexDeletionUtils.deleteIndex(searchClient, indexName);
        if (nameToTrack != null) {
          deletedIndexNames.add(nameToTrack);
        }
      } catch (IOException e) {
        log.error("Failed to delete index {} during clear operation", indexName, e);
        throw new RuntimeException("Failed to clear index: " + indexName, e);
      }
    }

    if (deletedIndexNames.isEmpty()) {
      log.info("No indices were deleted");
    } else {
      log.info(
          "Deleted {} indices. Caller should recreate them if needed.", deletedIndexNames.size());
    }

    return deletedIndexNames;
  }

  private String[] getIndices(String pattern) {
    try {
      GetIndexResponse response =
          searchClient.getIndex(new GetIndexRequest(pattern), RequestOptions.DEFAULT);
      return response.getIndices();
    } catch (IOException e) {
      // Only treat index_not_found_exception as "no indices"
      if (e.getMessage() != null && e.getMessage().contains("index_not_found_exception")) {
        log.debug("No indices found matching pattern {}", pattern);
        return new String[] {};
      }
      // For real errors (ES down, network issues, etc.), propagate the exception
      log.error("Failed to get indices using pattern {}", pattern, e);
      throw new RuntimeException(
          "Failed to communicate with Elasticsearch for pattern: " + pattern, e);
    } catch (Exception e) {
      // Handle OpenSearchStatusException and similar
      if (e.getMessage() != null && e.getMessage().contains("index_not_found_exception")) {
        log.debug("No indices found matching pattern {}", pattern);
        return new String[] {};
      }
      log.error("Failed to get indices using pattern {}", pattern, e);
      throw new RuntimeException(
          "Failed to communicate with Elasticsearch for pattern: " + pattern, e);
    }
  }

  /**
   * Performs an async delete by query operation (non-blocking) Returns immediately with a
   * CompletableFuture containing the task ID
   */
  @Nonnull
  public CompletableFuture<String> deleteByQueryAsync(
      @Nonnull String indexName,
      @Nonnull QueryBuilder query,
      @Nullable BulkDeleteConfiguration overrideConfig) {
    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return CompletableFuture.completedFuture(StringUtils.EMPTY);
    }

    final BulkDeleteConfiguration finalConfig =
        overrideConfig != null ? overrideConfig : config.getBulkDelete();

    return CompletableFuture.supplyAsync(
        () -> {
          try {
            DeleteByQueryRequest request = buildDeleteByQueryRequest(indexName, query, finalConfig);

            // Submit the task asynchronously
            String taskId = searchClient.submitDeleteByQueryTask(request, RequestOptions.DEFAULT);

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

    if (!canWrite) {
      log.warn(READ_ONLY_LOG);
      return DeleteByQueryResult.builder().build();
    }
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

        String taskSubmission =
            searchClient.submitDeleteByQueryTask(request, RequestOptions.DEFAULT);
        TaskId taskId = parseTaskId(taskSubmission);
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
          searchClient.getTask(getTaskRequest, RequestOptions.DEFAULT);

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

  private static String toIndexNameV3(
      @Nonnull OperationContext opContext, @Nonnull String searchGroup) {
    return opContext.getSearchContext().getIndexConvention().getEntityIndexNameV3(searchGroup);
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
