package com.linkedin.metadata.search.elasticsearch.update;

import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.client.indices.GetIndexResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;

@Slf4j
@RequiredArgsConstructor
public class ESWriteDAO {

  private final RestHighLevelClient searchClient;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;

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
            .retryOnConflict(numRetries);

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
            .retryOnConflict(numRetries)
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

  private static String toIndexName(
      @Nonnull OperationContext opContext, @Nonnull String entityName) {
    return opContext
        .getSearchContext()
        .getIndexConvention()
        .getIndexName(opContext.getEntityRegistry().getEntitySpec(entityName));
  }
}
