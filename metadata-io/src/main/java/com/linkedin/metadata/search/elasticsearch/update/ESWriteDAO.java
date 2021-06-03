package com.linkedin.metadata.search.elasticsearch.update;

import com.linkedin.metadata.models.registry.EntityRegistry;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import javax.annotation.Nonnull;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;


public class ESWriteDAO {
  private final EntityRegistry entityRegistry;
  private final BulkProcessor bulkProcessor;
  private final IndexConvention indexConvention;

  public ESWriteDAO(EntityRegistry entityRegistry, RestHighLevelClient searchClient, IndexConvention indexConvention,
      int bulkRequestsLimit, int bulkFlushPeriod, int numRetries, long retryInterval) {
    this.entityRegistry = entityRegistry;
    this.indexConvention = indexConvention;
    this.bulkProcessor = BulkProcessor.builder(
        (request, bulkListener) -> searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
        BulkListener.getInstance())
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }

  /**
   * Updates or inserts the given search document.
   *
   * @param entityName name of the entity
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String entityName, @Nonnull String document, @Nonnull String docId) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    final IndexRequest indexRequest = new IndexRequest(indexName).id(docId).source(document, XContentType.JSON);
    final UpdateRequest updateRequest = new UpdateRequest(indexName, docId).doc(document, XContentType.JSON)
        .detectNoop(false)
        .upsert(indexRequest);
    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the document with the given document ID from the index.
   *
   * @param entityName name of the entity
   * @param docId the ID of the document to delete
   */
  public void deleteDocument(@Nonnull String entityName, @Nonnull String docId) {
    final String indexName = indexConvention.getIndexName(entityRegistry.getEntitySpec(entityName));
    bulkProcessor.add(new DeleteRequest(indexName).id(docId));
  }
}
