package com.linkedin.metadata.graph.elastic;

import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
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

import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.*;


public class ESGraphWriteDAO {
  private final BulkProcessor bulkProcessor;
  private final IndexConvention indexConvention;

  public ESGraphWriteDAO(RestHighLevelClient searchClient, IndexConvention indexConvention, int bulkRequestsLimit, int bulkFlushPeriod, int numRetries,
      long retryInterval) {
    this.indexConvention = indexConvention;
    this.bulkProcessor = BulkProcessor.builder(
        (request, bulkListener) -> {
            searchClient.bulkAsync(request, RequestOptions.DEFAULT, bulkListener);
        },
        BulkListener.getInstance())
        .setBulkActions(bulkRequestsLimit)
        .setFlushInterval(TimeValue.timeValueSeconds(bulkFlushPeriod))
        .setBackoffPolicy(BackoffPolicy.constantBackoff(TimeValue.timeValueSeconds(retryInterval), numRetries))
        .build();
  }

  /**
   * Updates or inserts the given search document.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String document, @Nonnull String docId) {
    final IndexRequest indexRequest = new IndexRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId).source(document, XContentType.JSON);
    final UpdateRequest updateRequest = new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId).doc(document, XContentType.JSON)
        .detectNoop(false)
        .upsert(indexRequest);
    bulkProcessor.add(updateRequest);
  }

  /**
   * Deletes the document with the given document ID from the index.
   *
   * @param docId the ID of the document to delete
   */
  public void deleteDocument(@Nonnull String docId) {
    bulkProcessor.add(new DeleteRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId));
  }
}
