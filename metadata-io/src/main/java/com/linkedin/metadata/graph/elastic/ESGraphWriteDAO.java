package com.linkedin.metadata.graph.elastic;

import com.linkedin.metadata.query.filter.Filter;
import com.linkedin.metadata.query.filter.RelationshipFilter;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;

import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;

import static com.linkedin.metadata.graph.elastic.ESGraphQueryDAO.*;
import static com.linkedin.metadata.graph.elastic.ElasticSearchGraphService.*;


@Slf4j
public class ESGraphWriteDAO {
  private final BulkProcessor bulkProcessor;
  private final IndexConvention indexConvention;
  private final RestHighLevelClient client;

  public ESGraphWriteDAO(RestHighLevelClient searchClient, IndexConvention indexConvention, int bulkRequestsLimit, int bulkFlushPeriod, int numRetries,
      long retryInterval) {
    this.client = searchClient;
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
  public void upsertDocument(@Nonnull String docId, @Nonnull String document) {
    final IndexRequest indexRequest = new IndexRequest(indexConvention.getIndexName(INDEX_NAME)).id(docId).source(document, XContentType.JSON);
    final UpdateRequest updateRequest = new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId).doc(document, XContentType.JSON)
        .detectNoop(false)
        .upsert(indexRequest);
    bulkProcessor.add(updateRequest);
  }

  public BulkByScrollResponse deleteByQuery(
      @Nullable final String sourceType,
      @Nonnull  final Filter sourceEntityFilter,
      @Nullable final String destinationType,
      @Nonnull final Filter destinationEntityFilter,
      @Nonnull final List<String> relationshipTypes,
      @Nonnull final RelationshipFilter relationshipFilter) {
    BoolQueryBuilder finalQuery = buildQuery(
        sourceType,
        sourceEntityFilter,
        destinationType,
        destinationEntityFilter,
        relationshipTypes,
        relationshipFilter
    );

    DeleteByQueryRequest deleteByQueryRequest = new DeleteByQueryRequest();

    deleteByQueryRequest.setQuery(finalQuery);

    deleteByQueryRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final BulkByScrollResponse deleteResponse = client.deleteByQuery(deleteByQueryRequest, RequestOptions.DEFAULT);
      return deleteResponse;
    } catch (IOException e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:");
      e.printStackTrace();
    }
    return null;
  }
}
