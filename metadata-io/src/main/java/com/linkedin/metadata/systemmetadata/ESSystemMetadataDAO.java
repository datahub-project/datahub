package com.linkedin.metadata.systemmetadata;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.elasticsearch.update.BulkListener;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryRequest;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.PipelineAggregatorBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.*;


@Slf4j
public class ESSystemMetadataDAO {
  private final BulkProcessor bulkProcessor;
  private final IndexConvention indexConvention;
  private final RestHighLevelClient client;

  public ESSystemMetadataDAO(RestHighLevelClient searchClient, IndexConvention indexConvention, int bulkRequestsLimit, int bulkFlushPeriod, int numRetries,
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

  public DeleteResponse deleteByDocId(
      @Nonnull final String docId
  ) {
    DeleteRequest deleteRequest = new DeleteRequest(indexConvention.getIndexName(INDEX_NAME), docId);

    try {
      final DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
      return deleteResponse;
    } catch (IOException e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:");
      e.printStackTrace();
    }
    return null;
  }

  public BulkByScrollResponse deleteByUrn(
      @Nonnull final String urn
  ) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    finalQuery.must(QueryBuilders.termQuery("urn", urn));

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

  public SearchResponse findByParams(Map<String, String> searchParams) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    searchParams.entrySet().forEach(entry -> finalQuery.must(QueryBuilders.termQuery(entry.getKey(), entry.getValue())));
    searchSourceBuilder.query(finalQuery);

    // this is the max page size elastic will return
    searchSourceBuilder.size(10000);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public SearchResponse findByRegistry(String registryName, String registryVersion) {
    Map<String, String> params = new HashMap<>();
    params.put("registryName", registryName);
    params.put("registryVersion", registryVersion);
    return findByParams(params);
  }

  public SearchResponse findByRunId(String runId) {
    return findByParams(Collections.singletonMap("runId", runId));
  }

  public SearchResponse findRuns(Integer pageOffset, Integer pageSize) {

    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.size(0);

    FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("maxTimestamp");
    fieldSortBuilder.order(SortOrder.DESC);

    BucketSortPipelineAggregationBuilder bucketSort =
        PipelineAggregatorBuilders.bucketSort("mostRecent", ImmutableList.of(fieldSortBuilder));
    bucketSort.size(pageSize);
    bucketSort.from(pageOffset);

    TermsAggregationBuilder aggregation =
        AggregationBuilders.terms("runId")
            .field("runId")
            .subAggregation(AggregationBuilders.max("maxTimestamp").field("lastUpdated"))
            .subAggregation(bucketSort);


    searchSourceBuilder.aggregation(aggregation);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }
}
