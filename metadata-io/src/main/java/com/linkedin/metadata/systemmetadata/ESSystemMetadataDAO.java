package com.linkedin.metadata.systemmetadata;

import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_ASPECT;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_URN;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.INDEX_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.delete.DeleteRequest;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

@Slf4j
@RequiredArgsConstructor
public class ESSystemMetadataDAO {
  private final RestHighLevelClient client;
  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;

  /**
   * Gets the status of a Task running in ElasticSearch
   *
   * @param taskId the task ID to get the status of
   */
  public Optional<GetTaskResponse> getTaskStatus(@Nonnull String nodeId, long taskId) {
    final GetTaskRequest taskRequest = new GetTaskRequest(nodeId, taskId);
    try {
      return client.tasks().get(taskRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("ERROR: Failed to get task status: ", e);
      e.printStackTrace();
    }
    return Optional.empty();
  }

  /**
   * Updates or inserts the given search document.
   *
   * @param document the document to update / insert
   * @param docId the ID of the document
   */
  public void upsertDocument(@Nonnull String docId, @Nonnull String document) {
    final UpdateRequest updateRequest =
        new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(numRetries);
    bulkProcessor.add(updateRequest);
  }

  public DeleteResponse deleteByDocId(@Nonnull final String docId) {
    DeleteRequest deleteRequest =
        new DeleteRequest(indexConvention.getIndexName(INDEX_NAME), docId);

    try {
      final DeleteResponse deleteResponse = client.delete(deleteRequest, RequestOptions.DEFAULT);
      return deleteResponse;
    } catch (IOException e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:");
      e.printStackTrace();
    }
    return null;
  }

  public BulkByScrollResponse deleteByUrn(@Nonnull final String urn) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    finalQuery.must(QueryBuilders.termQuery("urn", urn));

    final Optional<BulkByScrollResponse> deleteResponse =
        bulkProcessor.deleteByQuery(finalQuery, indexConvention.getIndexName(INDEX_NAME));

    return deleteResponse.orElse(null);
  }

  public BulkByScrollResponse deleteByUrnAspect(
      @Nonnull final String urn, @Nonnull final String aspect) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    finalQuery.filter(QueryBuilders.termQuery("urn", urn));
    finalQuery.filter(QueryBuilders.termQuery("aspect", aspect));

    final Optional<BulkByScrollResponse> deleteResponse =
        bulkProcessor.deleteByQuery(finalQuery, indexConvention.getIndexName(INDEX_NAME));

    return deleteResponse.orElse(null);
  }

  public SearchResponse findByParams(
      Map<String, String> searchParams, boolean includeSoftDeleted, int from, int size) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    for (String key : searchParams.keySet()) {
      finalQuery.filter(QueryBuilders.termQuery(key, searchParams.get(key)));
    }

    if (!includeSoftDeleted) {
      finalQuery.mustNot(QueryBuilders.termQuery("removed", "true"));
    }

    searchSourceBuilder.query(finalQuery);

    searchSourceBuilder.from(from);
    searchSourceBuilder.size(size);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      log.error("Error while searching by params.", e);
    }
    return null;
  }

  // TODO: Scroll impl for searches bound by 10k limit
  public SearchResponse findByParams(
      Map<String, String> searchParams,
      boolean includeSoftDeleted,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nonnull String keepAlive,
      int size) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();

    for (String key : searchParams.keySet()) {
      finalQuery.filter(QueryBuilders.termQuery(key, searchParams.get(key)));
    }

    if (!includeSoftDeleted) {
      finalQuery.mustNot(QueryBuilders.termQuery("removed", "true"));
    }

    searchSourceBuilder.query(finalQuery);

    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);
    searchSourceBuilder.size(size);

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      log.error("Error while searching by params.", e);
    }
    return null;
  }

  public SearchResponse scroll(
      BoolQueryBuilder queryBuilder,
      boolean includeSoftDeleted,
      @Nullable String scrollId,
      @Nullable String pitId,
      @Nullable String keepAlive,
      int size) {
    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    if (!includeSoftDeleted) {
      queryBuilder.mustNot(QueryBuilders.termQuery("removed", "true"));
    }

    Object[] sort = null;
    if (scrollId != null) {
      SearchAfterWrapper searchAfterWrapper = SearchAfterWrapper.fromScrollId(scrollId);
      sort = searchAfterWrapper.getSort();
    }

    searchSourceBuilder.query(queryBuilder);
    ESUtils.setSearchAfter(searchSourceBuilder, sort, pitId, keepAlive);
    searchSourceBuilder.size(size);
    searchSourceBuilder.sort(FIELD_URN).sort(FIELD_ASPECT);

    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      return client.search(searchRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Error while searching by params.", e);
    }
    return null;
  }

  public SearchResponse findByRegistry(
      String registryName, String registryVersion, boolean includeSoftDeleted, int from, int size) {
    Map<String, String> params = new HashMap<>();
    params.put("registryName", registryName);
    params.put("registryVersion", registryVersion);
    return findByParams(params, includeSoftDeleted, from, size);
  }

  public SearchResponse findByRunId(String runId, boolean includeSoftDeleted, int from, int size) {
    return findByParams(Collections.singletonMap("runId", runId), includeSoftDeleted, from, size);
  }

  public SearchResponse findRuns(Integer pageOffset, Integer pageSize) {

    SearchRequest searchRequest = new SearchRequest();

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

    searchSourceBuilder.size(0);

    FieldSortBuilder fieldSortBuilder = new FieldSortBuilder("maxTimestamp");
    fieldSortBuilder.order(SortOrder.DESC);

    BucketSortPipelineAggregationBuilder bucketSort =
        PipelineAggregatorBuilders.bucketSort("mostRecent", ImmutableList.of(fieldSortBuilder));
    // TODO: Evaluate this usage, does this also hit pagination limits?
    bucketSort.size(pageSize);
    bucketSort.from(pageOffset);

    TermsAggregationBuilder aggregation =
        AggregationBuilders.terms("runId")
            .field("runId")
            .subAggregation(AggregationBuilders.max("maxTimestamp").field("lastUpdated"))
            .subAggregation(bucketSort)
            .subAggregation(
                AggregationBuilders.filter("removed", QueryBuilders.termQuery("removed", "true")));

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
