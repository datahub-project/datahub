package com.linkedin.metadata.systemmetadata;

import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_ASPECT;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_REMOVED;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.FIELD_URN;
import static com.linkedin.metadata.systemmetadata.ElasticSearchSystemMetadataService.INDEX_NAME;

import com.google.common.collect.ImmutableList;
import com.linkedin.metadata.config.ConfigUtils;
import com.linkedin.metadata.config.SystemMetadataServiceConfig;
import com.linkedin.metadata.search.elasticsearch.query.request.SearchAfterWrapper;
import com.linkedin.metadata.search.elasticsearch.update.ESBulkProcessor;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
import org.opensearch.client.tasks.GetTaskRequest;
import org.opensearch.client.tasks.GetTaskResponse;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.reindex.BulkByScrollResponse;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.PipelineAggregatorBuilders;
import org.opensearch.search.aggregations.bucket.filter.Filters;
import org.opensearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilter;
import org.opensearch.search.aggregations.bucket.filter.ParsedFilters;
import org.opensearch.search.aggregations.bucket.terms.ParsedStringTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.pipeline.BucketSortPipelineAggregationBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;

@Slf4j
@RequiredArgsConstructor
public class ESSystemMetadataDAO {
  static final String AGG_BY_KEY_ASPECT = "by_key_aspect";
  static final String AGG_BY_REMOVAL_STATUS = "by_removal_status";
  static final String AGG_ACTIVE = "active";
  static final String AGG_SOFT_DELETED = "softDeleted";
  static final String FILTER_ACTIVE = "active";
  static final String FILTER_SOFT_DELETED = "softDeleted";

  private final SearchClientShim<?> client;
  private final IndexConvention indexConvention;
  private final ESBulkProcessor bulkProcessor;
  private final int numRetries;
  private final SystemMetadataServiceConfig systemMetadataServiceConfig;

  /**
   * Gets the status of a Task running in ElasticSearch
   *
   * @param taskId the task ID to get the status of
   */
  public Optional<GetTaskResponse> getTaskStatus(
      @Nonnull OperationContext opContext, @Nonnull String nodeId, long taskId) {
    final GetTaskRequest taskRequest = new GetTaskRequest(nodeId, taskId);
    try {
      return client.getTask(taskRequest, RequestOptions.DEFAULT);
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
  public void upsertDocument(
      @Nonnull OperationContext opContext, @Nonnull String docId, @Nonnull String document) {
    final UpdateRequest updateRequest =
        new UpdateRequest(indexConvention.getIndexName(INDEX_NAME), docId)
            .detectNoop(false)
            .docAsUpsert(true)
            .doc(document, XContentType.JSON)
            .retryOnConflict(numRetries);
    // Route by docId (Base64(SHA-256(urn + "--" + aspect))) so concurrent writes for
    // the same (urn, aspect) — e.g. setDocStatus during soft-delete racing with a
    // normal aspect insert — serialize on one bulk thread and retryOnConflict works.
    bulkProcessor.add(opContext, docId, updateRequest);
  }

  public DeleteResponse deleteByDocId(
      @Nonnull OperationContext opContext, @Nonnull final String docId) {
    DeleteRequest deleteRequest =
        new DeleteRequest(indexConvention.getIndexName(INDEX_NAME), docId);

    try {
      final DeleteResponse deleteResponse =
          client.deleteDocument(opContext, deleteRequest, RequestOptions.DEFAULT);
      return deleteResponse;
    } catch (IOException e) {
      log.error("ERROR: Failed to delete by query. See stacktrace for a more detailed error:");
      e.printStackTrace();
    }
    return null;
  }

  public BulkByScrollResponse deleteByUrn(
      @Nonnull OperationContext opContext, @Nonnull final String urn) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    finalQuery.must(QueryBuilders.termQuery("urn", urn));

    final Optional<BulkByScrollResponse> deleteResponse =
        bulkProcessor.deleteByQuery(
            opContext, finalQuery, indexConvention.getIndexName(INDEX_NAME));

    return deleteResponse.orElse(null);
  }

  public BulkByScrollResponse deleteByUrnAspect(
      @Nonnull OperationContext opContext,
      @Nonnull final String urn,
      @Nonnull final String aspect) {
    BoolQueryBuilder finalQuery = QueryBuilders.boolQuery();
    finalQuery.filter(QueryBuilders.termQuery("urn", urn));
    finalQuery.filter(QueryBuilders.termQuery("aspect", aspect));

    final Optional<BulkByScrollResponse> deleteResponse =
        bulkProcessor.deleteByQuery(
            opContext, finalQuery, indexConvention.getIndexName(INDEX_NAME));

    return deleteResponse.orElse(null);
  }

  public SearchResponse findByParams(
      @Nonnull OperationContext opContext,
      Map<String, String> searchParams,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
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
    searchSourceBuilder.size(ConfigUtils.applyLimit(systemMetadataServiceConfig, size));

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final SearchResponse searchResponse =
          client.search(opContext, searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      log.error("Error while searching by params.", e);
    }
    return null;
  }

  // TODO: Scroll impl for searches bound by 10k limit
  public SearchResponse findByParams(
      @Nonnull OperationContext opContext,
      Map<String, String> searchParams,
      boolean includeSoftDeleted,
      @Nullable Object[] sort,
      @Nullable String pitId,
      @Nonnull String keepAlive,
      @Nullable Integer size) {
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
    searchSourceBuilder.size(ConfigUtils.applyLimit(systemMetadataServiceConfig, size));

    searchRequest.source(searchSourceBuilder);

    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      final SearchResponse searchResponse =
          client.search(opContext, searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      log.error("Error while searching by params.", e);
    }
    return null;
  }

  public SearchResponse scroll(
      @Nonnull OperationContext opContext,
      BoolQueryBuilder queryBuilder,
      boolean includeSoftDeleted,
      @Nullable String scrollId,
      @Nullable String pitId,
      @Nullable String keepAlive,
      @Nullable Integer size) {
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
    searchSourceBuilder.size(ConfigUtils.applyLimit(systemMetadataServiceConfig, size));
    searchSourceBuilder.sort(FIELD_URN).sort(FIELD_ASPECT);

    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      return client.search(opContext, searchRequest, RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Error while searching by params.", e);
    }
    return null;
  }

  public SearchResponse findByRegistry(
      @Nonnull OperationContext opContext,
      String registryName,
      String registryVersion,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
    Map<String, String> params = new HashMap<>();
    params.put("registryName", registryName);
    params.put("registryVersion", registryVersion);
    return findByParams(opContext, params, includeSoftDeleted, from, size);
  }

  public SearchResponse findByRunId(
      @Nonnull OperationContext opContext,
      String runId,
      boolean includeSoftDeleted,
      int from,
      @Nullable Integer size) {
    return findByParams(
        opContext, Collections.singletonMap("runId", runId), includeSoftDeleted, from, size);
  }

  public SearchResponse findRuns(
      @Nonnull OperationContext opContext, Integer pageOffset, Integer pageSize) {

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
      final SearchResponse searchResponse =
          client.search(opContext, searchRequest, RequestOptions.DEFAULT);
      return searchResponse;
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Nonnull
  public KeyAspectCount countByKeyAspect(
      @Nonnull OperationContext opContext, @Nonnull String keyAspectName) {
    BoolQueryBuilder query =
        QueryBuilders.boolQuery().filter(QueryBuilders.termQuery(FIELD_ASPECT, keyAspectName));

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(query);
    searchSourceBuilder.aggregation(buildRemovalStatusFiltersAggregation());

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      SearchResponse searchResponse =
          client.search(opContext, searchRequest, RequestOptions.DEFAULT);
      return parseRemovalStatusFiltersAggregation(searchResponse);
    } catch (IOException e) {
      log.error("Key aspect count query failed for aspect {}", keyAspectName, e);
      throw new RuntimeException("Key aspect count query failed", e);
    }
  }

  @Nonnull
  public Map<String, KeyAspectCount> countByKeyAspects(
      @Nonnull OperationContext opContext, @Nonnull List<String> keyAspectNames) {
    if (keyAspectNames.isEmpty()) {
      return Collections.emptyMap();
    }
    if (keyAspectNames.size() == 1) {
      return Map.of(keyAspectNames.get(0), countByKeyAspect(opContext, keyAspectNames.get(0)));
    }

    BoolQueryBuilder query =
        QueryBuilders.boolQuery().filter(QueryBuilders.termsQuery(FIELD_ASPECT, keyAspectNames));

    TermsAggregationBuilder termsAggregation =
        AggregationBuilders.terms(AGG_BY_KEY_ASPECT)
            .field(FIELD_ASPECT)
            .size(keyAspectNames.size())
            .subAggregation(buildActiveFilterAggregation())
            .subAggregation(buildSoftDeletedFilterAggregation());

    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.size(0);
    searchSourceBuilder.query(query);
    searchSourceBuilder.aggregation(termsAggregation);

    SearchRequest searchRequest = new SearchRequest();
    searchRequest.source(searchSourceBuilder);
    searchRequest.indices(indexConvention.getIndexName(INDEX_NAME));

    try {
      SearchResponse searchResponse =
          client.search(opContext, searchRequest, RequestOptions.DEFAULT);
      return parseKeyAspectTermsAggregation(searchResponse, keyAspectNames);
    } catch (IOException e) {
      log.error("Key aspect batch count query failed", e);
      throw new RuntimeException("Key aspect batch count query failed", e);
    }
  }

  private static FiltersAggregator.KeyedFilter[] removalStatusFilters() {
    return new FiltersAggregator.KeyedFilter[] {
      new FiltersAggregator.KeyedFilter(
          FILTER_ACTIVE,
          QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(FIELD_REMOVED, "true"))),
      new FiltersAggregator.KeyedFilter(
          FILTER_SOFT_DELETED, QueryBuilders.termQuery(FIELD_REMOVED, "true"))
    };
  }

  private static org.opensearch.search.aggregations.bucket.filter.FiltersAggregationBuilder
      buildRemovalStatusFiltersAggregation() {
    return AggregationBuilders.filters(AGG_BY_REMOVAL_STATUS, removalStatusFilters());
  }

  private static org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder
      buildActiveFilterAggregation() {
    return AggregationBuilders.filter(
        AGG_ACTIVE,
        QueryBuilders.boolQuery().mustNot(QueryBuilders.termQuery(FIELD_REMOVED, "true")));
  }

  private static org.opensearch.search.aggregations.bucket.filter.FilterAggregationBuilder
      buildSoftDeletedFilterAggregation() {
    return AggregationBuilders.filter(
        AGG_SOFT_DELETED, QueryBuilders.termQuery(FIELD_REMOVED, "true"));
  }

  @Nonnull
  private static KeyAspectCount parseRemovalStatusFiltersAggregation(
      @Nonnull SearchResponse searchResponse) {
    if (searchResponse.getAggregations() == null) {
      return KeyAspectCount.empty();
    }
    ParsedFilters filters = searchResponse.getAggregations().get(AGG_BY_REMOVAL_STATUS);
    if (filters == null) {
      return KeyAspectCount.empty();
    }
    long active = 0L;
    long softDeleted = 0L;
    for (Filters.Bucket bucket : filters.getBuckets()) {
      if (FILTER_ACTIVE.equals(bucket.getKeyAsString())) {
        active = bucket.getDocCount();
      } else if (FILTER_SOFT_DELETED.equals(bucket.getKeyAsString())) {
        softDeleted = bucket.getDocCount();
      }
    }
    return KeyAspectCount.builder().activeCount(active).softDeletedCount(softDeleted).build();
  }

  @Nonnull
  private static Map<String, KeyAspectCount> parseKeyAspectTermsAggregation(
      @Nonnull SearchResponse searchResponse, @Nonnull List<String> keyAspectNames) {
    Map<String, KeyAspectCount> results = new HashMap<>();
    for (String keyAspectName : keyAspectNames) {
      results.put(keyAspectName, KeyAspectCount.empty());
    }
    if (searchResponse.getAggregations() == null) {
      return results;
    }
    ParsedStringTerms terms = searchResponse.getAggregations().get(AGG_BY_KEY_ASPECT);
    if (terms == null) {
      return results;
    }
    for (Terms.Bucket bucket : terms.getBuckets()) {
      String aspectName = bucket.getKeyAsString();
      ParsedFilter activeAgg = bucket.getAggregations().get(AGG_ACTIVE);
      ParsedFilter softDeletedAgg = bucket.getAggregations().get(AGG_SOFT_DELETED);
      long active = activeAgg != null ? activeAgg.getDocCount() : 0L;
      long softDeleted = softDeletedAgg != null ? softDeletedAgg.getDocCount() : 0L;
      results.put(
          aspectName,
          KeyAspectCount.builder().activeCount(active).softDeletedCount(softDeleted).build());
    }
    return results;
  }
}
