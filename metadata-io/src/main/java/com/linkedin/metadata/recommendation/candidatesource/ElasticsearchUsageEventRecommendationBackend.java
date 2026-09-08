package com.linkedin.metadata.recommendation.candidatesource;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
@RequiredArgsConstructor
public class ElasticsearchUsageEventRecommendationBackend
    implements UsageEventRecommendationBackend {

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";
  private static final String ENTITY_AGG_NAME = "entity";

  private final SearchClientShim<?> searchClient;
  private final IndexConvention indexConvention;

  @Override
  public boolean isAvailable(@Nonnull OperationContext opContext) {
    return UsageEventIndexChecker.usageIndexExists(opContext, searchClient, indexConvention);
  }

  @Override
  @Nonnull
  public List<String> recentEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull Urn actorUrn,
      @Nonnull String eventType,
      int limit) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.must(
        QueryBuilders.termQuery(
            ESUtils.toKeywordField(
                opContext,
                DataHubUsageEventConstants.ACTOR_URN,
                false,
                opContext.getAspectRetriever()),
            actorUrn.toString()));
    query.must(QueryBuilders.termQuery(DataHubUsageEventConstants.TYPE, eventType));
    source.query(query);
    String lastSeen = "last_seen";
    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(
                ESUtils.toKeywordField(
                    opContext,
                    DataHubUsageEventConstants.ENTITY_URN,
                    false,
                    opContext.getAspectRetriever()))
            .size(limit)
            .order(BucketOrder.aggregation(lastSeen, false))
            .subAggregation(
                AggregationBuilders.max(lastSeen).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);
    request.source(source);
    request.indices(indexConvention.getIndexName(opContext, DATAHUB_USAGE_INDEX));
    return executeAggKeys(opContext, request);
  }

  @Override
  @Nonnull
  public List<String> mostViewedEntityUrns(
      @Nonnull OperationContext opContext,
      @Nonnull String eventType,
      int limit,
      @Nullable List<String> actorPeers) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    if (actorPeers != null && !actorPeers.isEmpty()) {
      query.must(
          QueryBuilders.termsQuery(DataHubUsageEventConstants.ACTOR_URN + ".keyword", actorPeers));
    }
    query.must(QueryBuilders.termQuery(DataHubUsageEventConstants.TYPE, eventType));
    source.query(query);
    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(
                ESUtils.toKeywordField(
                    opContext,
                    DataHubUsageEventConstants.ENTITY_URN,
                    false,
                    opContext.getAspectRetriever()))
            .size(limit);
    source.aggregation(aggregation);
    source.size(0);
    request.source(source);
    request.indices(indexConvention.getIndexName(opContext, DATAHUB_USAGE_INDEX));
    return executeAggKeys(opContext, request);
  }

  @Override
  @Nonnull
  public List<String> recentSearchQueries(
      @Nonnull OperationContext opContext, @Nonnull Urn actorUrn, int limit) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.ACTOR_URN + ".keyword", actorUrn.toString()));
    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.TYPE,
            DataHubUsageEventType.SEARCH_RESULTS_VIEW_EVENT.getType()));
    query.must(QueryBuilders.rangeQuery("total").gt(0));
    query.must(QueryBuilders.existsQuery(DataHubUsageEventConstants.QUERY));
    source.query(query);
    String lastSearched = "last_searched";
    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(DataHubUsageEventConstants.QUERY + ".keyword")
            .size(limit)
            .order(BucketOrder.aggregation(lastSearched, false))
            .subAggregation(
                AggregationBuilders.max(lastSearched).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);
    request.source(source);
    request.indices(indexConvention.getIndexName(opContext, DATAHUB_USAGE_INDEX));
    return executeAggKeys(opContext, request);
  }

  @Nonnull
  private List<String> executeAggKeys(
      @Nonnull OperationContext opContext, @Nonnull SearchRequest request) {
    try {
      SearchResponse searchResponse =
          searchClient.search(opContext, request, RequestOptions.DEFAULT);
      ParsedTerms parsedTerms = searchResponse.getAggregations().get(ENTITY_AGG_NAME);
      if (parsedTerms == null) {
        return Collections.emptyList();
      }
      return parsedTerms.getBuckets().stream()
          .map(MultiBucketsAggregation.Bucket::getKeyAsString)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Elasticsearch usage-event recommendation query failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }
}
