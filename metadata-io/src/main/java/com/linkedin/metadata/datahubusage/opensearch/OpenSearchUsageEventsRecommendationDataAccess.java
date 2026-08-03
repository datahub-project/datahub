package com.linkedin.metadata.datahubusage.opensearch;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.aspect.AspectRetriever;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationDataAccess;
import com.linkedin.metadata.datahubusage.UsageEventsRecommendationPeerActors;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import io.datahubproject.metadata.context.OperationContext;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Loads usage-derived recommendation URNs from the legacy OpenSearch usage index (analytics
 * pipeline).
 */
@Slf4j
@RequiredArgsConstructor
public class OpenSearchUsageEventsRecommendationDataAccess
    implements UsageEventsRecommendationDataAccess {

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";
  private static final String ENTITY_AGG_NAME = "entity";
  private static final int MAX_BUCKET_TERMS = 5;

  /** MostPopular samples more buckets than others for peer filtering fallout. */
  private static final int MOST_POPULAR_TERMS_BUCKET = MAX_BUCKET_TERMS * 2;

  private final SearchClientShim<?> searchClient;
  private final IndexConvention indexConvention;

  @Override
  public boolean isDataAvailable(@Nonnull OperationContext opContext) {
    try {
      return searchClient.indexExists(
          opContext,
          new GetIndexRequest(indexConvention.getIndexName(DATAHUB_USAGE_INDEX)),
          RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to determine whether DataHub usage index exists", e);
      return false;
    }
  }

  @Override
  @Nonnull
  public List<String> recentlyViewedEntityUrns(@Nonnull OperationContext opContext) {
    SearchRequest req =
        buildRecentlyViewedRequest(
            opContext,
            opContext.getSessionActorContext().getActorUrn(),
            indexConvention.getIndexName(DATAHUB_USAGE_INDEX),
            opContext.getAspectRetriever());

    try {
      return runTermsAggregation(opContext, searchClient, ENTITY_AGG_NAME, req);
    } catch (Exception e) {
      log.error("Search query to get most recently viewed entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Override
  @Nonnull
  public List<String> mostPopularEntityUrns(@Nonnull OperationContext opContext) {
    SearchRequest req = buildMostPopularRequest(opContext);
    try {
      return runTermsAggregation(opContext, searchClient, ENTITY_AGG_NAME, req);
    } catch (Exception e) {
      log.error("Search query to get most popular entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  @Override
  @Nonnull
  public List<String> recentlyEditedEntityUrns(@Nonnull OperationContext opContext) {
    SearchRequest req =
        buildRecentlyEditedRequest(
            opContext,
            opContext.getSessionActorContext().getActorUrn(),
            indexConvention.getIndexName(DATAHUB_USAGE_INDEX),
            opContext.getAspectRetriever());
    try {
      return runTermsAggregation(opContext, searchClient, ENTITY_AGG_NAME, req);
    } catch (Exception e) {
      log.error("Search query to get most recently edited entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private static SearchRequest buildRecentlyViewedRequest(
      @Nonnull OperationContext opContext,
      @Nonnull Urn userUrn,
      String resolvedIndexName,
      AspectRetriever aspectRetriever) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.must(
        QueryBuilders.termQuery(
            ESUtils.toKeywordField(
                opContext, DataHubUsageEventConstants.ACTOR_URN, false, aspectRetriever),
            userUrn.toString()));
    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.TYPE, DataHubUsageEventType.ENTITY_VIEW_EVENT.getType()));
    source.query(query);

    String lastViewed = "last_viewed";
    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(
                ESUtils.toKeywordField(
                    opContext, DataHubUsageEventConstants.ENTITY_URN, false, aspectRetriever))
            .size(MAX_BUCKET_TERMS)
            .order(BucketOrder.aggregation(lastViewed, false))
            .subAggregation(
                AggregationBuilders.max(lastViewed).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(resolvedIndexName);
    return request;
  }

  private SearchRequest buildMostPopularRequest(OperationContext opContext) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();

    UsageEventsRecommendationPeerActors.restrictPeersTermsQuery(opContext).ifPresent(query::must);

    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.TYPE, DataHubUsageEventType.ENTITY_VIEW_EVENT.getType()));
    source.query(query);

    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(
                ESUtils.toKeywordField(
                    opContext,
                    DataHubUsageEventConstants.ENTITY_URN,
                    false,
                    opContext.getAspectRetriever()))
            .size(MOST_POPULAR_TERMS_BUCKET);
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(indexConvention.getIndexName(DATAHUB_USAGE_INDEX));
    return request;
  }

  private static SearchRequest buildRecentlyEditedRequest(
      @Nonnull OperationContext opContext,
      @Nonnull Urn userUrn,
      String resolvedIndexName,
      AspectRetriever aspectRetriever) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.must(
        QueryBuilders.termQuery(
            ESUtils.toKeywordField(
                opContext, DataHubUsageEventConstants.ACTOR_URN, false, aspectRetriever),
            userUrn.toString()));
    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.TYPE, DataHubUsageEventType.ENTITY_ACTION_EVENT.getType()));
    source.query(query);

    String lastViewed = "last_viewed";
    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(
                ESUtils.toKeywordField(
                    opContext, DataHubUsageEventConstants.ENTITY_URN, false, aspectRetriever))
            .size(MAX_BUCKET_TERMS)
            .order(BucketOrder.aggregation(lastViewed, false))
            .subAggregation(
                AggregationBuilders.max(lastViewed).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(resolvedIndexName);
    return request;
  }

  private static List<String> runTermsAggregation(
      @Nonnull OperationContext opContext,
      SearchClientShim<?> client,
      String aggName,
      SearchRequest req)
      throws Exception {
    final SearchResponse searchResponse = client.search(opContext, req, RequestOptions.DEFAULT);
    ParsedTerms parsedTerms = searchResponse.getAggregations().get(aggName);
    return parsedTerms.getBuckets().stream()
        .map(MultiBucketsAggregation.Bucket::getKeyAsString)
        .collect(Collectors.toList());
  }
}
