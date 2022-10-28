package com.linkedin.metadata.recommendation.candidatesource;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.recommendation.SearchParams;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.terms.ParsedTerms;
import org.elasticsearch.search.builder.SearchSourceBuilder;


@Slf4j
@RequiredArgsConstructor
public class RecentlySearchedSource implements RecommendationSource {
  private final RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention;

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";
  private static final String ENTITY_AGG_NAME = "entity";
  private static final int MAX_CONTENT = 5;

  @Override
  public String getTitle() {
    return "Recent Searches";
  }

  @Override
  public String getModuleId() {
    return "RecentSearches";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.SEARCH_QUERY_LIST;
  }

  @Override
  public boolean isEligible(@Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    boolean analyticsEnabled = false;
    try {
      analyticsEnabled = _searchClient.indices()
          .exists(new GetIndexRequest(_indexConvention.getIndexName(DATAHUB_USAGE_INDEX)), RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to check whether DataHub usage index exists");
    }
    return requestContext.getScenario() == ScenarioType.SEARCH_BAR && analyticsEnabled;
  }

  @Override
  public List<RecommendationContent> getRecommendations(@Nonnull Urn userUrn,
      @Nonnull RecommendationRequestContext requestContext) {
    SearchRequest searchRequest = buildSearchRequest(userUrn);
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getRecentlySearched").time()) {
      final SearchResponse searchResponse = _searchClient.search(searchRequest, RequestOptions.DEFAULT);
      // extract results
      ParsedTerms parsedTerms = searchResponse.getAggregations().get(ENTITY_AGG_NAME);
      return parsedTerms.getBuckets()
          .stream()
          .map(bucket -> buildContent(bucket.getKeyAsString()))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .limit(MAX_CONTENT)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Search query to get most recently viewed entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private SearchRequest buildSearchRequest(@Nonnull Urn userUrn) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    // Filter for the entity view events of the user requesting recommendation
    query.must(QueryBuilders.termQuery(DataHubUsageEventConstants.ACTOR_URN + ".keyword", userUrn.toString()));
    query.must(QueryBuilders.termQuery(DataHubUsageEventConstants.TYPE,
        DataHubUsageEventType.SEARCH_RESULTS_VIEW_EVENT.getType()));
    query.must(QueryBuilders.rangeQuery("total").gt(0));
    query.must(QueryBuilders.existsQuery(DataHubUsageEventConstants.QUERY));
    source.query(query);

    // Find the entity with the largest last viewed timestamp
    String lastSearched = "last_searched";
    AggregationBuilder aggregation = AggregationBuilders.terms(ENTITY_AGG_NAME)
        .field(DataHubUsageEventConstants.QUERY + ".keyword")
        .size(MAX_CONTENT * 2) // Fetch more than max to account for post-filtering
        .order(BucketOrder.aggregation(lastSearched, false))
        .subAggregation(AggregationBuilders.max(lastSearched).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(_indexConvention.getIndexName(DATAHUB_USAGE_INDEX));
    return request;
  }

  private boolean isQueryInvalid(@Nonnull String query) {
    return query.trim().isEmpty() || query.equals("*");
  }

  private Optional<RecommendationContent> buildContent(@Nonnull String query) {
    if (isQueryInvalid(query)) {
      return Optional.empty();
    }
    return Optional.of(new RecommendationContent().setValue(query)
        .setParams(new RecommendationParams().setSearchParams(new SearchParams().setQuery(query))));
  }
}
