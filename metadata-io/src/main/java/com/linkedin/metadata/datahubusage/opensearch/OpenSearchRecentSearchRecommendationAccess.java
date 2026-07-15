package com.linkedin.metadata.datahubusage.opensearch;

import com.datahub.util.exception.ESQueryException;
import com.linkedin.common.urn.Urn;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.datahubusage.RecentSearchRecommendationAccess;
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
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.builder.SearchSourceBuilder;

/** Recent search-bar queries from the legacy OpenSearch usage index (analytics pipeline). */
@Slf4j
@RequiredArgsConstructor
public class OpenSearchRecentSearchRecommendationAccess
    implements RecentSearchRecommendationAccess {

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";
  private static final String ENTITY_AGG_NAME = "entity";
  private static final int AGG_BUCKET_SIZE = 10;

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
  public List<String> recentSearchQueries(@Nonnull OperationContext opContext) {
    SearchRequest searchRequest =
        buildSearchRequest(opContext.getSessionActorContext().getActorUrn());

    try {
      SearchResponse searchResponse =
          searchClient.search(opContext, searchRequest, RequestOptions.DEFAULT);
      ParsedTerms parsedTerms = searchResponse.getAggregations().get(ENTITY_AGG_NAME);
      return parsedTerms.getBuckets().stream()
          .map(Terms.Bucket::getKeyAsString)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Search query to get most recently searched queries failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private SearchRequest buildSearchRequest(@Nonnull Urn userUrn) {
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.ACTOR_URN + ".keyword", userUrn.toString()));
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
            .size(AGG_BUCKET_SIZE)
            .order(BucketOrder.aggregation(lastSearched, false))
            .subAggregation(
                AggregationBuilders.max(lastSearched).field(DataHubUsageEventConstants.TIMESTAMP));
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(indexConvention.getIndexName(DATAHUB_USAGE_INDEX));
    return request;
  }
}
