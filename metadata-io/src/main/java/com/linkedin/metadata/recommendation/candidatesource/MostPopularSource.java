package com.linkedin.metadata.recommendation.candidatesource;

import com.codahale.metrics.Timer;
import com.datahub.util.exception.ESQueryException;
import com.google.common.collect.ImmutableSet;
import com.linkedin.common.urn.Urn;
import com.linkedin.common.urn.UrnUtils;
import com.linkedin.metadata.Constants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventConstants;
import com.linkedin.metadata.datahubusage.DataHubUsageEventType;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityUtils;
import com.linkedin.metadata.recommendation.EntityProfileParams;
import com.linkedin.metadata.recommendation.RecommendationContent;
import com.linkedin.metadata.recommendation.RecommendationParams;
import com.linkedin.metadata.recommendation.RecommendationRenderType;
import com.linkedin.metadata.recommendation.RecommendationRequestContext;
import com.linkedin.metadata.recommendation.ScenarioType;
import com.linkedin.metadata.search.utils.ESUtils;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.opentelemetry.extension.annotations.WithSpan;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.ParsedTerms;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
@RequiredArgsConstructor
public class MostPopularSource implements RecommendationSource {
  /** Entity Types that should be in scope for this type of recommendation. */
  private static final Set<String> SUPPORTED_ENTITY_TYPES =
      ImmutableSet.of(
          Constants.DATASET_ENTITY_NAME,
          Constants.DATA_FLOW_ENTITY_NAME,
          Constants.DATA_JOB_ENTITY_NAME,
          Constants.CONTAINER_ENTITY_NAME,
          Constants.DASHBOARD_ENTITY_NAME,
          Constants.CHART_ENTITY_NAME,
          Constants.ML_MODEL_ENTITY_NAME,
          Constants.ML_FEATURE_ENTITY_NAME,
          Constants.ML_MODEL_GROUP_ENTITY_NAME,
          Constants.ML_FEATURE_TABLE_ENTITY_NAME);

  private final RestHighLevelClient _searchClient;
  private final IndexConvention _indexConvention;
  private final EntityService _entityService;

  private static final String DATAHUB_USAGE_INDEX = "datahub_usage_event";
  private static final String ENTITY_AGG_NAME = "entity";
  private static final int MAX_CONTENT = 5;

  @Override
  public String getTitle() {
    return "Most Popular";
  }

  @Override
  public String getModuleId() {
    return "HighUsageEntities";
  }

  @Override
  public RecommendationRenderType getRenderType() {
    return RecommendationRenderType.ENTITY_NAME_LIST;
  }

  @Override
  public boolean isEligible(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    boolean analyticsEnabled = false;
    try {
      analyticsEnabled =
          _searchClient
              .indices()
              .exists(
                  new GetIndexRequest(_indexConvention.getIndexName(DATAHUB_USAGE_INDEX)),
                  RequestOptions.DEFAULT);
    } catch (IOException e) {
      log.error("Failed to determine whether DataHub usage index exists");
    }
    return requestContext.getScenario() == ScenarioType.HOME && analyticsEnabled;
  }

  @Override
  @WithSpan
  public List<RecommendationContent> getRecommendations(
      @Nonnull Urn userUrn, @Nonnull RecommendationRequestContext requestContext) {
    SearchRequest searchRequest = buildSearchRequest(userUrn);
    try (Timer.Context ignored = MetricUtils.timer(this.getClass(), "getMostPopular").time()) {
      final SearchResponse searchResponse =
          _searchClient.search(searchRequest, RequestOptions.DEFAULT);
      // extract results
      ParsedTerms parsedTerms = searchResponse.getAggregations().get(ENTITY_AGG_NAME);
      return parsedTerms.getBuckets().stream()
          .map(bucket -> buildContent(bucket.getKeyAsString()))
          .filter(Optional::isPresent)
          .map(Optional::get)
          .limit(MAX_CONTENT)
          .collect(Collectors.toList());
    } catch (Exception e) {
      log.error("Search query to get most popular entities failed", e);
      throw new ESQueryException("Search query failed:", e);
    }
  }

  private SearchRequest buildSearchRequest(@Nonnull Urn userUrn) {
    // TODO: Proactively filter for entity types in the supported set.
    SearchRequest request = new SearchRequest();
    SearchSourceBuilder source = new SearchSourceBuilder();
    BoolQueryBuilder query = QueryBuilders.boolQuery();
    // Filter for all entity view events
    query.must(
        QueryBuilders.termQuery(
            DataHubUsageEventConstants.TYPE, DataHubUsageEventType.ENTITY_VIEW_EVENT.getType()));
    source.query(query);

    // Find the entities with the most views
    AggregationBuilder aggregation =
        AggregationBuilders.terms(ENTITY_AGG_NAME)
            .field(ESUtils.toKeywordField(DataHubUsageEventConstants.ENTITY_URN, false))
            .size(MAX_CONTENT * 2);
    source.aggregation(aggregation);
    source.size(0);

    request.source(source);
    request.indices(_indexConvention.getIndexName(DATAHUB_USAGE_INDEX));
    return request;
  }

  private Optional<RecommendationContent> buildContent(@Nonnull String entityUrn) {
    Urn entity = UrnUtils.getUrn(entityUrn);
    if (EntityUtils.checkIfRemoved(_entityService, entity)
        || !RecommendationUtils.isSupportedEntityType(entity, SUPPORTED_ENTITY_TYPES)) {
      return Optional.empty();
    }

    return Optional.of(
        new RecommendationContent()
            .setEntity(entity)
            .setValue(entityUrn)
            .setParams(
                new RecommendationParams()
                    .setEntityProfileParams(new EntityProfileParams().setUrn(entity))));
  }
}
