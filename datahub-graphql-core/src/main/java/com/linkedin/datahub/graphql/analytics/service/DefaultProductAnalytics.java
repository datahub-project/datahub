package com.linkedin.datahub.graphql.analytics.service;

import com.datahub.context.OperationFingerprint;
import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresUsageEventsAnalyticsQueries;
import com.linkedin.datahub.graphql.generated.EntityType;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;

@Slf4j
public final class DefaultProductAnalytics implements ProductAnalytics {

  private static final String SERVICE_ACCOUNT_SUB_TYPE = "SERVICE_ACCOUNT";

  @Nullable private final SearchClientShim<?> elasticClient;
  private final AnalyticsService analyticsService;

  public DefaultProductAnalytics(
      @Nullable SearchClientShim<?> elasticClient,
      @Nonnull IndexConvention indexConvention,
      @Nullable PostgresUsageEventsAnalyticsQueries postgresUsageAnalytics) {
    this.elasticClient = elasticClient;
    // Mirrors PlatformAnalyticsConfiguration#createAnalyticsService (graphql-core cannot reference
    // the factories module).
    this.analyticsService =
        postgresUsageAnalytics != null
            ? new PostgresAnalyticsService(indexConvention, postgresUsageAnalytics)
            : new DefaultAnalyticsService(elasticClient, indexConvention);
  }

  @Override
  @Nonnull
  public AnalyticsService analyticsService() {
    return analyticsService;
  }

  @Override
  public boolean isEntitySearchAvailable() {
    return elasticClient != null;
  }

  @Override
  public int countCorpUsersTotal() {
    if (elasticClient == null) {
      return 0;
    }
    try {
      String corpUserIndex = analyticsService.getEntityIndexName(EntityType.CORP_USER);
      SearchRequest searchRequest = new SearchRequest(corpUserIndex);
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(0);
      searchSourceBuilder.query(QueryBuilders.matchAllQuery());
      searchSourceBuilder.trackTotalHits(true);
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse =
          elasticClient.search(OperationFingerprint.EMPTY, searchRequest, RequestOptions.DEFAULT);
      return (int) searchResponse.getHits().getTotalHits().value;
    } catch (Exception e) {
      log.warn("Failed to count users for telemetry: {}", e.getMessage());
      return 0;
    }
  }

  @Override
  public int countCorpUserServiceAccounts() {
    if (elasticClient == null) {
      return 0;
    }
    try {
      String corpUserIndex = analyticsService.getEntityIndexName(EntityType.CORP_USER);
      SearchRequest searchRequest = new SearchRequest(corpUserIndex);
      SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
      searchSourceBuilder.size(0);
      searchSourceBuilder.query(QueryBuilders.termQuery("typeNames", SERVICE_ACCOUNT_SUB_TYPE));
      searchSourceBuilder.trackTotalHits(true);
      searchRequest.source(searchSourceBuilder);

      SearchResponse searchResponse =
          elasticClient.search(OperationFingerprint.EMPTY, searchRequest, RequestOptions.DEFAULT);
      return (int) searchResponse.getHits().getTotalHits().value;
    } catch (Exception e) {
      log.warn("Failed to count service accounts for telemetry: {}", e.getMessage());
      return 0;
    }
  }
}
