package com.linkedin.gms.factory.recommendation.candidatesource;

import com.linkedin.gms.factory.analytics.PgAnalyticsEbeanConfigFactory;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.analytics.postgres.PgAnalyticsStoreRegistry;
import com.linkedin.metadata.recommendation.candidatesource.ElasticsearchUsageEventRecommendationBackend;
import com.linkedin.metadata.recommendation.candidatesource.NoOpUsageEventRecommendationBackend;
import com.linkedin.metadata.recommendation.candidatesource.UsageEventRecommendationBackend;
import com.linkedin.metadata.recommendation.candidatesource.postgres.PostgresUsageEventRecommendationBackend;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * One usage-event backend for all recommendation modules. Elasticsearch vs pgAnalytics is selected
 * here so {@code RecentlyViewedSource} and peers stay store-agnostic.
 */
@Configuration
@Import({PgAnalyticsEbeanConfigFactory.class})
public class UsageEventRecommendationBackendFactory {

  @Bean(name = "usageEventRecommendationBackend")
  @Nonnull
  protected UsageEventRecommendationBackend usageEventRecommendationBackend(
      @Autowired(required = false) @Qualifier("searchClientShim") SearchClientShim<?> searchClient,
      @Autowired(required = false) @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN)
          IndexConvention indexConvention,
      @Autowired(required = false) @Nullable PgAnalyticsStoreRegistry pgAnalyticsStoreRegistry) {
    if (searchClient != null && indexConvention != null) {
      return new ElasticsearchUsageEventRecommendationBackend(searchClient, indexConvention);
    }
    if (pgAnalyticsStoreRegistry != null) {
      return new PostgresUsageEventRecommendationBackend(pgAnalyticsStoreRegistry);
    }
    return new NoOpUsageEventRecommendationBackend();
  }
}
