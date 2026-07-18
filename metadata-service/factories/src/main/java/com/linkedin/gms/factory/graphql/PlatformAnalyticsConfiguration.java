package com.linkedin.gms.factory.graphql;

import com.linkedin.datahub.graphql.analytics.service.AnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.DefaultAnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.PostgresAnalyticsService;
import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresUsageEventsAnalyticsQueries;
import com.linkedin.gms.factory.common.IndexConventionFactory;
import com.linkedin.metadata.utils.elasticsearch.IndexConvention;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;

@Configuration
public class PlatformAnalyticsConfiguration {

  public static final String GRAPHQL_ANALYTICS_SERVICE_BEAN = "graphqlAnalyticsService";

  /**
   * Analytics for GraphQL platform charts/highlights. Mirrors prior {@link GraphQLEngineFactory}
   * wiring: feature flag on, and at least one of search or Postgres usage analytics available.
   */
  @Bean(name = GRAPHQL_ANALYTICS_SERVICE_BEAN)
  @ConditionalOnProperty(name = "platformAnalytics.enabled", havingValue = "true")
  @Conditional(PlatformAnalyticsBackendAvailableCondition.class)
  @Nonnull
  public AnalyticsService graphqlAnalyticsService(
      ObjectProvider<SearchClientShim<?>> elasticClientProvider,
      ObjectProvider<PostgresUsageEventsAnalyticsQueries> postgresUsageEventsAnalytics,
      @Qualifier(IndexConventionFactory.INDEX_CONVENTION_BEAN) IndexConvention indexConvention) {
    return createAnalyticsService(
        elasticClientProvider.getIfAvailable(),
        indexConvention,
        postgresUsageEventsAnalytics.getIfAvailable());
  }

  /**
   * Postgres-backed analytics when usage events are stored in Postgres; otherwise charts are served
   * from the search cluster via {@link DefaultAnalyticsService}. Shared by the Spring bean and
   * factory tests.
   */
  @Nonnull
  public static AnalyticsService createAnalyticsService(
      @Nullable SearchClientShim<?> elasticClient,
      @Nonnull IndexConvention indexConvention,
      @Nullable PostgresUsageEventsAnalyticsQueries postgresUsageAnalytics) {
    if (postgresUsageAnalytics != null) {
      return new PostgresAnalyticsService(indexConvention, postgresUsageAnalytics);
    }
    return new DefaultAnalyticsService(elasticClient, indexConvention);
  }
}
