package com.linkedin.gms.factory.graphql;

import com.linkedin.datahub.graphql.analytics.service.postgres.PostgresUsageEventsAnalyticsQueries;
import com.linkedin.metadata.utils.elasticsearch.SearchClientShim;
import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.ConfigurationCondition;

/**
 * Matches when either Elasticsearch/OpenSearch is configured or Postgres-backed usage analytics is
 * present — the same OR used when wiring {@link
 * com.linkedin.datahub.graphql.analytics.service.AnalyticsService} for platform analytics.
 */
class PlatformAnalyticsBackendAvailableCondition extends AnyNestedCondition {

  PlatformAnalyticsBackendAvailableCondition() {
    super(ConfigurationCondition.ConfigurationPhase.REGISTER_BEAN);
  }

  @ConditionalOnBean(SearchClientShim.class)
  static class OnSearchClient {}

  @ConditionalOnBean(PostgresUsageEventsAnalyticsQueries.class)
  static class OnPostgresUsageAnalytics {}
}
