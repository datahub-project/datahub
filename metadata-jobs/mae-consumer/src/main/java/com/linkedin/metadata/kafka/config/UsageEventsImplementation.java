package com.linkedin.metadata.kafka.config;

import javax.annotation.Nullable;
import org.springframework.context.annotation.ConditionContext;

/**
 * Shared property lookups for the usage-event indexer conditions, so the Elasticsearch and Postgres
 * conditions stay mutually exclusive and exactly one {@code dataHubUsageEventIndexer} is
 * registered.
 */
final class UsageEventsImplementation {

  private UsageEventsImplementation() {}

  @Nullable
  static String configured(ConditionContext context) {
    String impl =
        context.getEnvironment().getProperty("platformAnalytics.usage-events.implementation");
    if (impl == null || impl.isBlank()) {
      impl = context.getEnvironment().getProperty("DATAHUB_USAGE_EVENTS_IMPLEMENTATION");
    }
    return impl == null ? null : impl.trim();
  }

  /** Absent property means Elasticsearch integration is on (master default). */
  static boolean elasticsearchEnabled(ConditionContext context) {
    return !Boolean.FALSE.equals(
        context.getEnvironment().getProperty("elasticsearch.enabled", Boolean.class));
  }
}
