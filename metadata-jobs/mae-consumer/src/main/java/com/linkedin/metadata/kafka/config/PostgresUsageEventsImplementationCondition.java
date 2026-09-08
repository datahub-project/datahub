package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class PostgresUsageEventsImplementationCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    if (!pgAnalyticsEnabled(context)) {
      return false;
    }
    // With no Elasticsearch cluster, pgAnalytics is the only place usage events can land.
    if (!UsageEventsImplementation.elasticsearchEnabled(context)) {
      return true;
    }
    return "postgres".equalsIgnoreCase(UsageEventsImplementation.configured(context));
  }

  private static boolean pgAnalyticsEnabled(ConditionContext context) {
    return Boolean.TRUE.equals(
        context.getEnvironment().getProperty("postgres.pgAnalytics.enabled", Boolean.class));
  }
}
