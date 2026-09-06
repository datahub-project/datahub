package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class PostgresUsageEventsImplementationCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    // With no Elasticsearch cluster, pgAnalytics is the only place usage events can land, so it
    // takes over even if the configured implementation still names Elasticsearch. Leaving neither
    // condition matched would drop the dataHubUsageEventIndexer bean entirely.
    if (!UsageEventsImplementation.elasticsearchEnabled(context)) {
      return true;
    }
    return "postgres".equalsIgnoreCase(UsageEventsImplementation.configured(context));
  }
}
