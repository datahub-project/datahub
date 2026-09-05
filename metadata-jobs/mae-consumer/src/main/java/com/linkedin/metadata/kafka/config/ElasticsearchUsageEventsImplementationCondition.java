package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ElasticsearchUsageEventsImplementationCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    String impl =
        context.getEnvironment().getProperty("platformAnalytics.usage-events.implementation");
    if (impl == null || impl.isBlank()) {
      impl = context.getEnvironment().getProperty("DATAHUB_USAGE_EVENTS_IMPLEMENTATION");
    }
    return impl == null
        || impl.isBlank()
        || "elasticsearch".equalsIgnoreCase(impl)
        || "opensearch".equalsIgnoreCase(impl);
  }
}
