package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class ElasticsearchUsageEventsImplementationCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    // An unset implementation means Elasticsearch (master default), but only when a cluster exists
    // to write to; otherwise the Postgres condition takes the default.
    if (!UsageEventsImplementation.elasticsearchEnabled(context)) {
      return false;
    }
    String impl = UsageEventsImplementation.configured(context);
    return impl == null
        || impl.isBlank()
        || "elasticsearch".equalsIgnoreCase(impl)
        || "opensearch".equalsIgnoreCase(impl);
  }
}
