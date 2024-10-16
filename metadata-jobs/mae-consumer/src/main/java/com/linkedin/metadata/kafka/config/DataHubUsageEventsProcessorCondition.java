package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class DataHubUsageEventsProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    return "true".equals(env.getProperty("MAE_CONSUMER_ENABLED"))
        && (env.getProperty("DATAHUB_ANALYTICS_ENABLED") == null
            || "true".equals(env.getProperty("DATAHUB_ANALYTICS_ENABLED")));
  }
}
