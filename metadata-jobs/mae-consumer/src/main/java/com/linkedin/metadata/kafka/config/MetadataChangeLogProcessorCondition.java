package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

public class MetadataChangeLogProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    return "true".equals(env.getProperty("MAE_CONSUMER_ENABLED"))
        || "true".equals(env.getProperty("MCL_CONSUMER_ENABLED"));
  }
}
