package com.linkedin.metadata.kafka.config;

import javax.annotation.Nonnull;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Condition for enabling the legacy MCE (MetadataChangeEvents) consumer.
 *
 * <p>Enable with: MCE_CONSUMER_ENABLED=true
 */
public class MetadataChangeEventsProcessorCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, @Nonnull AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    // MCE consumer should be enabled whenever MCE_CONSUMER_ENABLED=true,
    // regardless of batch mode (since there is no batch MCE consumer)
    return "true".equals(env.getProperty("MCE_CONSUMER_ENABLED"));
  }
}
