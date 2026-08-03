package com.linkedin.metadata.kafka.config;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Matches when {@code elasticsearch.enabled} is true (or unset). Used to skip ES-only beans like
 * {@code ElasticsearchConnectorFactory} on Postgres-only profiles, which is necessary because
 * {@code mae-consumer} (a library module) does not have spring-boot-autoconfigure on its compile
 * classpath and cannot use {@link
 * org.springframework.boot.autoconfigure.condition.ConditionalOnProperty} directly.
 */
public class ElasticsearchEnabledCondition implements Condition {
  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    Environment env = context.getEnvironment();
    String enabled = env.getProperty("elasticsearch.enabled");
    return enabled == null || "true".equalsIgnoreCase(enabled);
  }
}
