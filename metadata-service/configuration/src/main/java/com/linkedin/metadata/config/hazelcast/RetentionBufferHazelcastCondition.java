package com.linkedin.metadata.config.hazelcast;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * True only when the retention buffer feature is on AND the Hazelcast backend was selected via
 * {@code datahub.buffer.implementation}. Used to gate the Hazelcast-specific {@code MapConfig}
 * beans in {@code RetentionBufferFactory} — they're pointless (and potentially confusing) when the
 * Caffeine backend is selected instead.
 */
public class RetentionBufferHazelcastCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    var env = context.getEnvironment();
    return Boolean.parseBoolean(
            env.getProperty(HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED, "false"))
        && "hazelcast"
            .equalsIgnoreCase(HazelcastBootstrapProperties.resolveBufferImplementation(env));
  }
}
