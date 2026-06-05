package com.linkedin.metadata.config.hazelcast;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Creates a shared {@link com.hazelcast.core.HazelcastInstance} when search cache or GMS endpoint
 * rate limiting requires cluster coordination.
 */
public class HazelcastInstanceBootstrapCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    var env = context.getEnvironment();
    if ("hazelcast"
        .equalsIgnoreCase(
            env.getProperty(
                HazelcastBootstrapProperties.SEARCH_CACHE_IMPLEMENTATION, "caffeine"))) {
      return true;
    }
    return Boolean.parseBoolean(
        env.getProperty(HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, "false"));
  }
}
