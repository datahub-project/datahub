package com.linkedin.metadata.config.hazelcast;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Creates a shared {@link com.hazelcast.core.HazelcastInstance} when any of these features need
 * cluster coordination: search Hazelcast cache, entity graph cache, or GMS endpoint rate limiting.
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
    if (Boolean.parseBoolean(
        env.getProperty(HazelcastBootstrapProperties.ENTITY_GRAPH_CACHE_ENABLED, "false"))) {
      return true;
    }
    return Boolean.parseBoolean(
        env.getProperty(HazelcastBootstrapProperties.RATE_LIMIT_ENDPOINT_ENABLED, "false"));
  }
}
