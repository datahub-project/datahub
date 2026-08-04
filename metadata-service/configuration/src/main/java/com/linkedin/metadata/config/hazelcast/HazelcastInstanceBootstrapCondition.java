package com.linkedin.metadata.config.hazelcast;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Creates a shared {@link com.hazelcast.core.HazelcastInstance} when any of these features need
 * cluster coordination: search Hazelcast cache, entity graph cache, GMS endpoint rate limiting, or
 * the post-commit retention buffer.
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
    if (Boolean.parseBoolean(
        env.getProperty(HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED, "false"))) {
      // Retention buffer is Hazelcast-backed only, so the flag alone requires the embedded node.
      return true;
    }
    // Endpoint rules OR the scoped chain need the shared Hazelcast store. Keying on endpoint alone
    // would leave a scoped-only deployment without a Hazelcast instance, and the engine throws at
    // startup when scoped is active but Hazelcast is null.
    return HazelcastBootstrapProperties.rateLimitNeedsHazelcast(env);
  }
}
