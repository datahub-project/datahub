package com.linkedin.metadata.config.hazelcast;

import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;

/**
 * Provisions Hazelcast resources (e.g. the bucket map config) used by GMS rate limiting. Fires when
 * the endpoint rules <b>or</b> the scoped chain is enabled — both use the shared Hazelcast store,
 * so keying on {@code endpoint.enabled} alone would leave a scoped-only deployment's tenant map
 * without idle/LRU eviction (unbounded growth).
 */
public class RateLimitEndpointEnabledCondition implements Condition {

  @Override
  public boolean matches(ConditionContext context, AnnotatedTypeMetadata metadata) {
    return HazelcastBootstrapProperties.rateLimitNeedsHazelcast(context.getEnvironment());
  }
}
