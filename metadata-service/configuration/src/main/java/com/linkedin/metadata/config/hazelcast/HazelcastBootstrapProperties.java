package com.linkedin.metadata.config.hazelcast;

import org.springframework.core.env.PropertyResolver;

/** Shared Spring environment property keys for Hazelcast bootstrap conditions. */
public final class HazelcastBootstrapProperties {

  public static final String SEARCH_CACHE_IMPLEMENTATION = "searchService.cacheImplementation";
  public static final String RATE_LIMIT_ENDPOINT_ENABLED =
      "datahub.gms.rateLimits.endpoint.enabled";
  public static final String RATE_LIMIT_SCOPED_ENABLED = "datahub.gms.rateLimits.scoped.enabled";
  public static final String ENTITY_GRAPH_CACHE_ENABLED = "datahub.gms.entityGraphCache.enabled";

  /** Backend selection for {@code CoalesceBuffer<K,V>}; see {@code BufferImplementation}. */
  public static final String BUFFER_IMPLEMENTATION = "datahub.buffer.implementation";

  /**
   * Canonical gate: {@code featureFlags.retentionBufferEnabled} / {@code RETENTION_BUFFER_ENABLED}.
   */
  public static final String RETENTION_BUFFER_ENABLED = "featureFlags.retentionBufferEnabled";

  private HazelcastBootstrapProperties() {}

  /**
   * Resolves the effective {@code datahub.buffer.implementation}, mirroring the YAML default chain
   * ({@code DATAHUB_BUFFER_IMPLEMENTATION -> searchService.cacheImplementation -> "caffeine"})
   * directly in Java as a safety net for early-lifecycle {@link
   * org.springframework.context.annotation.Condition}s that run before the environment's nested
   * placeholder resolution is guaranteed to have settled.
   */
  public static String resolveBufferImplementation(PropertyResolver environment) {
    String configured = environment.getProperty(BUFFER_IMPLEMENTATION);
    if (configured != null && !configured.isBlank()) {
      return configured;
    }
    return environment.getProperty(SEARCH_CACHE_IMPLEMENTATION, "caffeine");
  }

  /**
   * True when GMS rate limiting needs the shared Hazelcast store. Both the endpoint rules and the
   * scoped chain (per-actor/class/global buckets) live in Hazelcast, so either being enabled
   * requires it — provisioning must not key on {@code endpoint.enabled} alone, or a scoped-only
   * deployment gets no Hazelcast instance and GMS fails to start.
   */
  public static boolean rateLimitNeedsHazelcast(PropertyResolver environment) {
    return Boolean.parseBoolean(environment.getProperty(RATE_LIMIT_ENDPOINT_ENABLED, "false"))
        || Boolean.parseBoolean(environment.getProperty(RATE_LIMIT_SCOPED_ENABLED, "false"));
  }
}
