package com.linkedin.metadata.config.hazelcast;

/** Shared Spring environment property keys for Hazelcast bootstrap conditions. */
public final class HazelcastBootstrapProperties {

  public static final String SEARCH_CACHE_IMPLEMENTATION = "searchService.cacheImplementation";
  public static final String RATE_LIMIT_ENDPOINT_ENABLED =
      "datahub.gms.rateLimits.endpoint.enabled";

  private HazelcastBootstrapProperties() {}
}
