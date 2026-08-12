package com.linkedin.metadata.config.hazelcast;

import org.springframework.core.env.PropertyResolver;

/** Shared Spring environment property keys for Hazelcast bootstrap conditions. */
public final class HazelcastBootstrapProperties {

  public static final String SEARCH_CACHE_IMPLEMENTATION = "searchService.cacheImplementation";
  public static final String RATE_LIMIT_ENDPOINT_ENABLED =
      "datahub.gms.rateLimits.endpoint.enabled";
  public static final String RATE_LIMIT_SCOPED_ENABLED = "datahub.gms.rateLimits.scoped.enabled";
  public static final String ENTITY_GRAPH_CACHE_ENABLED = "datahub.gms.entityGraphCache.enabled";

  /**
   * Entity write-lock backend ({@code ebean.entityWriteLockBackend} / {@code
   * ENTITY_WRITE_LOCK_BACKEND}), one of {@code none} | {@code hazelcast}. The {@code hazelcast}
   * gate needs the embedded node, but only when it will actually engage: it activates when
   * optimistic locking is enabled ({@link #OPTIMISTIC_LOCKING_ENABLED}), independently of scoped
   * retry. With OL off the gate is bypassed, so the node is not booted for it. {@code none} never
   * needs it.
   */
  public static final String ENTITY_WRITE_LOCK_BACKEND = "ebean.entityWriteLockBackend";

  /**
   * Optimistic locking toggle ({@code ebean.optimisticLockingEnabled} / {@code
   * OPTIMISTIC_LOCKING_ENABLED}). The Hazelcast write gate only engages in OL mode, so the embedded
   * node is booted for the gate only when this is also true.
   */
  public static final String OPTIMISTIC_LOCKING_ENABLED = "ebean.optimisticLockingEnabled";

  /**
   * Entity service implementation ({@code entityService.impl}), {@code ebean} (default when
   * missing) or {@code cassandra}. Only Ebean implements optimistic locking, so the write gate can
   * engage only on Ebean — the node is not booted for the gate on Cassandra.
   */
  public static final String ENTITY_SERVICE_IMPL = "entityService.impl";

  /**
   * Canonical gate: {@code featureFlags.retentionBufferEnabled} / {@code RETENTION_BUFFER_ENABLED}.
   * The retention buffer's only backend is Hazelcast (cluster-wide shared map + drain lock), so
   * this flag alone decides whether the embedded node must boot for it.
   */
  public static final String RETENTION_BUFFER_ENABLED = "featureFlags.retentionBufferEnabled";

  /**
   * Master gate for post-commit retention; the buffer + drainer only wire when this is also true.
   */
  public static final String POST_COMMIT_RETENTION_ENABLED =
      "featureFlags.postCommitRetentionEnabled";

  private HazelcastBootstrapProperties() {}

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
