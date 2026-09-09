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
    // The Hazelcast entity write-gate needs the embedded node — but ONLY when it will actually
    // engage: optimistic-locking mode with the hazelcast backend. With OL off the gate is bypassed
    // (EntityWriteLockFactory logs it), so booting a cluster for it would waste resources and
    // expose
    // startup to Hazelcast join failures for an inactive feature. Trim to match the backend parsing
    // elsewhere (getNormalizedEntityWriteLockBackend); null-safe (mocked Environment).
    final String writeLockBackend =
        env.getProperty(HazelcastBootstrapProperties.ENTITY_WRITE_LOCK_BACKEND, "none");
    // Trim before parsing (Spring's relaxed binding trims at runtime, so " true " enables OL
    // there);
    // not trimming here would boot-skip Hazelcast while the gate is live, degrading it to no-op.
    final String optimisticLockingRaw =
        env.getProperty(HazelcastBootstrapProperties.OPTIMISTIC_LOCKING_ENABLED, "false");
    final boolean optimisticLockingEnabled =
        optimisticLockingRaw != null && Boolean.parseBoolean(optimisticLockingRaw.trim());
    // Only Ebean implements OL; on Cassandra the gate never engages, so don't boot HZ for it even
    // if
    // OPTIMISTIC_LOCKING_ENABLED is left true. entityService.impl defaults to ebean (missing →
    // ebean).
    final String entityServiceImpl =
        env.getProperty(HazelcastBootstrapProperties.ENTITY_SERVICE_IMPL, "ebean");
    final boolean isEbean =
        entityServiceImpl == null || "ebean".equalsIgnoreCase(entityServiceImpl.trim());
    if (isEbean
        && optimisticLockingEnabled
        && writeLockBackend != null
        && "hazelcast".equalsIgnoreCase(writeLockBackend.trim())) {
      return true;
    }
    if (Boolean.parseBoolean(
            env.getProperty(HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED, "false"))
        && Boolean.parseBoolean(
            env.getProperty(HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED, "false"))) {
      // Retention buffer is Hazelcast-backed only, so it requires the embedded node — but ONLY when
      // it will actually wire (RetentionBufferFactory needs BOTH flags). Gating on retentionBuffer
      // alone would boot an unused cluster (and risk startup failure if it can't join) while ingest
      // still runs legacy in-transaction retention with RetentionBuffer.NO_OP.
      return true;
    }
    // Endpoint rules OR the scoped chain need the shared Hazelcast store. Keying on endpoint alone
    // would leave a scoped-only deployment without a Hazelcast instance, and the engine throws at
    // startup when scoped is active but Hazelcast is null.
    return HazelcastBootstrapProperties.rateLimitNeedsHazelcast(env);
  }
}
