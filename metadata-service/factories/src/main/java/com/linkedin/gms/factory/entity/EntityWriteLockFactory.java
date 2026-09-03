package com.linkedin.gms.factory.entity;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.EbeanConfiguration;
import com.linkedin.metadata.entity.lock.EntityWriteLock;
import com.linkedin.metadata.entity.lock.HazelcastEntityWriteLock;
import com.linkedin.metadata.entity.lock.NoOpEntityWriteLock;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Selects the {@link EntityWriteLock} implementation from {@code ebean.entityWriteLockBackend}:
 *
 * <ul>
 *   <li>{@code hazelcast} — distributed per-URN gate that keeps lock waits off the DB connection
 *       pool (preferred when connections are the bottleneck). Falls back to no-op with a warning if
 *       no Hazelcast instance is available (never fails startup).
 *   <li>{@code none} — no pre-transaction gate; relies purely on CAS.
 * </ul>
 */
@Slf4j
@Configuration
public class EntityWriteLockFactory {

  static final String LOCK_MAP_NAME = "datahub-entity-write-lock";

  @Bean
  @Nonnull
  public EntityWriteLock entityWriteLock(
      final ConfigurationProvider configurationProvider,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          final HazelcastInstance hazelcastInstance) {
    final EbeanConfiguration ebean = configurationProvider.getEbean();
    final String backend = ebean.getNormalizedEntityWriteLockBackend();

    if ("hazelcast".equals(backend)) {
      if (hazelcastInstance == null) {
        log.warn(
            "entityWriteLockBackend=hazelcast but no HazelcastInstance is available; entity writes "
                + "run lockless (optimistic-locking CAS still guards correctness).");
        return new NoOpEntityWriteLock();
      }
      if (!ebean.isOptimisticLockingEnabled()) {
        // The gate only activates in optimistic-locking mode (it targets CAS thrash, not the FOR
        // UPDATE path); wired-but-bypassed otherwise. Surface it so an operator who set the backend
        // but not optimisticLockingEnabled isn't silently unprotected.
        log.warn(
            "entityWriteLockBackend=hazelcast but the write gate only activates when "
                + "optimisticLockingEnabled=true — it is wired but currently bypassed.");
      }
      if (ebean.isEntityWriteAdvisoryLockEnabled()) {
        // Both the Hazelcast gate and the Postgres advisory lock are enabled. When the gate is
        // engaged the advisory lock is skipped (avoids double-locking). Note the gate is
        // best-effort:
        // on acquire timeout or a Hazelcast outage it degrades to lockless CAS, NOT to the advisory
        // lock — so the advisory does NOT serve as a fallback here. Enable only one unless the
        // deadlock-ordering advisory is wanted for the non-OL FOR UPDATE path.
        log.warn(
            "Both entityWriteLockBackend=hazelcast and entityWriteAdvisoryLockEnabled=true are set. "
                + "In optimistic-locking mode the Postgres advisory lock is skipped while the gate is "
                + "engaged, and the gate degrades to lockless CAS (not the advisory) on "
                + "timeout/outage — the advisory is not a gate fallback.");
      }
      final long leaseSeconds = ebean.getEntityWriteLockLeaseSeconds();
      log.info(
          "Entity write-lock backend: hazelcast (map={}, acquireTimeout={}s, lease={}s).",
          LOCK_MAP_NAME,
          ebean.getEntityWriteLockAcquireTimeoutSeconds(),
          leaseSeconds);
      return new HazelcastEntityWriteLock(
          hazelcastInstance,
          LOCK_MAP_NAME,
          ebean.getEntityWriteLockAcquireTimeoutSeconds(),
          leaseSeconds);
    }

    if ("none".equals(backend)) {
      log.info("Entity write-lock backend: none (no pre-transaction gate).");
    } else {
      // Typo/misconfig: surface it. Degrades to lockless (CAS still guards) rather than failing
      // startup, but a WARN so it is not mistaken for an intentional "none".
      log.warn(
          "Unrecognized entityWriteLockBackend='{}' (expected none|hazelcast); running with no "
              + "pre-transaction gate (CAS still guards).",
          backend);
    }
    return new NoOpEntityWriteLock();
  }
}
