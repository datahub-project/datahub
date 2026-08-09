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
 *   <li>{@code db} / {@code none} — no pre-transaction gate here; {@code db} uses the DAO's
 *       advisory lock instead, {@code none} relies purely on CAS.
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
      if (!(ebean.isOptimisticLockingEnabled() && ebean.isScopedRetryEnabled())) {
        // The gate only activates on the scoped path; wired-but-bypassed otherwise. Surface it so
        // an
        // operator who set the backend but not the mode flags isn't silently unprotected.
        log.warn(
            "entityWriteLockBackend=hazelcast but the write gate only activates when "
                + "optimisticLockingEnabled AND scopedRetryEnabled are both true — it is wired but "
                + "currently bypassed (optimisticLockingEnabled={}, scopedRetryEnabled={}).",
            ebean.isOptimisticLockingEnabled(),
            ebean.isScopedRetryEnabled());
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

    // none | db -> no pre-transaction gate (the DB advisory lock, if any, lives in the DAO).
    if ("none".equals(backend) || "db".equals(backend)) {
      log.info("Entity write-lock backend: {} (no pre-transaction gate).", backend);
    } else {
      // Typo/misconfig: surface it. Degrades to lockless (CAS still guards) rather than failing
      // startup, but a WARN so it is not mistaken for an intentional "none".
      log.warn(
          "Unrecognized entityWriteLockBackend='{}' (expected none|db|hazelcast); running with no "
              + "pre-transaction gate (CAS still guards).",
          backend);
    }
    return new NoOpEntityWriteLock();
  }
}
