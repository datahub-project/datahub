package com.linkedin.gms.factory.entity;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.entity.coordinator.CoordinationLockProvider;
import com.linkedin.metadata.entity.coordinator.HazelcastLockProvider;
import com.linkedin.metadata.entity.coordinator.LocalLockProvider;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Selects the coordinated-ingest lock substrate as a Spring bean. A new provider is added by
 * declaring a new conditional {@link CoordinationLockProvider} bean here — the coordinator and
 * {@code EntityServiceFactory} never change (Open/Closed).
 *
 * <p>Selection is driven by {@code metadataChangeProposal.coordinatedIngest.lockProvider}, whose
 * default derives from {@code searchService.cacheImplementation} (so a caffeine-cache deployment
 * with no Hazelcast lands on the in-JVM provider automatically).
 *
 * <p>Only {@code hazelcast} is distributed. Any other value — {@code local}, {@code caffeine}, or
 * an unset property — falls through to the in-JVM {@link LocalLockProvider}. Correctness never
 * depends on the provider (the DB single-sorted commit is authoritative), so a missing Hazelcast
 * instance degrades to local rather than failing startup.
 *
 * <p>To add e.g. Redis: declare, above the local fallback, {@code @Bean @ConditionalOnProperty(name
 * = LOCK_PROVIDER_PROPERTY, havingValue = "redis")} returning a {@code RedisLockProvider}. It wins
 * over the {@code @ConditionalOnMissingBean} default.
 */
@Slf4j
@Configuration
public class CoordinationLockProviderFactory {

  static final String LOCK_PROVIDER_PROPERTY =
      "metadataChangeProposal.coordinatedIngest.lockProvider";

  @Bean
  @ConditionalOnProperty(name = LOCK_PROVIDER_PROPERTY, havingValue = "hazelcast")
  @Nonnull
  public CoordinationLockProvider hazelcastLockProvider(
      @Nullable final HazelcastInstance hazelcastInstance) {
    if (hazelcastInstance == null) {
      // Distributed lock requested but Hazelcast isn't deployed on this node. Degrade to in-JVM
      // (DB commit remains authoritative) rather than failing startup — coordination stays on,
      // just node-local. Cross-node contention falls back to DB-level serialization + retry.
      log.warn(
          "coordinatedIngest lockProvider=hazelcast but no HazelcastInstance is available; "
              + "using the in-JVM local provider (node-local coordination; DB commit authoritative). "
              + "Deploy Hazelcast for cross-node serialization.");
      return new LocalLockProvider();
    }
    log.info("Coordinated ingest lock provider: hazelcast (distributed).");
    return new HazelcastLockProvider(hazelcastInstance);
  }

  @Bean
  @ConditionalOnMissingBean(CoordinationLockProvider.class)
  @Nonnull
  public CoordinationLockProvider localLockProvider() {
    log.info("Coordinated ingest lock provider: local (in-JVM only; DB commit authoritative).");
    return new LocalLockProvider();
  }
}
