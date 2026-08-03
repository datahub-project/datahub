package com.linkedin.gms.factory.entity;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CoordinatedIngestConfiguration;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.coordinator.ConflictKeyResolver;
import com.linkedin.metadata.entity.coordinator.CoordinationLockProvider;
import com.linkedin.metadata.entity.coordinator.HazelcastLockProvider;
import com.linkedin.metadata.entity.coordinator.LocalLockProvider;
import com.linkedin.metadata.entity.coordinator.MutationCoordinator;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.event.EventProducer;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

@Slf4j
@Configuration
public class EntityServiceFactory {

  @Value("${EBEAN_MAX_TRANSACTION_RETRY:#{null}}")
  private Integer _ebeanMaxTransactionRetry;

  @Bean(name = "entityService")
  @DependsOn({"entityAspectDao", "kafkaEventProducer"})
  @Nonnull
  protected EntityService<ChangeItemImpl> createInstance(
      @Qualifier("kafkaEventProducer") final EventProducer eventProducer,
      @Qualifier("entityAspectDao") final AspectDao aspectDao,
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider,
      @Value("${featureFlags.showBrowseV2}") final boolean enableBrowsePathV2,
      @Value("${featureFlags.cdcModeChangeLog}") final boolean enableCDCModeChangeLog,
      final List<ThrottleSensor> throttleSensors,
      @javax.annotation.Nullable final com.linkedin.metadata.utils.metrics.MetricUtils metricUtils,
      @javax.annotation.Nullable final HazelcastInstance hazelcastInstance) {

    FeatureFlags featureFlags = configurationProvider.getFeatureFlags();

    EntityServiceImpl entityService =
        new EntityServiceImpl(
            aspectDao,
            eventProducer,
            featureFlags.isAlwaysEmitChangeLog(),
            featureFlags.isCdcModeChangeLog(),
            featureFlags.getPreProcessHooks(),
            _ebeanMaxTransactionRetry,
            enableBrowsePathV2,
            metricUtils);

    // Coordinated ingest (Plan -> Coordinate -> Commit). Wired only when the feature flag is on and
    // the tunables are present; when the flag is off nothing is constructed and the legacy path
    // runs.
    // The lock substrate is pluggable and env-selected (COORDINATED_INGEST_LOCK_PROVIDER); it is a
    // best-effort serializer only — the DB single-sorted commit stays authoritative regardless.
    final CoordinatedIngestConfiguration coordinatedIngestConfig =
        configurationProvider.getMetadataChangeProposal().getCoordinatedIngest();
    if (featureFlags.isCoordinatedIngestEnabled() && coordinatedIngestConfig != null) {
      final CoordinationLockProvider lockProvider =
          buildLockProvider(coordinatedIngestConfig, hazelcastInstance);
      MutationCoordinator mutationCoordinator =
          new MutationCoordinator(lockProvider, coordinatedIngestConfig, metricUtils);
      entityService.setCoordinatedIngest(
          mutationCoordinator,
          new ConflictKeyResolver(),
          true,
          coordinatedIngestConfig.getMaxMutationCount());
    }

    if (throttleSensors != null
        && !throttleSensors.isEmpty()
        && configurationProvider
            .getMetadataChangeProposal()
            .getThrottle()
            .getComponents()
            .getApiRequests()
            .isEnabled()) {
      log.info("API Requests Throttle Enabled");
      throttleSensors.forEach(sensor -> sensor.addCallback(entityService::handleThrottleEvent));
    } else {
      log.info("API Requests Throttle Disabled");
    }

    return entityService;
  }

  /**
   * Resolves the env-selected ({@code COORDINATED_INGEST_LOCK_PROVIDER}) coordination lock
   * substrate. Fails fast on a misconfiguration rather than silently downgrading coordination:
   * {@code hazelcast} without a live instance, or an unrecognized value, both throw. Correctness
   * never depends on the chosen provider — the DB single-sorted commit is authoritative — but the
   * operator's explicit choice must be honored.
   */
  @Nonnull
  private static CoordinationLockProvider buildLockProvider(
      @Nonnull final CoordinatedIngestConfiguration config,
      @javax.annotation.Nullable final HazelcastInstance hazelcastInstance) {
    final String selected = config.getLockProvider();
    switch (selected == null ? "" : selected) {
      case "local":
        log.info("Coordinated ingest lock provider: local (in-JVM only; DB commit authoritative).");
        return new LocalLockProvider();
      case "hazelcast":
        if (hazelcastInstance == null) {
          throw new IllegalStateException(
              "coordinatedIngest lockProvider=hazelcast but no HazelcastInstance is available; "
                  + "deploy Hazelcast or set COORDINATED_INGEST_LOCK_PROVIDER=local");
        }
        log.info("Coordinated ingest lock provider: hazelcast (distributed).");
        return new HazelcastLockProvider(hazelcastInstance);
        // case "redis": user-supplied RedisLockProvider bean (future distributed provider).
      default:
        throw new IllegalStateException(
            "Unknown coordinatedIngest lockProvider: "
                + selected
                + " (expected: local | hazelcast)");
    }
  }
}
