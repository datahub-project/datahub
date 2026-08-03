package com.linkedin.gms.factory.entity;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.CoordinatedIngestConfiguration;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.coordinator.ConflictKeyResolver;
import com.linkedin.metadata.entity.coordinator.CoordinationLockProvider;
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
      @javax.annotation.Nullable final CoordinationLockProvider coordinationLockProvider) {

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
      // The lock provider is a Spring bean chosen by CoordinationLockProviderFactory (env-selected,
      // pluggable — a new substrate is a new conditional bean, not a change here). Best-effort
      // serializer only; the DB single-sorted commit stays authoritative.
      MutationCoordinator mutationCoordinator =
          new MutationCoordinator(coordinationLockProvider, coordinatedIngestConfig, metricUtils);
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
}
