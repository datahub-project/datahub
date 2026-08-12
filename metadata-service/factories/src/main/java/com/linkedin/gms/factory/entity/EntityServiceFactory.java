package com.linkedin.gms.factory.entity;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.EntityServiceConfiguration;
import com.linkedin.metadata.dao.throttle.ThrottleSensor;
import com.linkedin.metadata.entity.AspectDao;
import com.linkedin.metadata.entity.EntityService;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.hooks.buffer.HookContextResolver;
import com.linkedin.metadata.entity.hooks.buffer.PostCommitHookBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import com.linkedin.metadata.event.EventProducer;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
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
      final ObjectProvider<RetentionBuffer> retentionBufferProvider,
      final ObjectProvider<PostCommitHookBuffer> postCommitHookBufferProvider,
      final ObjectProvider<HookContextResolver> hookContextResolverProvider) {

    FeatureFlags featureFlags = configurationProvider.getFeatureFlags();

    EntityServiceImpl entityService =
        new EntityServiceImpl(
            aspectDao,
            eventProducer,
            featureFlags.getPreProcessHooks(),
            new EntityServiceConfiguration()
                .setAlwaysEmitChangeLog(featureFlags.isAlwaysEmitChangeLog())
                .setCdcModeChangeLog(featureFlags.isCdcModeChangeLog())
                .setRetry(_ebeanMaxTransactionRetry)
                .setEnableBrowseV2(enableBrowsePathV2)
                .setPostCommitRetentionEnabled(featureFlags.isPostCommitRetentionEnabled()),
            metricUtils);

    // Absent (NO_OP) unless RetentionBufferFactory activated a coalesce-backed buffer.
    entityService.setRetentionBuffer(retentionBufferProvider.getIfAvailable());
    // Absent (NO_OP) unless PostCommitHookBufferFactory activated the async replay buffer
    // (featureFlags.postCommitHookBufferEnabled). NO_OP keeps synchronous hook execution.
    entityService.setPostCommitHookBuffer(postCommitHookBufferProvider.getIfAvailable());
    // Routing resolver for the async hook replay path. Absent (NO_OP / single-tenant) unless
    // PostCommitHookBufferFactory registered the SimpleHookContextResolver bean (or a cloud
    // extension registered a tenant-aware one as @Primary).
    entityService.setHookContextResolver(hookContextResolverProvider.getIfAvailable());

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
