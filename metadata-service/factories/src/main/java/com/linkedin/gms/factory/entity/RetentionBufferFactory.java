package com.linkedin.gms.factory.entity;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.buffer.OffloadBufferFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.buffer.offload.OffloadDrainer;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import com.linkedin.metadata.config.hazelcast.HazelcastBootstrapProperties;
import com.linkedin.metadata.config.retention.RetentionBufferProperties;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.SimpleRetentionContextResolver;
import com.linkedin.metadata.entity.retention.buffer.CoalesceRetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionDrainAction;
import com.linkedin.metadata.entity.retention.buffer.RetentionDrainer;
import com.linkedin.metadata.entity.retention.buffer.RetentionKey;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.TaskScheduler;

/**
 * Wires the optional post-commit retention buffer + drainer on top of the shared {@link
 * OffloadBufferFactory} (Hazelcast-backed). Controlled by {@link
 * HazelcastBootstrapProperties#RETENTION_BUFFER_ENABLED} ({@code
 * featureFlags.retentionBufferEnabled} / {@code RETENTION_BUFFER_ENABLED}); turning it on also
 * makes {@code HazelcastInstanceBootstrapCondition} provision the shared {@code hazelcastInstance}
 * bean.
 *
 * <p>Behavior matrix:
 *
 * <ul>
 *   <li>{@code featureFlags.retentionBufferEnabled=false} (default) — no beans created here; {@code
 *       EntityServiceImpl} keeps its {@code RetentionBuffer.NO_OP} default and applies retention
 *       synchronously post-commit (when post-commit itself is on).
 *   <li>{@code retentionBufferEnabled=true} but {@code postCommitRetentionEnabled=false} — neither
 *       buffer nor drainer bean is created; {@code EntityServiceImpl} keeps {@code
 *       RetentionBuffer.NO_OP} (sync DELETE).
 *   <li>Both flags true — a real {@link CoalesceRetentionBuffer} + {@link RetentionDrainer} over
 *       the shared Hazelcast map.
 * </ul>
 *
 * <p>All infra (map provisioning, buffer construction, drainer construction, scheduling) is
 * delegated to {@link OffloadBufferFactory}; this class supplies only the retention feature bits
 * ({@link MergePolicy#KEEP_MAX_LONG} + {@link SizingPolicy#EVICT_LRU}, the retention drain
 * comparator, and the {@link RetentionDrainAction}) and the namespaced {@link MapConfig} beans.
 * Scheduling is programmatic via the shared factory's {@link TaskScheduler} — no {@code
 * @EnableScheduling} config is needed here (the old {@code RetentionBufferSchedulingConfig} is
 * deleted).
 */
@Slf4j
@Configuration
public class RetentionBufferFactory {

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public MapConfig retentionPendingMapConfig(
      ConfigurationProvider configurationProvider, OffloadBufferFactory offloadBufferFactory) {
    // EVICT_LRU → the framework provisions a PER_NODE LRU EvictionConfig sized to
    // maxPendingEntries (latest-wins; eviction = bloat, not loss).
    return offloadBufferFactory.pendingMapConfig(effectiveProperties(configurationProvider));
  }

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public MapConfig retentionDrainLockMapConfig(
      ConfigurationProvider configurationProvider, OffloadBufferFactory offloadBufferFactory) {
    return offloadBufferFactory.drainLockMapConfig(effectiveProperties(configurationProvider));
  }

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public MapConfig retentionSeqMapConfig(
      ConfigurationProvider configurationProvider, OffloadBufferFactory offloadBufferFactory) {
    return offloadBufferFactory.seqMapConfig(effectiveProperties(configurationProvider));
  }

  // The single OffloadBuffer bean is shared: retentionBuffer enqueues into it and retentionDrainer
  // drains it, so no RetentionBuffer downcast is needed. (Two-flag gating + state matrix: class
  // javadoc; none is created rather than a null bean when only one flag is on.)
  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public OffloadBuffer<RetentionKey, Long> retentionOffloadBuffer(
      @Qualifier("hazelcastInstance") @Lazy HazelcastInstance hazelcastInstance,
      ConfigurationProvider configurationProvider,
      OffloadBufferFactory offloadBufferFactory,
      @Nullable MetricUtils metricUtils) {
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    return offloadBufferFactory.createBuffer(
        hazelcastInstance,
        props,
        MergePolicy.KEEP_MAX_LONG,
        SizingPolicy.EVICT_LRU,
        CoalesceRetentionBuffer.drainOrder(),
        "retention",
        metricUtils);
  }

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @ConditionalOnMissingBean(RetentionContextResolver.class)
  @Nonnull
  public RetentionContextResolver<RetentionKey> retentionContextResolver() {
    return new SimpleRetentionContextResolver();
  }

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public RetentionBuffer retentionBuffer(
      OffloadBuffer<RetentionKey, Long> retentionOffloadBuffer,
      RetentionContextResolver<RetentionKey> retentionContextResolver) {
    return new CoalesceRetentionBuffer(retentionOffloadBuffer, retentionContextResolver);
  }

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public RetentionDrainer retentionDrainer(
      OffloadBuffer<RetentionKey, Long> retentionOffloadBuffer,
      RetentionContextResolver<RetentionKey> retentionContextResolver,
      @Qualifier("retentionService") RetentionService<ChangeItemImpl> retentionService,
      ConfigurationProvider configurationProvider,
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext,
      OffloadBufferFactory offloadBufferFactory,
      TaskScheduler taskScheduler,
      @Nullable MetricUtils metricUtils) {
    if (!(retentionService instanceof EbeanRetentionService)) {
      // Only EbeanRetentionService overrides applyRetentionBatchWithPolicyDefaults to apply each
      // context in its own transaction (poison-pair isolation). Other impls (e.g. Cassandra)
      // inherit the default, which treats the whole batch as all-or-nothing — weaker contract, no
      // data loss.
      log.warn(
          "Coalesced retention drainer wired with non-Ebean RetentionService ({}); batch retention"
              + " has no per-context transaction isolation.",
          retentionService.getClass().getSimpleName());
    }
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    OffloadDrainer<RetentionKey, Long> drainer =
        offloadBufferFactory.createDrainer(
            retentionOffloadBuffer,
            retentionContextResolver,
            systemOperationContext,
            new RetentionDrainAction(retentionService, metricUtils),
            props,
            true,
            "retention",
            metricUtils);
    // Programmatic scheduling — no @EnableScheduling / @Scheduled needed.
    offloadBufferFactory.scheduleDrainer(taskScheduler, drainer, props.getDrainIntervalMs());
    return new RetentionDrainer(drainer);
  }

  @Nonnull
  private static RetentionBufferProperties effectiveProperties(
      @Nonnull ConfigurationProvider configurationProvider) {
    if (configurationProvider.getDatahub().getRetention() != null
        && configurationProvider.getDatahub().getRetention().getBuffer() != null) {
      return configurationProvider.getDatahub().getRetention().getBuffer();
    }
    return new RetentionBufferProperties();
  }
}
