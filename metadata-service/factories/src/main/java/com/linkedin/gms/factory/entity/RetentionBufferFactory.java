package com.linkedin.gms.factory.entity;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBufferFactory;
import com.linkedin.metadata.config.hazelcast.HazelcastBootstrapProperties;
import com.linkedin.metadata.config.retention.RetentionBufferProperties;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.EbeanRetentionService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.retention.RetentionContextResolver;
import com.linkedin.metadata.entity.retention.RetentionKey;
import com.linkedin.metadata.entity.retention.buffer.CoalesceRetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionDrainer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Wires the optional post-commit retention buffer + drainer on top of the {@link
 * CoalesceBufferFactory} (Hazelcast-backed). Controlled by {@link
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
 * <p>{@link #retentionPendingMapConfig} and {@link #retentionDrainLockMapConfig} register bounded
 * {@link MapConfig}s (gated on the same flag) that {@code CacheConfig.hazelcastInstance} picks up
 * automatically via its {@code List<MapConfig>} dependency (no {@code hazelcast.xml}/{@code .yaml}
 * config file exists in this repo).
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
  public MapConfig retentionPendingMapConfig(ConfigurationProvider configurationProvider) {
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    MapConfig mapConfig = new MapConfig(props.getMapName()).setBackupCount(1);
    // Second line of defense behind the maxPendingEntries soft-cap enforced in
    // HazelcastCoalesceBuffer.merge; matches "overflow drop -> metric, bloat not loss".
    mapConfig.setEvictionConfig(
        new EvictionConfig()
            .setEvictionPolicy(EvictionPolicy.LRU)
            .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
            .setSize(props.getMaxPendingEntries()));
    return mapConfig;
  }

  @Bean
  @ConditionalOnProperty(
      name = {
        HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
        HazelcastBootstrapProperties.POST_COMMIT_RETENTION_ENABLED
      },
      havingValue = "true")
  @Nonnull
  public MapConfig retentionDrainLockMapConfig(ConfigurationProvider configurationProvider) {
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    // Single sentinel key ("drain") ever lives here; no eviction needed.
    return new MapConfig(props.getLockMapName()).setBackupCount(1);
  }

  // The single CoalesceBuffer bean is shared: retentionBuffer enqueues into it and retentionDrainer
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
  public CoalesceBuffer<RetentionKey, Long> retentionCoalesceBuffer(
      ConfigurationProvider configurationProvider, CoalesceBufferFactory coalesceBufferFactory) {
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    return coalesceBufferFactory.create(
        props.getMapName(), props.getLockMapName(), props.getMaxPendingEntries());
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
      CoalesceBuffer<RetentionKey, Long> retentionCoalesceBuffer,
      RetentionContextResolver retentionContextResolver) {
    return new CoalesceRetentionBuffer(retentionCoalesceBuffer, retentionContextResolver);
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
      CoalesceBuffer<RetentionKey, Long> retentionCoalesceBuffer,
      @Qualifier("retentionService") RetentionService<ChangeItemImpl> retentionService,
      ConfigurationProvider configurationProvider,
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext,
      RetentionContextResolver retentionContextResolver,
      @Nullable MetricUtils metricUtils) {
    if (!(retentionService instanceof EbeanRetentionService)) {
      // Only EbeanRetentionService overrides applyRetentionBatchWithPolicyDefaults to apply each
      // context in its own transaction (poison-pair isolation). Other impls (e.g. Cassandra)
      // inherit
      // the default, which treats the whole batch as all-or-nothing — weaker contract, no data
      // loss.
      log.warn(
          "Coalesced retention drainer wired with non-Ebean RetentionService ({}); batch retention"
              + " has no per-context transaction isolation.",
          retentionService.getClass().getSimpleName());
    }
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    return new RetentionDrainer(
        retentionCoalesceBuffer,
        retentionService,
        systemOperationContext,
        retentionContextResolver,
        props.getDrainBatchSize(),
        props.getDrainLockLeaseMs(),
        true,
        metricUtils);
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
