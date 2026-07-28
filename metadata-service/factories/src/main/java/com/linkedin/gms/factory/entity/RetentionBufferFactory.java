package com.linkedin.gms.factory.entity;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.buffer.CoalesceBuffer;
import com.linkedin.metadata.buffer.CoalesceBufferFactory;
import com.linkedin.metadata.config.hazelcast.HazelcastBootstrapProperties;
import com.linkedin.metadata.config.hazelcast.RetentionBufferHazelcastCondition;
import com.linkedin.metadata.config.retention.RetentionBufferProperties;
import com.linkedin.metadata.entity.RetentionService;
import com.linkedin.metadata.entity.ebean.batch.ChangeItemImpl;
import com.linkedin.metadata.entity.retention.buffer.CoalesceRetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionBuffer;
import com.linkedin.metadata.entity.retention.buffer.RetentionDrainer;
import com.linkedin.metadata.entity.retention.buffer.RetentionKey;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;

/**
 * Wires the optional post-commit retention buffer + drainer (see
 * docs/superpowers/plans/2026-07-28-hazelcast-retention-buffer.md and the coalesce buffer design
 * doc) on top of the store-agnostic {@link CoalesceBufferFactory}. Controlled by {@link
 * HazelcastBootstrapProperties#RETENTION_BUFFER_ENABLED} ({@code
 * featureFlags.retentionBufferEnabled} / {@code RETENTION_BUFFER_ENABLED}); the backend (Caffeine
 * or Hazelcast) is selected separately by {@code datahub.buffer.implementation}, which also decides
 * whether {@code HazelcastInstanceBootstrapCondition} provisions the shared {@code
 * hazelcastInstance} bean for this feature.
 *
 * <p>Behavior matrix:
 *
 * <ul>
 *   <li>{@code featureFlags.retentionBufferEnabled=false} (default) — no beans created here; {@code
 *       EntityServiceImpl} keeps its {@code RetentionBuffer.NO_OP} default and applies retention
 *       synchronously post-commit (when post-commit itself is on).
 *   <li>{@code retentionBufferEnabled=true} but {@code postCommitRetentionEnabled=false} — falls
 *       back to {@code RetentionBuffer.NO_OP} (sync DELETE); no drainer bean is created.
 *   <li>Both flags true — a real {@link CoalesceRetentionBuffer} + {@link RetentionDrainer}, backed
 *       by whichever implementation {@link CoalesceBufferFactory} resolved.
 * </ul>
 *
 * <p>{@link #retentionPendingMapConfig} and {@link #retentionDrainLockMapConfig} only matter when
 * the Hazelcast backend is selected; they register bounded {@link MapConfig}s that {@code
 * CacheConfig.hazelcastInstance} picks up automatically via its {@code List<MapConfig>} dependency
 * (no {@code hazelcast.xml}/{@code .yaml} config file exists in this repo).
 */
@Slf4j
@Configuration
public class RetentionBufferFactory {

  @Bean
  @Conditional(RetentionBufferHazelcastCondition.class)
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
  @Conditional(RetentionBufferHazelcastCondition.class)
  @Nonnull
  public MapConfig retentionDrainLockMapConfig(ConfigurationProvider configurationProvider) {
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    // Single sentinel key ("drain") ever lives here; no eviction needed.
    return new MapConfig(props.getLockMapName()).setBackupCount(1);
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
      havingValue = "true")
  @Nonnull
  public RetentionBuffer retentionBuffer(
      ConfigurationProvider configurationProvider, CoalesceBufferFactory coalesceBufferFactory) {
    boolean postCommitRetentionEnabled =
        configurationProvider.getFeatureFlags().isPostCommitRetentionEnabled();
    if (!postCommitRetentionEnabled) {
      log.warn(
          "featureFlags.retentionBufferEnabled=true but featureFlags.postCommitRetentionEnabled"
              + " is false — falling back to synchronous post-commit retention (NO_OP buffer)");
      return RetentionBuffer.NO_OP;
    }
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    CoalesceBuffer<RetentionKey, Long> coalesceBuffer =
        coalesceBufferFactory.create(
            props.getMapName(), props.getLockMapName(), props.getMaxPendingEntries());
    return new CoalesceRetentionBuffer(coalesceBuffer);
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.RETENTION_BUFFER_ENABLED,
      havingValue = "true")
  @Nullable
  public RetentionDrainer retentionDrainer(
      RetentionBuffer retentionBuffer,
      @Qualifier("retentionService") RetentionService<ChangeItemImpl> retentionService,
      ConfigurationProvider configurationProvider,
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext,
      @Nullable MetricUtils metricUtils) {
    if (!(retentionBuffer instanceof CoalesceRetentionBuffer)) {
      // retentionBuffer() above already fell back to NO_OP (post-commit flag off) and logged
      // why; no drainer needed without a real coalesce-backed buffer.
      return null;
    }
    RetentionBufferProperties props = effectiveProperties(configurationProvider);
    return new RetentionDrainer(
        ((CoalesceRetentionBuffer) retentionBuffer).getCoalesceBuffer(),
        retentionService,
        systemOperationContext,
        props.getDrainBatchSize(),
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
