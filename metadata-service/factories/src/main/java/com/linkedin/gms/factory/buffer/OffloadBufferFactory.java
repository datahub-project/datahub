package com.linkedin.gms.factory.buffer;

import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MaxSizePolicy;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.buffer.offload.DrainAction;
import com.linkedin.metadata.buffer.offload.HazelcastOffloadBuffer;
import com.linkedin.metadata.buffer.offload.OffloadBuffer;
import com.linkedin.metadata.buffer.offload.OffloadContextResolver;
import com.linkedin.metadata.buffer.offload.OffloadDrainer;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.OffloadBufferProperties;
import com.linkedin.metadata.config.offload.SizingPolicy;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.OperationContext;
import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 * Shared wiring for every async offload (post-commit hooks now; retention in a follow-up). A new
 * offload supplies only the feature bits — namespace, feature flag, key/value types, {@link
 * MergePolicy}, {@link SizingPolicy}, a serializable drain {@link Comparator}, a {@link
 * DrainAction}, and an {@link OffloadContextResolver} — and this factory provisions the three
 * namespaced {@link MapConfig}s, the {@link HazelcastOffloadBuffer}, the {@link OffloadDrainer},
 * and the drain scheduling. That removes the per-use "infra keys" (a dedicated
 * {@code @EnableScheduling} config, the duplicated drain-lock/paging/CAS code, and the per-use
 * map-config boilerplate).
 *
 * <h2>Scheduling</h2>
 *
 * No use carries {@code @EnableScheduling} or {@code @Scheduled}. This factory provisions a single
 * daemon {@link ThreadPoolTaskScheduler} ({@link #offloadTaskScheduler}, unless one already exists)
 * and each use calls {@link #scheduleDrainer} from its flag-gated drainer {@code @Bean} method,
 * registering {@code drainer::tick} at the use's {@code drainIntervalMs}. Programmatic scheduling
 * needs no {@code @EnableScheduling}; the bean is conditional so it never fights an existing
 * scheduler (e.g. GMS's analytics scheduler).
 *
 * <h2>Usage</h2>
 *
 * A use-specific {@code @Configuration} keeps its own flag-gated {@code @Bean} methods (so Spring's
 * conditional registration and the namespaced bean names are preserved) but delegates the actual
 * construction here:
 *
 * <pre>{@code
 * @Bean
 * public MapConfig myPendingMapConfig(ConfigurationProvider cp) {
 *   return offloadBufferFactory.pendingMapConfig(effectiveProps(cp));
 * }
 * @Bean
 * public MyDrainer myDrainer(...) {
 *   OffloadDrainer<K,V> d = offloadBufferFactory.createDrainer(buffer, resolver, sysCtx, action, props, true, "my", metricUtils);
 *   offloadBufferFactory.scheduleDrainer(d, props.getDrainIntervalMs());
 *   return new MyDrainer(d);
 * }
 * }</pre>
 */
@Configuration
public class OffloadBufferFactory {

  /**
   * Single daemon scheduler for all offload drainers. Conditional so it never replaces a scheduler
   * the host process already provides (GMS analytics, an app-level {@code @EnableScheduling}).
   */
  @Bean
  @ConditionalOnMissingBean(TaskScheduler.class)
  @Nonnull
  public ThreadPoolTaskScheduler offloadTaskScheduler() {
    ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(1);
    scheduler.setThreadNamePrefix("offload-drainer-");
    scheduler.setDaemon(true);
    scheduler.setWaitForTasksToCompleteOnShutdown(false);
    scheduler.setRemoveOnCancelPolicy(true);
    return scheduler;
  }

  /**
   * Pending-map {@link MapConfig}. {@link SizingPolicy#EVICT_LRU} gets a PER_NODE LRU {@link
   * EvictionConfig} sized to {@code maxPendingEntries} (latest-wins; eviction = bloat, not loss).
   * {@link SizingPolicy#REJECT_AT_CAP} gets <b>no</b> eviction — the bound is the {@code enqueue}
   * size-check (reject → caller sync fallback = no loss); eviction would silently drop a distinct
   * committed entry.
   */
  @Nonnull
  public MapConfig pendingMapConfig(@Nonnull OffloadBufferProperties props) {
    MapConfig mapConfig = new MapConfig(props.getMapName()).setBackupCount(1);
    if (props.getSizingPolicy() == SizingPolicy.EVICT_LRU) {
      mapConfig.setEvictionConfig(
          new EvictionConfig()
              .setEvictionPolicy(EvictionPolicy.LRU)
              .setMaxSizePolicy(MaxSizePolicy.PER_NODE)
              .setSize(props.getMaxPendingEntries()));
    }
    return mapConfig;
  }

  /** Drain-lock {@link MapConfig}: a single sentinel key ("drain") ever lives here; no eviction. */
  @Nonnull
  public MapConfig drainLockMapConfig(@Nonnull OffloadBufferProperties props) {
    return new MapConfig(props.getLockMapName()).setBackupCount(1);
  }

  /**
   * Sequence-counter {@link MapConfig}: a single long-lived key backs {@link
   * OffloadBuffer#nextSequence}. {@code backupCount=1} so a pod loss does not reset the sequence.
   * No eviction — the single entry must not be evicted or the sequence restarts and collides with
   * in-flight keys.
   */
  @Nonnull
  public MapConfig seqMapConfig(@Nonnull OffloadBufferProperties props) {
    return new MapConfig(props.getSeqMapName()).setBackupCount(1);
  }

  /**
   * Build a {@link HazelcastOffloadBuffer} from a use's properties + policies. The drain comparator
   * must be {@link Serializable} (it ships to Hazelcast cluster members for {@code
   * PagingPredicate}).
   */
  @Nonnull
  public <K extends Serializable, V extends Serializable> HazelcastOffloadBuffer<K, V> createBuffer(
      @Nonnull HazelcastInstance hazelcastInstance,
      @Nonnull OffloadBufferProperties props,
      @Nonnull MergePolicy mergePolicy,
      @Nonnull SizingPolicy sizingPolicy,
      @Nonnull Comparator<Map.Entry<K, V>> drainComparator,
      @Nonnull String metricPrefix,
      @Nullable MetricUtils metricUtils) {
    Objects.requireNonNull(hazelcastInstance, "hazelcastInstance");
    return new HazelcastOffloadBuffer<>(
        hazelcastInstance,
        props.getMapName(),
        props.getLockMapName(),
        props.getSeqMapName(),
        props.getMaxPendingEntries(),
        mergePolicy,
        sizingPolicy,
        drainComparator,
        metricPrefix,
        metricUtils);
  }

  /** Build an {@link OffloadDrainer} from a use's buffer, resolver, action, and properties. */
  @Nonnull
  public <K extends Serializable, V extends Serializable> OffloadDrainer<K, V> createDrainer(
      @Nonnull OffloadBuffer<K, V> buffer,
      @Nonnull OffloadContextResolver<K> contextResolver,
      @Nonnull OperationContext systemOperationContext,
      @Nonnull DrainAction<K, V> drainAction,
      @Nonnull OffloadBufferProperties props,
      boolean enabled,
      @Nonnull String metricPrefix,
      @Nullable MetricUtils metricUtils) {
    return new OffloadDrainer<>(
        buffer,
        contextResolver,
        systemOperationContext,
        drainAction,
        props.getDrainBatchSize(),
        props.getDrainLockLeaseMs(),
        enabled,
        metricPrefix,
        metricUtils,
        props.isBackoffEnabled(),
        props.getBackoffTicks());
  }

  /**
   * Register {@code drainer::tick} with the host's {@link TaskScheduler} at a fixed delay. No
   * {@code @EnableScheduling} is needed for programmatic scheduling. The returned {@link
   * ScheduledFuture} is logged but not retained — the drainer lives for the process lifetime and is
   * cancelled on JVM shutdown with the scheduler.
   */
  @Nonnull
  public ScheduledFuture<?> scheduleDrainer(
      @Nonnull TaskScheduler taskScheduler,
      @Nonnull OffloadDrainer<?, ?> drainer,
      long intervalMs) {
    return taskScheduler.scheduleWithFixedDelay(drainer::tick, Math.max(1L, intervalMs));
  }
}
