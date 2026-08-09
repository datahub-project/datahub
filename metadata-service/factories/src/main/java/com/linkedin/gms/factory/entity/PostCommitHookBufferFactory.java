package com.linkedin.gms.factory.entity;

import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.buffer.OffloadBufferFactory;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.buffer.HazelcastPostCommitHookBuffer;
import com.linkedin.metadata.buffer.offload.OffloadDrainer;
import com.linkedin.metadata.config.offload.MergePolicy;
import com.linkedin.metadata.config.offload.SizingPolicy;
import com.linkedin.metadata.config.hazelcast.HazelcastBootstrapProperties;
import com.linkedin.metadata.config.hooks.PostCommitHookBufferProperties;
import com.linkedin.metadata.entity.EntityServiceImpl;
import com.linkedin.metadata.entity.hooks.buffer.HookContextResolver;
import com.linkedin.metadata.entity.hooks.buffer.HookDrainAction;
import com.linkedin.metadata.entity.hooks.buffer.HookKey;
import com.linkedin.metadata.entity.hooks.buffer.HookPayload;
import com.linkedin.metadata.entity.hooks.buffer.PostCommitHookBuffer;
import com.linkedin.metadata.entity.hooks.buffer.PostCommitHookDrainer;
import com.linkedin.metadata.entity.hooks.buffer.PostCommitHookSink;
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
 * Wires the optional post-commit hook async replay buffer + drainer on top of the shared {@link
 * OffloadBufferFactory} (Hazelcast-backed). Controlled by {@link
 * HazelcastBootstrapProperties#POST_COMMIT_HOOK_BUFFER_ENABLED} ({@code
 * featureFlags.postCommitHookBufferEnabled}); turning it on also makes {@code
 * HazelcastInstanceBootstrapCondition} provision the shared {@code hazelcastInstance} bean.
 *
 * <p>Behavior:
 *
 * <ul>
 *   <li>{@code featureFlags.postCommitHookBufferEnabled=false} (default) — no beans created here;
 *       {@code EntityServiceImpl} keeps its {@code PostCommitHookBuffer.NO_OP} default and hooks
 *       run synchronously on the ingest thread (legacy behavior), even if a hook opts in via
 *       {@code MCPSideEffect#defersPostCommit()}.
 *   <li>Flag true — a real {@link HazelcastPostCommitHookBuffer} + {@link PostCommitHookDrainer}
 *       over the shared Hazelcast map. Hooks that opt in via {@code defersPostCommit()} are
 *       enqueued for async replay; the rest still run inline.
 * </ul>
 *
 * <p>All infra (map provisioning, buffer construction, drainer construction, scheduling) is
 * delegated to {@link OffloadBufferFactory}; this class supplies only the hook feature bits
 * ({@link MergePolicy#NO_COALESCE} + {@link SizingPolicy#REJECT_AT_CAP}, the FIFO drain comparator,
 * and the {@link HookDrainAction}) and the namespaced {@link MapConfig} beans. Scheduling is
 * programmatic via the shared factory's {@link TaskScheduler} — no {@code @EnableScheduling}
 * config is needed here (the old {@code PostCommitHookBufferSchedulingConfig} is deleted).
 */
@Slf4j
@Configuration
public class PostCommitHookBufferFactory {

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.POST_COMMIT_HOOK_BUFFER_ENABLED,
      havingValue = "true")
  @Nonnull
  public MapConfig postCommitHookPendingMapConfig(
      ConfigurationProvider configurationProvider, OffloadBufferFactory offloadBufferFactory) {
    // REJECT_AT_CAP → no EvictionConfig (the framework's enqueue size-check is the no-loss bound;
    // eviction would silently drop a distinct committed MCL = lost side effect).
    return offloadBufferFactory.pendingMapConfig(effectiveProperties(configurationProvider));
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.POST_COMMIT_HOOK_BUFFER_ENABLED,
      havingValue = "true")
  @Nonnull
  public MapConfig postCommitHookDrainLockMapConfig(
      ConfigurationProvider configurationProvider, OffloadBufferFactory offloadBufferFactory) {
    return offloadBufferFactory.drainLockMapConfig(effectiveProperties(configurationProvider));
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.POST_COMMIT_HOOK_BUFFER_ENABLED,
      havingValue = "true")
  @Nonnull
  public MapConfig postCommitHookSeqMapConfig(
      ConfigurationProvider configurationProvider, OffloadBufferFactory offloadBufferFactory) {
    return offloadBufferFactory.seqMapConfig(effectiveProperties(configurationProvider));
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.POST_COMMIT_HOOK_BUFFER_ENABLED,
      havingValue = "true")
  @Nonnull
  public PostCommitHookBuffer postCommitHookBuffer(
      @Qualifier("hazelcastInstance") @Lazy HazelcastInstance hazelcastInstance,
      ConfigurationProvider configurationProvider,
      OffloadBufferFactory offloadBufferFactory,
      @Nullable MetricUtils metricUtils) {
    PostCommitHookBufferProperties props = effectiveProperties(configurationProvider);
    return new HazelcastPostCommitHookBuffer(
        offloadBufferFactory.createBuffer(
            hazelcastInstance,
            props,
            MergePolicy.NO_COALESCE,
            SizingPolicy.REJECT_AT_CAP,
            HazelcastPostCommitHookBuffer.drainOrder(),
            "post_commit_hook",
            metricUtils));
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.POST_COMMIT_HOOK_BUFFER_ENABLED,
      havingValue = "true")
  @ConditionalOnMissingBean(HookContextResolver.class)
  @Nonnull
  public HookContextResolver hookContextResolver(
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext) {
    return new SimpleHookContextResolver(systemOperationContext);
  }

  @Bean
  @ConditionalOnProperty(
      name = HazelcastBootstrapProperties.POST_COMMIT_HOOK_BUFFER_ENABLED,
      havingValue = "true")
  @Nonnull
  public PostCommitHookDrainer postCommitHookDrainer(
      PostCommitHookBuffer postCommitHookBuffer,
      HookContextResolver hookContextResolver,
      @Qualifier("systemOperationContext") @Lazy OperationContext systemOperationContext,
      @Qualifier("entityService") @Lazy EntityServiceImpl entityService,
      ConfigurationProvider configurationProvider,
      OffloadBufferFactory offloadBufferFactory,
      TaskScheduler taskScheduler,
      @Nullable MetricUtils metricUtils) {
    PostCommitHookBufferProperties props = effectiveProperties(configurationProvider);
    // Sink feeds a deferred hook's generated MCPs back through the normal async ingest path
    // (mirrors the synchronous processPostCommitMCLSideEffects emit block).
    PostCommitHookSink sink = entityService::ingestSideEffectMcps;
    OffloadDrainer<HookKey, HookPayload> drainer =
        offloadBufferFactory.createDrainer(
            ((HazelcastPostCommitHookBuffer) postCommitHookBuffer).getDelegate(),
            hookContextResolver,
            systemOperationContext,
            new HookDrainAction(sink, metricUtils),
            props,
            true,
            "post_commit_hook",
            metricUtils);
    // Programmatic scheduling — no @EnableScheduling / @Scheduled needed.
    offloadBufferFactory.scheduleDrainer(taskScheduler, drainer, props.getDrainIntervalMs());
    return new PostCommitHookDrainer(drainer);
  }

  @Nonnull
  private static PostCommitHookBufferProperties effectiveProperties(
      @Nonnull ConfigurationProvider configurationProvider) {
    if (configurationProvider.getDatahub() != null
        && configurationProvider.getDatahub().getPostCommitHook() != null
        && configurationProvider.getDatahub().getPostCommitHook().getBuffer() != null) {
      return configurationProvider.getDatahub().getPostCommitHook().getBuffer();
    }
    return new PostCommitHookBufferProperties();
  }
}
