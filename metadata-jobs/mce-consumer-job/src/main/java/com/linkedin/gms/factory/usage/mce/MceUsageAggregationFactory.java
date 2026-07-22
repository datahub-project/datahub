package com.linkedin.gms.factory.usage.mce;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.usage.UsageAggregationFactory;
import com.linkedin.gms.factory.usage.UsageYamlConfigFactory;
import com.linkedin.metadata.config.UsageAggregationConfiguration;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.kafka.usage.UsageQueueIngestRecorder;
import com.linkedin.metadata.usage.flush.AdaptiveFlushCoordinator;
import com.linkedin.metadata.usage.flush.MicrometerUsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushSinkComposer;
import com.linkedin.metadata.usage.identity.AspectCorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricContributor;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import com.linkedin.metadata.usage.store.InMemoryUsageAggregationStore;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PreDestroy;
import java.util.List;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

/**
 * Slim usage aggregation wiring for the MCE consumer — store, flush coordinator, Micrometer export,
 * and queue-path ingest recorder. No servlet filters (MCE has no HTTP API traffic).
 */
@Slf4j
@Configuration
@Import(UsageYamlConfigFactory.class)
@ConditionalOnProperty(name = "datahub.usage.aggregation.enabled", havingValue = "true")
public class MceUsageAggregationFactory {

  private AdaptiveFlushCoordinator adaptiveFlushCoordinator;

  @Bean
  @Nonnull
  public UsageOperationsLoader usageOperationsLoader(
      @Qualifier("usageYamlMapper") ObjectMapper usageYamlMapper) {
    return new UsageOperationsLoader(usageYamlMapper);
  }

  @Bean
  @Nonnull
  public UsageQueueIngestRecorder usageQueueIngestRecorder(
      @Qualifier("usageAggregationStore") InMemoryUsageAggregationStore usageAggregationStore) {
    return new UsageQueueIngestRecorder(usageAggregationStore);
  }

  @Bean
  @Nonnull
  public UsageMetricRegistryLoader mceUsageMetricRegistryLoader(
      @Qualifier("usageYamlMapper") ObjectMapper usageYamlMapper) {
    return new UsageMetricRegistryLoader(usageYamlMapper);
  }

  @Bean
  @Nonnull
  public UsageOperationsRegistry mceUsageOperationsRegistry(
      UsageOperationsLoader usageOperationsLoader) {
    return UsageOperationsRegistry.loadOssOnly(usageOperationsLoader);
  }

  @Bean
  @Nonnull
  public UsageMetricRegistry mceUsageMetricRegistry(
      @Qualifier("mceUsageMetricRegistryLoader")
          UsageMetricRegistryLoader usageMetricRegistryLoader,
      @Autowired(required = false) List<UsageMetricContributor> contributors) {
    return UsageMetricRegistry.loadBundled(
        usageMetricRegistryLoader, contributors != null ? contributors : List.of());
  }

  @Bean
  @Nonnull
  public AspectCorpUserFlagsProvider mceCorpUserFlagsProvider() {
    return new AspectCorpUserFlagsProvider();
  }

  @Bean
  @Nonnull
  public UsageActorClassResolver mceUsageActorClassResolver(
      CorpUserFlagsProvider mceCorpUserFlagsProvider) {
    return new UsageActorClassResolver(mceCorpUserFlagsProvider);
  }

  @Bean
  @Nonnull
  @ConditionalOnProperty(
      name = "datahub.usage.aggregation.micrometerExport.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public MicrometerUsageFlushSink mceMicrometerUsageFlushSink(
      @Qualifier("mceUsageMetricRegistry") UsageMetricRegistry usageMetricRegistry,
      MeterRegistry meterRegistry) {
    return new MicrometerUsageFlushSink(usageMetricRegistry, meterRegistry);
  }

  @Bean
  @Nonnull
  public UsageFlushSinkComposer mceUsageFlushSinkComposer(List<UsageFlushSink> usageFlushSinks) {
    List<UsageFlushSink> delegates =
        usageFlushSinks.stream().filter(sink -> !(sink instanceof UsageFlushSinkComposer)).toList();
    return new UsageFlushSinkComposer(delegates);
  }

  @Bean(name = "usageAggregationStore")
  @Nonnull
  public InMemoryUsageAggregationStore mceUsageAggregationStore(
      @Qualifier("mceUsageOperationsRegistry") UsageOperationsRegistry usageOperationsRegistry,
      @Qualifier("mceUsageMetricRegistry") UsageMetricRegistry usageMetricRegistry,
      UsageActorClassResolver mceUsageActorClassResolver,
      UsageFlushSinkComposer mceUsageFlushSinkComposer,
      ConfigurationProvider configurationProvider) {
    UsageAggregationConfiguration config =
        UsageAggregationFactory.resolveAggregationConfig(configurationProvider);
    UsageAggregationConfiguration.FlushConfiguration flush = config.getFlush();
    return new InMemoryUsageAggregationStore(
        usageOperationsRegistry,
        usageMetricRegistry,
        mceUsageActorClassResolver,
        mceUsageFlushSinkComposer,
        flush.getMaxCardinality(),
        flush.getMaxWindowSeconds(),
        flush.getRetryAttempts(),
        flush.getRetryInitialBackoffMillis(),
        flush.getAlignmentPeriodSeconds(),
        flush.isIncludeAgentNameDimension());
  }

  @Bean
  @Nonnull
  public AdaptiveFlushCoordinator mceAdaptiveFlushCoordinator(
      @Qualifier("usageAggregationStore") InMemoryUsageAggregationStore usageAggregationStore,
      ConfigurationProvider configurationProvider) {
    UsageAggregationConfiguration config =
        UsageAggregationFactory.resolveAggregationConfig(configurationProvider);
    adaptiveFlushCoordinator =
        new AdaptiveFlushCoordinator(
            usageAggregationStore,
            config.getFlush().getScheduledIntervalSeconds(),
            usageAggregationStore.clock());
    return adaptiveFlushCoordinator;
  }

  @PreDestroy
  public void shutdownMceUsageAggregationFlush() {
    if (adaptiveFlushCoordinator != null) {
      log.info("Flushing MCE in-memory usage aggregation on shutdown");
      adaptiveFlushCoordinator.shutdown();
      adaptiveFlushCoordinator = null;
    }
  }
}
