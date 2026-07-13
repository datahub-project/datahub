package com.linkedin.gms.factory.usage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.UsageAggregationConfiguration;
import com.linkedin.metadata.config.UsageConfiguration;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.flush.AdaptiveFlushCoordinator;
import com.linkedin.metadata.usage.flush.MicrometerUsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushSink;
import com.linkedin.metadata.usage.flush.UsageFlushSinkComposer;
import com.linkedin.metadata.usage.identity.AspectCorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.CorpUserFlagsProvider;
import com.linkedin.metadata.usage.identity.UsageActorClassResolver;
import com.linkedin.metadata.usage.instrumentation.UsageMetricsSessionEnricher;
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
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

@Slf4j
@Configuration
@ConditionalOnProperty(name = "datahub.usage.aggregation.enabled", havingValue = "true")
public class UsageAggregationFactory {

  private AdaptiveFlushCoordinator adaptiveFlushCoordinator;

  @Bean
  @Nonnull
  public UsageMetricRegistryLoader usageMetricRegistryLoader(
      @Qualifier("usageYamlMapper") ObjectMapper usageYamlMapper) {
    return new UsageMetricRegistryLoader(usageYamlMapper);
  }

  @Bean
  @Nonnull
  public UsageOperationsRegistry usageOperationsRegistry(
      UsageOperationsLoader usageOperationsLoader) {
    return UsageOperationsRegistry.loadOssOnly(usageOperationsLoader);
  }

  @Bean
  @Nonnull
  public UsageMetricRegistry usageMetricRegistry(
      UsageMetricRegistryLoader usageMetricRegistryLoader,
      @Autowired(required = false) List<UsageMetricContributor> contributors) {
    return UsageMetricRegistry.loadBundled(
        usageMetricRegistryLoader, contributors != null ? contributors : List.of());
  }

  @Bean
  @Nonnull
  public AspectCorpUserFlagsProvider corpUserFlagsProvider() {
    return new AspectCorpUserFlagsProvider();
  }

  @Bean
  @Nonnull
  public UsageActorClassResolver usageActorClassResolver(
      CorpUserFlagsProvider corpUserFlagsProvider) {
    return new UsageActorClassResolver(corpUserFlagsProvider);
  }

  @Bean
  @Nonnull
  @ConditionalOnProperty(
      name = "datahub.usage.aggregation.micrometerExport.enabled",
      havingValue = "true",
      matchIfMissing = true)
  public MicrometerUsageFlushSink micrometerUsageFlushSink(
      UsageMetricRegistry usageMetricRegistry, MeterRegistry meterRegistry) {
    return new MicrometerUsageFlushSink(usageMetricRegistry, meterRegistry);
  }

  @Bean
  @Nonnull
  public UsageFlushSinkComposer usageFlushSinkComposer(List<UsageFlushSink> usageFlushSinks) {
    List<UsageFlushSink> delegates =
        usageFlushSinks.stream().filter(sink -> !(sink instanceof UsageFlushSinkComposer)).toList();
    return new UsageFlushSinkComposer(delegates);
  }

  @Bean(name = "usageAggregationStore")
  @Nonnull
  public InMemoryUsageAggregationStore usageAggregationStore(
      UsageOperationsRegistry usageOperationsRegistry,
      UsageMetricRegistry usageMetricRegistry,
      UsageActorClassResolver usageActorClassResolver,
      UsageFlushSinkComposer usageFlushSinkComposer,
      ConfigurationProvider configurationProvider) {
    // When commercial usage metrics are enabled, @Primary overlay-backed registries are injected.
    UsageAggregationConfiguration config = resolveAggregationConfig(configurationProvider);
    UsageAggregationConfiguration.FlushConfiguration flush = config.getFlush();
    return new InMemoryUsageAggregationStore(
        usageOperationsRegistry,
        usageMetricRegistry,
        usageActorClassResolver,
        usageFlushSinkComposer,
        flush.getMaxCardinality(),
        flush.getMaxWindowSeconds(),
        flush.getRetryAttempts(),
        flush.getRetryInitialBackoffMillis(),
        flush.getAlignmentPeriodSeconds());
  }

  @Bean
  @Nonnull
  public AdaptiveFlushCoordinator adaptiveFlushCoordinator(
      InMemoryUsageAggregationStore usageAggregationStore,
      ConfigurationProvider configurationProvider) {
    UsageAggregationConfiguration config = resolveAggregationConfig(configurationProvider);
    adaptiveFlushCoordinator =
        new AdaptiveFlushCoordinator(
            usageAggregationStore,
            config.getFlush().getScheduledIntervalSeconds(),
            usageAggregationStore.clock());
    return adaptiveFlushCoordinator;
  }

  @PreDestroy
  public void shutdownUsageAggregationFlush() {
    if (adaptiveFlushCoordinator != null) {
      log.info("Flushing in-memory usage aggregation on shutdown");
      adaptiveFlushCoordinator.shutdown();
      adaptiveFlushCoordinator = null;
    }
  }

  @Bean
  @Nonnull
  public UsageMetricsSessionEnricher usageMetricsSessionEnricher(
      InMemoryUsageAggregationStore usageAggregationStore) {
    return new UsageMetricsSessionEnricher(usageAggregationStore, true);
  }

  @Bean
  @Order(Ordered.LOWEST_PRECEDENCE - 10)
  public FilterRegistrationBean<UsageRecordingFilter> usageRecordingFilterRegistration(
      UsageMetricsSessionEnricher usageMetricsSessionEnricher) {
    FilterRegistrationBean<UsageRecordingFilter> registration = new FilterRegistrationBean<>();
    registration.setFilter(new UsageRecordingFilter(usageMetricsSessionEnricher, true));
    registration.addUrlPatterns("/*");
    registration.setOrder(Ordered.LOWEST_PRECEDENCE - 10);
    return registration;
  }

  public static UsageAggregationConfiguration resolveAggregationConfig(
      ConfigurationProvider configurationProvider) {
    UsageConfiguration usage = configurationProvider.getDatahub().getUsage();
    if (usage == null || usage.getAggregation() == null) {
      throw new IllegalStateException(
          "datahub.usage.aggregation must be defined in application.yaml");
    }
    UsageAggregationConfiguration aggregation = usage.getAggregation();
    if (aggregation.getFlush() == null) {
      throw new IllegalStateException(
          "datahub.usage.aggregation.flush must be defined in application.yaml");
    }
    return aggregation;
  }
}
