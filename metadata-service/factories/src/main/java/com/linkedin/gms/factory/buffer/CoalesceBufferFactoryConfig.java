package com.linkedin.gms.factory.buffer;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.buffer.BufferImplementation;
import com.linkedin.metadata.buffer.CoalesceBufferFactory;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wires the {@link CoalesceBufferFactory} bean used by any feature that needs a store-agnostic
 * coalescing buffer (e.g. post-commit retention). Always created (unlike the Hazelcast-gated
 * retention beans) since callers such as {@code RetentionBufferFactory} only build a buffer when
 * their own feature flag is on.
 */
@Slf4j
@Configuration
public class CoalesceBufferFactoryConfig {

  @Bean("coalesceBufferFactory")
  @Nonnull
  public CoalesceBufferFactory coalesceBufferFactory(
      ConfigurationProvider configurationProvider,
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Nullable MetricUtils metricUtils) {
    return new DefaultCoalesceBufferFactory(
        resolveImplementation(configurationProvider), hazelcastInstance, metricUtils);
  }

  @Nonnull
  private static BufferImplementation resolveImplementation(
      @Nonnull ConfigurationProvider configurationProvider) {
    String raw =
        configurationProvider.getDatahub().getBuffer() != null
            ? configurationProvider.getDatahub().getBuffer().getImplementation()
            : "caffeine";
    try {
      return BufferImplementation.valueOf(raw.trim().toUpperCase());
    } catch (IllegalArgumentException e) {
      log.warn("Unknown datahub.buffer.implementation '{}'; falling back to caffeine", raw);
      return BufferImplementation.CAFFEINE;
    }
  }
}
