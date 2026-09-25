package com.linkedin.gms.factory.buffer;

import com.hazelcast.core.HazelcastInstance;
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
 * Wires the {@link CoalesceBufferFactory} bean used by any feature that needs a coalescing buffer
 * (e.g. post-commit retention). Always created (unlike the flag-gated retention beans) since
 * callers such as {@code RetentionBufferFactory} only build a buffer when their own feature flag is
 * on. The {@code hazelcastInstance} is optional here: it is null when no feature has requested the
 * embedded node, and the factory only dereferences it in {@code create()}, which the retention flag
 * guards.
 */
@Slf4j
@Configuration
public class CoalesceBufferFactoryConfig {

  @Bean("coalesceBufferFactory")
  @Nonnull
  public CoalesceBufferFactory coalesceBufferFactory(
      @Autowired(required = false) @Qualifier("hazelcastInstance")
          HazelcastInstance hazelcastInstance,
      @Nullable MetricUtils metricUtils) {
    return new HazelcastCoalesceBufferFactory(hazelcastInstance, metricUtils);
  }
}
