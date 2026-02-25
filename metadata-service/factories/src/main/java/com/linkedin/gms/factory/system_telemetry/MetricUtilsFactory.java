package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricUtilsFactory {
  @Bean
  public MetricUtils metricUtils(
      MeterRegistry meterRegistry, ConfigurationProvider configurationProvider) {
    return MetricUtils.builder().registry(meterRegistry).build();
  }
}
