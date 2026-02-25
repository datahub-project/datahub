package com.linkedin.gms.factory.config;

import com.linkedin.gms.factory.system_telemetry.OpenTelemetryBaseFactory;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class GMSOpenTelemetryConfig extends OpenTelemetryBaseFactory {

  @Override
  protected String getApplicationComponent() {
    return "datahub-gms";
  }

  @Bean
  @Override
  protected SystemTelemetryContext traceContext(
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider,
      @Qualifier("dataHubUsageEventProducer") GenericProducer<String> dueProducer) {
    return super.traceContext(metricUtils, configurationProvider, dueProducer);
  }
}
