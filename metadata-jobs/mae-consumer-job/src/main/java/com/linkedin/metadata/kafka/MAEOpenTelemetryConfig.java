package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.OpenTelemetryBaseFactory;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MAEOpenTelemetryConfig extends OpenTelemetryBaseFactory {

  @Override
  protected String getApplicationComponent() {
    return "datahub-mae-consumer";
  }

  @Bean
  @Override
  protected SystemTelemetryContext traceContext(
      MetricUtils metricUtils,
      ConfigurationProvider configurationProvider,
      @Qualifier("dataHubUsageProducer") Producer<String, String> dueProducer) {
    return super.traceContext(metricUtils, configurationProvider, dueProducer);
  }
}
