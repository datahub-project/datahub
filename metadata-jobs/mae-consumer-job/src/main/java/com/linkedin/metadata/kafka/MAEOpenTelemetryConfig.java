package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.gms.factory.system_telemetry.OpenTelemetryBaseFactory;
import com.linkedin.metadata.event.GenericProducer;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.datahubproject.metadata.context.SystemTelemetryContext;
import io.datahubproject.metadata.context.kafka.SpanProducerRecordResolver;
import io.datahubproject.metadata.context.telemetry.EnrichingSpanProcessor;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
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
      @Autowired(required = false) @Qualifier("dataHubUsageGenericProducer") @Nullable
          GenericProducer<String> usageProducer,
      SpanProducerRecordResolver spanProducerRecordResolver,
      EnrichingSpanProcessor enrichingSpanProcessor) {
    return super.traceContext(
        metricUtils,
        configurationProvider,
        usageProducer,
        spanProducerRecordResolver,
        enrichingSpanProcessor);
  }
}
