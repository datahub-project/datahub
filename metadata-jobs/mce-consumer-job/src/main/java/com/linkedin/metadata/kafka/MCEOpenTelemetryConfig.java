package com.linkedin.metadata.kafka;

import com.linkedin.gms.factory.system_telemetry.OpenTelemetryBaseFactory;
import io.datahubproject.metadata.context.TraceContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MCEOpenTelemetryConfig extends OpenTelemetryBaseFactory {

  @Override
  protected String getApplicationComponent() {
    return "datahub-mce-consumer";
  }

  @Bean
  @Override
  protected TraceContext traceContext() {
    return super.traceContext();
  }
}
