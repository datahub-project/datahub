package io.datahubproject.openapi.config;

import io.datahubproject.openapi.delegates.DatahubUsageEventsImpl;
import io.datahubproject.openapi.v2.generated.controller.DatahubUsageEventsApiDelegate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class OpenapiAnalyticsConfig {
  @Bean
  public DatahubUsageEventsApiDelegate datahubUsageEventsApiDelegate() {
    return new DatahubUsageEventsImpl();
  }
}
