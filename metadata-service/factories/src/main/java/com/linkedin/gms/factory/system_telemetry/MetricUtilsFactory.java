package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.UsageAggregationConfiguration;
import com.linkedin.metadata.config.UsageConfiguration;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Slf4j
@Configuration
public class MetricUtilsFactory {
  @Bean
  public MetricUtils metricUtils(
      MeterRegistry meterRegistry, ConfigurationProvider configurationProvider) {
    boolean suppressLegacyRequestCount =
        shouldSuppressLegacyRequestCountMicrometer(configurationProvider);
    if (suppressLegacyRequestCount) {
      log.info(
          "Suppressing legacy per-request datahub_request_count Micrometer export; "
              + "usage aggregation flush owns that meter");
    }
    return MetricUtils.builder()
        .registry(meterRegistry)
        .suppressLegacyRequestCountMicrometer(suppressLegacyRequestCount)
        .build();
  }

  /**
   * Aggregation Micrometer export registers {@code datahub_request_count} with billing tag keys.
   * The legacy per-request counter uses a different tag set ({@code user_category}); suppress it so
   * Micrometer does not reject the flush registration.
   */
  static boolean shouldSuppressLegacyRequestCountMicrometer(
      ConfigurationProvider configurationProvider) {
    if (configurationProvider == null || configurationProvider.getDatahub() == null) {
      return false;
    }
    UsageConfiguration usage = configurationProvider.getDatahub().getUsage();
    if (usage == null || usage.getAggregation() == null) {
      return false;
    }
    UsageAggregationConfiguration aggregation = usage.getAggregation();
    if (!aggregation.isEnabled()) {
      return false;
    }
    UsageAggregationConfiguration.MicrometerExportConfiguration micrometerExport =
        aggregation.getMicrometerExport();
    // Match MicrometerUsageFlushSink: enabled when unset (matchIfMissing=true).
    return micrometerExport == null || micrometerExport.isEnabled();
  }
}
