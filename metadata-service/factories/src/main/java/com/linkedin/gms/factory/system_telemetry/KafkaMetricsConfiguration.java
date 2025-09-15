package com.linkedin.gms.factory.system_telemetry;

import static com.linkedin.metadata.utils.metrics.MetricUtils.KAFKA_MESSAGE_QUEUE_TIME;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetricsOptions;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import jakarta.annotation.Nonnull;
import java.time.Duration;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaMetricsConfiguration {

  @Bean
  public MeterRegistryCustomizer<MeterRegistry> kafkaMetricsCustomizer(
      final ConfigurationProvider configurationProvider) {

    return registry -> {
      registry
          .config()
          .meterFilter(
              new MeterFilter() {
                @Override
                public DistributionStatisticConfig configure(
                    @Nonnull Meter.Id id, @Nonnull DistributionStatisticConfig config) {
                  if (id.getName().equals(KAFKA_MESSAGE_QUEUE_TIME)) {

                    MetricsOptions metricOptions =
                        configurationProvider.getKafka().getConsumer().getMetrics();
                    String percentilesConfig = metricOptions.getPercentiles();

                    double[] slosInSeconds = MetricUtils.parseSLOSeconds(metricOptions.getSlo());
                    double[] slosInNanos = new double[slosInSeconds.length];
                    for (int i = 0; i < slosInSeconds.length; i++) {
                      slosInNanos[i] = slosInSeconds[i] * 1_000_000_000.0; // Convert to nanoseconds
                    }

                    return DistributionStatisticConfig.builder()
                        // Use Timer instead of Summary for time measurements
                        .percentilesHistogram(true)
                        .percentiles(MetricUtils.parsePercentiles(percentilesConfig))
                        .serviceLevelObjectives(slosInNanos)
                        .minimumExpectedValue(1_000_000.0) // 1ms in nanoseconds
                        .maximumExpectedValue(
                            Long.valueOf(metricOptions.getMaxExpectedValue()).doubleValue()
                                * 1_000_000_000.0)
                        .expiry(Duration.ofHours(1)) // Stats reset every 1h
                        .bufferLength(24) // 24 hours of history
                        .build()
                        .merge(config);
                  }
                  return config;
                }
              });
    };
  }
}
