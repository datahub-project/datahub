package com.linkedin.gms.factory.system_telemetry;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetricsOptions;
import com.linkedin.metadata.utils.metrics.MetricUtils;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.Set;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Adds fixed SLO buckets to the lineage timers so latency distributions and the share of slow
 * requests can be aggregated across pods. Only the configured buckets are published (no percentile
 * histogram), which keeps the series count small.
 */
@Configuration
public class LineageMetricsConfiguration {

  // Fallback when the property is absent; application.yaml carries the same default
  static final String DEFAULT_SLO_SECONDS = "0.1,0.5,1,2,5,10,30,60";

  static final Set<String> LINEAGE_TIMERS =
      Set.of("datahub.lineage.duration", "datahub.lineage.graph_walk.duration");

  @Bean
  public MeterRegistryCustomizer<MeterRegistry> lineageMetricsCustomizer(
      final ConfigurationProvider configurationProvider) {
    final MetricsOptions options =
        configurationProvider.getDatahub().getMetrics().getLineageLatency();
    final String slo =
        options == null || options.getSlo() == null || options.getSlo().isBlank()
            ? DEFAULT_SLO_SECONDS
            : options.getSlo();
    final double[] slosInNanos =
        Arrays.stream(MetricUtils.parseSLOSeconds(slo))
            .map(seconds -> seconds * 1_000_000_000.0)
            .toArray();

    return registry ->
        registry
            .config()
            .meterFilter(
                new MeterFilter() {
                  @Override
                  public DistributionStatisticConfig configure(
                      @Nonnull Meter.Id id, @Nonnull DistributionStatisticConfig config) {
                    if (LINEAGE_TIMERS.contains(id.getName())) {
                      return DistributionStatisticConfig.builder()
                          .serviceLevelObjectives(slosInNanos)
                          .build()
                          .merge(config);
                    }
                    return config;
                  }
                });
  }
}
