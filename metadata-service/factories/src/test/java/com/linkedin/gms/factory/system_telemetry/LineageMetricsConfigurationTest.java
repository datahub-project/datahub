package com.linkedin.gms.factory.system_telemetry;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.MetricsOptions;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.Test;

public class LineageMetricsConfigurationTest {

  private static PrometheusMeterRegistry registryWithSlo(String slo) {
    MetricsOptions options = new MetricsOptions();
    options.setSlo(slo);
    DataHubConfiguration.DataHubMetrics metrics = new DataHubConfiguration.DataHubMetrics();
    metrics.setLineageLatency(options);
    DataHubConfiguration datahub = mock(DataHubConfiguration.class);
    when(datahub.getMetrics()).thenReturn(metrics);
    ConfigurationProvider configurationProvider = mock(ConfigurationProvider.class);
    when(configurationProvider.getDatahub()).thenReturn(datahub);

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    new LineageMetricsConfiguration()
        .lineageMetricsCustomizer(configurationProvider)
        .customize(registry);
    return registry;
  }

  @Test
  public void testLineageTimersPublishOnlyConfiguredBuckets() {
    PrometheusMeterRegistry registry = registryWithSlo("1,5,30");
    Timer.builder("datahub.lineage.duration")
        .tag("kind", "impact")
        .register(registry)
        .record(3, TimeUnit.SECONDS);
    Timer.builder("datahub.lineage.graph_walk.duration")
        .register(registry)
        .record(40, TimeUnit.SECONDS);

    String scrape = registry.scrape();
    assertThat(scrape)
        .contains("datahub_lineage_duration_seconds_bucket{kind=\"impact\",le=\"1.0\"} 0")
        .contains("datahub_lineage_duration_seconds_bucket{kind=\"impact\",le=\"5.0\"} 1")
        .contains("datahub_lineage_graph_walk_duration_seconds_bucket{le=\"30.0\"} 0")
        .contains("datahub_lineage_graph_walk_duration_seconds_bucket{le=\"+Inf\"} 1");
    // Three SLOs plus +Inf per timer, no percentile histogram
    assertThat(scrape.lines().filter(l -> l.startsWith("datahub_lineage_duration_seconds_bucket")))
        .hasSize(4);
  }

  @Test
  public void testOtherTimersGetNoBuckets() {
    PrometheusMeterRegistry registry = registryWithSlo("1,5,30");
    Timer.builder("datahub.cascade.duration").register(registry).record(3, TimeUnit.SECONDS);

    assertThat(registry.scrape()).doesNotContain("datahub_cascade_duration_seconds_bucket");
  }

  @Test
  public void testMissingConfigFallsBackToDefaultBuckets() {
    PrometheusMeterRegistry registry = registryWithSlo(null);
    Timer.builder("datahub.lineage.duration").register(registry).record(3, TimeUnit.SECONDS);

    assertThat(registry.scrape().lines().filter(l -> l.contains("_bucket"))).hasSize(9);
  }
}
