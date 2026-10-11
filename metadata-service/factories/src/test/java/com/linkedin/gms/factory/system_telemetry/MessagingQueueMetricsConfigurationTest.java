package com.linkedin.gms.factory.system_telemetry;

import static com.linkedin.metadata.utils.metrics.MetricUtils.MESSAGING_QUEUE_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.MetricsOptions;
import com.linkedin.metadata.config.kafka.ConsumerConfiguration;
import com.linkedin.metadata.config.kafka.KafkaConfiguration;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.micrometer.metrics.autoconfigure.MeterRegistryCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest
public class MessagingQueueMetricsConfigurationTest {

  private ApplicationContextRunner contextRunner;
  private MessagingQueueMetricsConfiguration configuration;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private KafkaConfiguration kafkaConfiguration;
  @Mock private ConsumerConfiguration consumerConfiguration;
  @Mock private MetricsOptions metricsConfiguration;

  private AutoCloseable mockitoCloseable;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);
    configuration = new MessagingQueueMetricsConfiguration();

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(MessagingQueueMetricsConfiguration.class)
            .withBean(ConfigurationProvider.class, () -> configurationProvider);

    // Setup mock chain
    when(configurationProvider.getKafka()).thenReturn(kafkaConfiguration);
    when(kafkaConfiguration.getConsumer()).thenReturn(consumerConfiguration);
    when(consumerConfiguration.getMetrics()).thenReturn(metricsConfiguration);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mockitoCloseable != null) {
      mockitoCloseable.close();
    }
  }

  @Test
  public void testKafkaMetricsCustomizerBeanCreation() {
    contextRunner.run(
        context -> {
          assertThat(context).hasSingleBean(MeterRegistryCustomizer.class);
          @SuppressWarnings("unchecked")
          MeterRegistryCustomizer<MeterRegistry> customizer =
              context.getBean(MeterRegistryCustomizer.class);
          assertThat(customizer).isNotNull();
        });
  }

  @Test
  public void testOtherMetricsNotAffected() {
    when(metricsConfiguration.getSlo()).thenReturn("100,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10000L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer otherTimer = registry.timer("other.metric.name");
    otherTimer.record(100, TimeUnit.MILLISECONDS);

    HistogramSnapshot snapshot = otherTimer.takeSnapshot();
    assertThat(snapshot.percentileValues()).isEmpty();
  }

  @Test
  public void testPrometheusExportExposesHistogramNotClientSideQuantiles() {
    when(metricsConfiguration.getSlo()).thenReturn("300,3600,43200");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(86400L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    PrometheusMeterRegistry registry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);
    timer.record(500, TimeUnit.MILLISECONDS);

    String scrape = registry.scrape();

    assertThat(scrape).contains("messaging_queue_time_seconds_bucket");
    assertThat(scrape).contains("messaging_queue_time_seconds_count");
    assertThat(scrape.lines())
        .filteredOn(
            line ->
                line.startsWith("messaging_queue_time_seconds{")
                    && !line.startsWith("messaging_queue_time_seconds_bucket")
                    && !line.startsWith("messaging_queue_time_seconds_count")
                    && !line.startsWith("messaging_queue_time_seconds_sum")
                    && !line.startsWith("messaging_queue_time_seconds_max"))
        .noneMatch(line -> line.contains("quantile="));
  }

  @Test
  public void testMultipleRegistryTypes() {
    when(metricsConfiguration.getSlo()).thenReturn("100,500,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(30000L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
    customizer.customize(simpleRegistry);
    Timer simpleTimer = simpleRegistry.timer(MESSAGING_QUEUE_TIME);
    simpleTimer.record(250, TimeUnit.MILLISECONDS);
    assertThat(simpleTimer.count()).isEqualTo(1);

    PrometheusMeterRegistry prometheusRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    customizer.customize(prometheusRegistry);
    Timer prometheusTimer = prometheusRegistry.timer(MESSAGING_QUEUE_TIME);
    prometheusTimer.record(250, TimeUnit.MILLISECONDS);
    assertThat(prometheusTimer.count()).isEqualTo(1);
  }

  @Test
  public void testDistributionStatisticConfigMerge() {
    when(metricsConfiguration.getSlo()).thenReturn("100,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(20000L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    registry
        .config()
        .meterFilter(
            new io.micrometer.core.instrument.config.MeterFilter() {
              @Override
              public DistributionStatisticConfig configure(
                  @Nonnull Meter.Id id, @Nonnull DistributionStatisticConfig config) {
                return DistributionStatisticConfig.builder()
                    .percentilesHistogram(false)
                    .build()
                    .merge(config);
              }
            });

    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(50, TimeUnit.MILLISECONDS);

    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.histogramCounts()).isNotEmpty();
  }

  @Test
  public void testLargeMaxExpectedValue() {
    when(metricsConfiguration.getSlo()).thenReturn("1000,10000,100000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(3600000L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(500, TimeUnit.MILLISECONDS);
    timer.record(5000, TimeUnit.MILLISECONDS);
    timer.record(50000, TimeUnit.MILLISECONDS);
    timer.record(500000, TimeUnit.MILLISECONDS);

    assertThat(timer.count()).isEqualTo(4);
    assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(555000);
  }

  @Test
  public void testExpiryAndBufferLength() {
    when(metricsConfiguration.getSlo()).thenReturn("100");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(5000L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    for (int i = 0; i < 100; i++) {
      timer.record(i * 10, TimeUnit.MILLISECONDS);
    }

    assertThat(timer.count()).isEqualTo(100);
  }

  @Test
  public void testNullConfigurationHandling() {
    when(metricsConfiguration.getSlo()).thenReturn(null);
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10000L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void testQueueTimeMetricUsesPercentileHistogram() {
    when(metricsConfiguration.getSlo()).thenReturn("0.1,0.5,1.0,5.0");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(60L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(50, TimeUnit.MILLISECONDS);
    timer.record(150, TimeUnit.MILLISECONDS);
    timer.record(450, TimeUnit.MILLISECONDS);
    timer.record(950, TimeUnit.MILLISECONDS);
    timer.record(1500, TimeUnit.MILLISECONDS);

    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.histogramCounts()).isNotEmpty();
    assertThat(snapshot.count()).isEqualTo(5);
  }

  @Test
  public void testSLOConversion() {
    when(metricsConfiguration.getSlo()).thenReturn("0.1,0.5,1.0");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(50, TimeUnit.MILLISECONDS);
    timer.record(200, TimeUnit.MILLISECONDS);
    timer.record(750, TimeUnit.MILLISECONDS);
    timer.record(2000, TimeUnit.MILLISECONDS);

    assertThat(timer.count()).isEqualTo(4);
  }

  @Test
  public void testMaxExpectedValueConversionFromSecondsToNanoseconds() {
    when(metricsConfiguration.getSlo()).thenReturn("1.0,10.0,100.0");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(3600L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(500, TimeUnit.MILLISECONDS);
    timer.record(5, TimeUnit.SECONDS);
    timer.record(50, TimeUnit.SECONDS);
    timer.record(500, TimeUnit.SECONDS);

    assertThat(timer.count()).isEqualTo(4);
    assertThat(timer.totalTime(TimeUnit.SECONDS)).isGreaterThan(555);
  }

  @Test
  public void testEmptySLOConfiguration() {
    when(metricsConfiguration.getSlo()).thenReturn("");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void testFractionalSecondsInSLO() {
    when(metricsConfiguration.getSlo()).thenReturn("0.05,0.25,0.5,1.5");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(30L);

    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.messagingQueueMetricsCustomizer(configurationProvider);

    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    customizer.customize(registry);

    Timer timer = registry.timer(MESSAGING_QUEUE_TIME);
    timer.record(25, TimeUnit.MILLISECONDS);
    timer.record(75, TimeUnit.MILLISECONDS);
    timer.record(300, TimeUnit.MILLISECONDS);
    timer.record(1000, TimeUnit.MILLISECONDS);
    timer.record(2000, TimeUnit.MILLISECONDS);

    assertThat(timer.count()).isEqualTo(5);
  }
}
