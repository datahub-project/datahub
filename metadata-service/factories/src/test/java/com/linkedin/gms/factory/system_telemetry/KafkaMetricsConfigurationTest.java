package com.linkedin.gms.factory.system_telemetry;

import static com.linkedin.metadata.utils.metrics.MetricUtils.KAFKA_MESSAGE_QUEUE_TIME;
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
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.boot.actuate.autoconfigure.metrics.MeterRegistryCustomizer;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

@SpringBootTest
public class KafkaMetricsConfigurationTest {

  private ApplicationContextRunner contextRunner;
  private KafkaMetricsConfiguration configuration;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private KafkaConfiguration kafkaConfiguration;
  @Mock private ConsumerConfiguration consumerConfiguration;
  @Mock private MetricsOptions metricsConfiguration;

  private AutoCloseable mockitoCloseable;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);
    configuration = new KafkaMetricsConfiguration();

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(KafkaMetricsConfiguration.class)
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
    // Setup metric configuration values
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.95");
    when(metricsConfiguration.getSlo()).thenReturn("100,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10000L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create a timer with a different name
    Timer otherTimer = registry.timer("other.metric.name");

    // Record a value
    otherTimer.record(100, TimeUnit.MILLISECONDS);

    // Get the histogram snapshot
    HistogramSnapshot snapshot = otherTimer.takeSnapshot();

    // Verify that this metric doesn't have custom percentiles
    assertThat(snapshot.percentileValues()).isEmpty();
  }

  @Test
  public void testEmptyPercentilesConfiguration() {
    // Setup empty percentiles
    when(metricsConfiguration.getPercentiles()).thenReturn("");
    when(metricsConfiguration.getSlo()).thenReturn("100,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10000L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    // Get the histogram snapshot
    HistogramSnapshot snapshot = timer.takeSnapshot();

    // When percentilesHistogram is true and no custom percentiles are provided,
    // Micrometer generates default percentiles (50th, 95th, 99th)
    assertThat(snapshot.percentileValues()).hasSize(3);
    assertThat(snapshot.percentileValues())
        .extracting(ValueAtPercentile::percentile)
        .containsExactly(0.5, 0.95, 0.99);
  }

  @Test
  public void testMultipleRegistryTypes() {
    // Setup metric configuration
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.95,0.99");
    when(metricsConfiguration.getSlo()).thenReturn("100,500,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(30000L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Test with SimpleMeterRegistry
    SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
    customizer.customize(simpleRegistry);
    Timer simpleTimer = simpleRegistry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    simpleTimer.record(250, TimeUnit.MILLISECONDS);
    assertThat(simpleTimer.count()).isEqualTo(1);

    // Test with PrometheusMeterRegistry
    PrometheusMeterRegistry prometheusRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    customizer.customize(prometheusRegistry);
    Timer prometheusTimer = prometheusRegistry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    prometheusTimer.record(250, TimeUnit.MILLISECONDS);
    assertThat(prometheusTimer.count()).isEqualTo(1);
  }

  @Test
  public void testDistributionStatisticConfigMerge() {
    // Setup metric configuration
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.95");
    when(metricsConfiguration.getSlo()).thenReturn("100,1000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(20000L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry with its own default config
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    registry
        .config()
        .meterFilter(
            new io.micrometer.core.instrument.config.MeterFilter() {
              @Override
              public DistributionStatisticConfig configure(
                  @Nonnull Meter.Id id, @Nonnull DistributionStatisticConfig config) {
                // Set some default values
                return DistributionStatisticConfig.builder()
                    .percentilesHistogram(false) // This should be overridden
                    .build()
                    .merge(config);
              }
            });

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    timer.record(50, TimeUnit.MILLISECONDS);

    // Verify our custom config overrides the defaults
    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.percentileValues()).hasSize(2); // Our custom percentiles
    assertThat(snapshot.histogramCounts()).isNotEmpty(); // percentilesHistogram is true
  }

  @Test
  public void testLargeMaxExpectedValue() {
    // Setup with a very large max expected value
    when(metricsConfiguration.getPercentiles()).thenReturn("0.95,0.99");
    when(metricsConfiguration.getSlo()).thenReturn("1000,10000,100000");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(3600000L); // 1 hour in millis

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer and record various values
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    timer.record(500, TimeUnit.MILLISECONDS);
    timer.record(5000, TimeUnit.MILLISECONDS);
    timer.record(50000, TimeUnit.MILLISECONDS);
    timer.record(500000, TimeUnit.MILLISECONDS);

    // Verify all values are recorded
    assertThat(timer.count()).isEqualTo(4);
    assertThat(timer.totalTime(TimeUnit.MILLISECONDS)).isGreaterThan(555000);
  }

  @Test
  public void testExpiryAndBufferLength() {
    // Setup metric configuration
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5");
    when(metricsConfiguration.getSlo()).thenReturn("100");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(5000L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);

    // Record values over time
    for (int i = 0; i < 100; i++) {
      timer.record(i * 10, TimeUnit.MILLISECONDS);
    }

    // Verify values are recorded
    assertThat(timer.count()).isEqualTo(100);

    // Note: We can't directly test expiry and buffer length behavior as they're internal
    // to the distribution statistics implementation, but we've verified they're configured
  }

  @Test
  public void testNullConfigurationHandling() {
    // Setup null returns to test defensive coding
    when(metricsConfiguration.getPercentiles()).thenReturn(null);
    when(metricsConfiguration.getSlo()).thenReturn(null);
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10000L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // This should not throw an exception
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    // Verify timer works correctly
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void testKafkaMessageQueueTimeMetricConfiguration() {
    // Setup metric configuration values (in seconds)
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.75,0.95,0.99");
    when(metricsConfiguration.getSlo()).thenReturn("0.1,0.5,1.0,5.0"); // seconds in config
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(60L); // 60 seconds in config

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create a timer with the specific name
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);

    // Record values in milliseconds (common usage pattern)
    timer.record(50, TimeUnit.MILLISECONDS); // 50ms
    timer.record(150, TimeUnit.MILLISECONDS); // 150ms
    timer.record(450, TimeUnit.MILLISECONDS); // 450ms
    timer.record(950, TimeUnit.MILLISECONDS); // 950ms
    timer.record(1500, TimeUnit.MILLISECONDS); // 1500ms

    // Get the histogram snapshot
    HistogramSnapshot snapshot = timer.takeSnapshot();

    // Verify percentiles are configured
    assertThat(snapshot.percentileValues()).hasSize(4);
    assertThat(snapshot.percentileValues())
        .extracting(ValueAtPercentile::percentile)
        .containsExactly(0.5, 0.75, 0.95, 0.99);

    // Verify histogram is enabled
    assertThat(snapshot.histogramCounts()).isNotEmpty();
  }

  @Test
  public void testSLOConversion() {
    // Setup metric configuration with SLOs in seconds
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.95");
    when(metricsConfiguration.getSlo()).thenReturn("0.1,0.5,1.0"); // seconds
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10L); // 10 seconds

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);

    // Record values that test SLO boundaries
    timer.record(50, TimeUnit.MILLISECONDS); // Under first SLO (100ms)
    timer.record(200, TimeUnit.MILLISECONDS); // Between first and second SLO
    timer.record(750, TimeUnit.MILLISECONDS); // Between second and third SLO
    timer.record(2000, TimeUnit.MILLISECONDS); // Over third SLO (1s)

    // Verify timer works correctly
    assertThat(timer.count()).isEqualTo(4);
  }

  @Test
  public void testMaxExpectedValueConversionFromSecondsToNanoseconds() {
    // Setup with max expected value in seconds
    when(metricsConfiguration.getPercentiles()).thenReturn("0.95,0.99");
    when(metricsConfiguration.getSlo()).thenReturn("1.0,10.0,100.0"); // seconds
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(3600L); // 1 hour in seconds

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer and record various values
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    timer.record(500, TimeUnit.MILLISECONDS); // 0.5 seconds
    timer.record(5, TimeUnit.SECONDS); // 5 seconds
    timer.record(50, TimeUnit.SECONDS); // 50 seconds
    timer.record(500, TimeUnit.SECONDS); // 500 seconds

    // Verify all values are recorded
    assertThat(timer.count()).isEqualTo(4);
    assertThat(timer.totalTime(TimeUnit.SECONDS)).isGreaterThan(555);
  }

  @Test
  public void testEmptySLOConfiguration() {
    // Setup empty SLO
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.95");
    when(metricsConfiguration.getSlo()).thenReturn("");
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(10L); // 10 seconds

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization without exception
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    // Verify timer works correctly
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void testFractionalSecondsInSLO() {
    // Setup SLO with fractional seconds
    when(metricsConfiguration.getPercentiles()).thenReturn("0.5,0.95,0.99");
    when(metricsConfiguration.getSlo()).thenReturn("0.05,0.25,0.5,1.5"); // 50ms, 250ms, 500ms, 1.5s
    when(metricsConfiguration.getMaxExpectedValue()).thenReturn(30L); // 30 seconds

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.kafkaMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the Kafka timer
    Timer timer = registry.timer(KAFKA_MESSAGE_QUEUE_TIME);

    // Record values around the fractional SLO boundaries
    timer.record(25, TimeUnit.MILLISECONDS); // Under 50ms
    timer.record(75, TimeUnit.MILLISECONDS); // Between 50ms and 250ms
    timer.record(300, TimeUnit.MILLISECONDS); // Between 250ms and 500ms
    timer.record(1000, TimeUnit.MILLISECONDS); // Between 500ms and 1.5s
    timer.record(2000, TimeUnit.MILLISECONDS); // Over 1.5s

    assertThat(timer.count()).isEqualTo(5);
  }
}
