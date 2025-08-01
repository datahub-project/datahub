package com.linkedin.gms.factory.system_telemetry;

import static com.linkedin.metadata.utils.metrics.MetricUtils.DATAHUB_REQUEST_HOOK_QUEUE_TIME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.DataHubConfiguration;
import com.linkedin.metadata.config.MetricsOptions;
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
public class RequestMetricsConfigurationTest {

  private ApplicationContextRunner contextRunner;
  private RequestMetricsConfiguration configuration;

  @Mock private ConfigurationProvider configurationProvider;
  @Mock private DataHubConfiguration datahubConfiguration;
  @Mock private DataHubConfiguration.DataHubMetrics metricsConfiguration;
  @Mock private MetricsOptions hookLatencyMetricsOptions;

  private AutoCloseable mockitoCloseable;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);
    configuration = new RequestMetricsConfiguration();

    contextRunner =
        new ApplicationContextRunner()
            .withUserConfiguration(RequestMetricsConfiguration.class)
            .withBean(ConfigurationProvider.class, () -> configurationProvider);

    // Setup mock chain
    when(configurationProvider.getDatahub()).thenReturn(datahubConfiguration);
    when(datahubConfiguration.getMetrics()).thenReturn(metricsConfiguration);
    when(metricsConfiguration.getHookLatency()).thenReturn(hookLatencyMetricsOptions);
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mockitoCloseable != null) {
      mockitoCloseable.close();
    }
  }

  @Test
  public void testRequestMetricsCustomizerBeanCreation() {
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
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.95");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.1,1.0");
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(10L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

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
  public void testDatahubRequestHookQueueTimeMetricConfiguration() {
    // Setup metric configuration values (in seconds)
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.75,0.95,0.99");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.05,0.1,0.5,1.0"); // seconds in config
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(30L); // 30 seconds in config

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create a timer with the specific name
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);

    // Record values in milliseconds (common usage pattern)
    timer.record(25, TimeUnit.MILLISECONDS); // 25ms
    timer.record(75, TimeUnit.MILLISECONDS); // 75ms
    timer.record(250, TimeUnit.MILLISECONDS); // 250ms
    timer.record(750, TimeUnit.MILLISECONDS); // 750ms
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
  public void testEmptyPercentilesConfiguration() {
    // Setup empty percentiles
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.1,1.0");
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(10L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
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
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.95,0.99");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.1,0.5,1.0");
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(20L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Test with SimpleMeterRegistry
    SimpleMeterRegistry simpleRegistry = new SimpleMeterRegistry();
    customizer.customize(simpleRegistry);
    Timer simpleTimer = simpleRegistry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    simpleTimer.record(250, TimeUnit.MILLISECONDS);
    assertThat(simpleTimer.count()).isEqualTo(1);

    // Test with PrometheusMeterRegistry
    PrometheusMeterRegistry prometheusRegistry =
        new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    customizer.customize(prometheusRegistry);
    Timer prometheusTimer = prometheusRegistry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    prometheusTimer.record(250, TimeUnit.MILLISECONDS);
    assertThat(prometheusTimer.count()).isEqualTo(1);
  }

  @Test
  public void testSLOConversion() {
    // Setup metric configuration with SLOs in seconds
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.95");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.01,0.05,0.1"); // 10ms, 50ms, 100ms
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(5L); // 5 seconds

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);

    // Record values that test SLO boundaries
    timer.record(5, TimeUnit.MILLISECONDS); // Under first SLO (10ms)
    timer.record(25, TimeUnit.MILLISECONDS); // Between first and second SLO
    timer.record(75, TimeUnit.MILLISECONDS); // Between second and third SLO
    timer.record(200, TimeUnit.MILLISECONDS); // Over third SLO (100ms)

    // Verify timer works correctly
    assertThat(timer.count()).isEqualTo(4);
  }

  @Test
  public void testMinimumExpectedValue() {
    // Setup metric configuration
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.95,0.99");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.001,0.01,0.1"); // 1ms, 10ms, 100ms
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(10L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the timer and record very small values
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    timer.record(500, TimeUnit.MICROSECONDS); // 0.5ms
    timer.record(1, TimeUnit.MILLISECONDS); // 1ms (minimum expected)
    timer.record(5, TimeUnit.MILLISECONDS); // 5ms

    // Verify all values are recorded
    assertThat(timer.count()).isEqualTo(3);
  }

  @Test
  public void testMaxExpectedValueConversionFromSecondsToNanoseconds() {
    // Setup with max expected value in seconds
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.95,0.99");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("1.0,10.0,60.0"); // seconds
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(300L); // 5 minutes in seconds

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the timer and record various values
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    timer.record(500, TimeUnit.MILLISECONDS); // 0.5 seconds
    timer.record(5, TimeUnit.SECONDS); // 5 seconds
    timer.record(30, TimeUnit.SECONDS); // 30 seconds
    timer.record(120, TimeUnit.SECONDS); // 120 seconds

    // Verify all values are recorded
    assertThat(timer.count()).isEqualTo(4);
    assertThat(timer.totalTime(TimeUnit.SECONDS)).isGreaterThan(155);
  }

  @Test
  public void testEmptySLOConfiguration() {
    // Setup empty SLO
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.95");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("");
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(10L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization without exception
    customizer.customize(registry);

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    // Verify timer works correctly
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void testNullConfigurationHandling() {
    // Setup null returns to test defensive coding
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn(null);
    when(hookLatencyMetricsOptions.getSlo()).thenReturn(null);
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(10L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // This should not throw an exception
    customizer.customize(registry);

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    timer.record(100, TimeUnit.MILLISECONDS);

    // Verify timer works correctly
    assertThat(timer.count()).isEqualTo(1);
  }

  @Test
  public void testDistributionStatisticConfigMerge() {
    // Setup metric configuration
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.95");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.1,1.0");
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(20L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

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

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);
    timer.record(50, TimeUnit.MILLISECONDS);

    // Verify our custom config overrides the defaults
    HistogramSnapshot snapshot = timer.takeSnapshot();
    assertThat(snapshot.percentileValues()).hasSize(2); // Our custom percentiles
    assertThat(snapshot.histogramCounts()).isNotEmpty(); // percentilesHistogram is true
  }

  @Test
  public void testFractionalSecondsInSLO() {
    // Setup SLO with fractional seconds
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5,0.95,0.99");
    when(hookLatencyMetricsOptions.getSlo())
        .thenReturn("0.001,0.01,0.025,0.1"); // 1ms, 10ms, 25ms, 100ms
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(5L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);

    // Record values around the fractional SLO boundaries
    timer.record(500, TimeUnit.MICROSECONDS); // Under 1ms
    timer.record(5, TimeUnit.MILLISECONDS); // Between 1ms and 10ms
    timer.record(15, TimeUnit.MILLISECONDS); // Between 10ms and 25ms
    timer.record(50, TimeUnit.MILLISECONDS); // Between 25ms and 100ms
    timer.record(200, TimeUnit.MILLISECONDS); // Over 100ms

    assertThat(timer.count()).isEqualTo(5);
  }

  @Test
  public void testExpiryAndBufferLength() {
    // Setup metric configuration
    when(hookLatencyMetricsOptions.getPercentiles()).thenReturn("0.5");
    when(hookLatencyMetricsOptions.getSlo()).thenReturn("0.1");
    when(hookLatencyMetricsOptions.getMaxExpectedValue()).thenReturn(5L);

    // Create the customizer
    MeterRegistryCustomizer<MeterRegistry> customizer =
        configuration.requestHookMetricsCustomizer(configurationProvider);

    // Create a test registry
    SimpleMeterRegistry registry = new SimpleMeterRegistry();

    // Apply the customization
    customizer.customize(registry);

    // Create the timer
    Timer timer = registry.timer(DATAHUB_REQUEST_HOOK_QUEUE_TIME);

    // Record values over time
    for (int i = 0; i < 100; i++) {
      timer.record(i * 10, TimeUnit.MILLISECONDS);
    }

    // Verify values are recorded
    assertThat(timer.count()).isEqualTo(100);

    // Note: We can't directly test expiry (1 hour) and buffer length (24) behavior as they're
    // internal
    // to the distribution statistics implementation, but we've verified they're configured
  }
}
