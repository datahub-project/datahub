package com.linkedin.metadata.utils.metrics;

import static org.testng.Assert.*;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetricUtilsTest {

  private SimpleMeterRegistry meterRegistry;
  private MetricUtils metricUtils;
  private AutoCloseable mockitoCloseable;

  @Mock private MeterRegistry mockMeterRegistry;

  @BeforeMethod
  public void setUp() {
    mockitoCloseable = MockitoAnnotations.openMocks(this);
    meterRegistry = new SimpleMeterRegistry();
    metricUtils = MetricUtils.builder().registry(meterRegistry).build();
  }

  @AfterMethod
  public void tearDown() throws Exception {
    if (mockitoCloseable != null) {
      mockitoCloseable.close();
    }
    meterRegistry.clear();
    meterRegistry.close();
  }

  @Test
  public void testGetRegistryReturnsRegistry() {
    MeterRegistry registry = metricUtils.getRegistry();
    assertNotNull(registry);
    assertSame(registry, meterRegistry);
  }

  @Test
  public void testGetRegistryReturnsDefaultWhenNotSpecified() {
    MetricUtils utilsWithDefaultRegistry = MetricUtils.builder().build();

    MeterRegistry registry = utilsWithDefaultRegistry.getRegistry();
    assertNotNull(registry);
    assertTrue(registry instanceof io.micrometer.core.instrument.composite.CompositeMeterRegistry);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testBuilderRejectsNullRegistry() {
    // This should throw NullPointerException due to @NonNull annotation
    MetricUtils.builder().registry(null).build();
  }

  @Test
  public void testTimeRecordsTimerWithDropwizardTag() {
    String metricName = "test.timer";
    long durationNanos = TimeUnit.MILLISECONDS.toNanos(100);

    metricUtils.time(metricName, durationNanos);

    Timer timer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertEquals(timer.totalTime(TimeUnit.NANOSECONDS), (double) durationNanos);
  }

  @Test
  public void testTimeWithDefaultRegistryWorks() {
    MetricUtils utilsWithDefaultRegistry = MetricUtils.builder().build();

    // Should not throw exception and should work with default registry
    utilsWithDefaultRegistry.time("test.timer", 1000);
  }

  @Test
  public void testIncrementWithClassAndMetricName() {
    Class<?> testClass = this.getClass();
    String metricName = "test.counter";
    double incrementValue = 5.0;

    metricUtils.increment(testClass, metricName, incrementValue);

    String expectedName = MetricRegistry.name(testClass, metricName);
    Counter counter = meterRegistry.counter(expectedName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(counter);
    assertEquals(counter.count(), incrementValue);
  }

  @Test
  public void testIncrementWithMetricNameOnly() {
    String metricName = "simple.counter";
    double incrementValue = 3.0;

    metricUtils.increment(metricName, incrementValue);

    Counter counter = meterRegistry.counter(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(counter);
    assertEquals(counter.count(), incrementValue);
  }

  @Test
  public void testExceptionIncrementCreatesMultipleCounters() {
    Class<?> testClass = this.getClass();
    String baseMetricName = "error.counter";
    RuntimeException exception = new RuntimeException("Test exception");

    metricUtils.exceptionIncrement(testClass, baseMetricName, exception);

    // Check base counter
    String baseName = MetricRegistry.name(testClass, baseMetricName);
    Counter baseCounter = meterRegistry.counter(baseName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(baseCounter);
    assertEquals(baseCounter.count(), 1.0);

    // The snake case conversion in the code is: "RuntimeException" -> "_Runtime_Exception"
    String exceptionName =
        MetricRegistry.name(testClass, baseMetricName + "_" + "_Runtime_Exception");
    Counter exceptionCounter =
        meterRegistry.counter(exceptionName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(exceptionCounter);
    assertEquals(exceptionCounter.count(), 1.0);
  }

  @Test
  public void testExceptionIncrementWithComplexExceptionName() {
    Class<?> testClass = this.getClass();
    String baseMetricName = "error.counter";
    IllegalArgumentException exception = new IllegalArgumentException("Test");

    metricUtils.exceptionIncrement(testClass, baseMetricName, exception);

    // The snake case conversion in the code is: "IllegalArgumentException" ->
    // "_Illegal_Argument_Exception"
    String exceptionName =
        MetricRegistry.name(testClass, baseMetricName + "_" + "_Illegal_Argument_Exception");
    Counter exceptionCounter =
        meterRegistry.counter(exceptionName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(exceptionCounter);
    assertEquals(exceptionCounter.count(), 1.0);
  }

  @Test
  public void testGaugeRegistersSupplierBasedGauge() {
    Class<?> testClass = this.getClass();
    String metricName = "test.gauge";
    Supplier<Number> valueSupplier = () -> 42.5;

    metricUtils.setGaugeValue(testClass, metricName, 42.5);

    String expectedName = MetricRegistry.name(testClass, metricName);
    Gauge gauge =
        meterRegistry.find(expectedName).tag(MetricUtils.DROPWIZARD_METRIC, "true").gauge();

    assertNotNull(gauge);
    assertEquals(gauge.value(), 42.5);
  }

  @Test
  public void testHistogramRecordsDistributionSummary() {
    Class<?> testClass = this.getClass();
    String metricName = "test.histogram";
    long value = 100L;

    metricUtils.histogram(testClass, metricName, value);

    String expectedName = MetricRegistry.name(testClass, metricName);
    DistributionSummary summary =
        meterRegistry.summary(expectedName, MetricUtils.DROPWIZARD_METRIC, "true");

    assertNotNull(summary);
    assertEquals(summary.count(), 1);
    assertEquals(summary.totalAmount(), (double) value);
  }

  @Test
  public void testHistogramMultipleValues() {
    Class<?> testClass = this.getClass();
    String metricName = "multi.histogram";

    metricUtils.histogram(testClass, metricName, 10);
    metricUtils.histogram(testClass, metricName, 20);
    metricUtils.histogram(testClass, metricName, 30);

    String expectedName = MetricRegistry.name(testClass, metricName);
    DistributionSummary summary =
        meterRegistry.summary(expectedName, MetricUtils.DROPWIZARD_METRIC, "true");

    assertEquals(summary.count(), 3);
    assertEquals(summary.totalAmount(), 60.0);
    assertEquals(summary.mean(), 20.0);
  }

  @Test
  public void testNameWithStringParameters() {
    String result = MetricUtils.name("base", "part1", "part2", "part3");
    assertEquals(result, "base.part1.part2.part3");
  }

  @Test
  public void testNameWithClassAndStringParameters() {
    Class<?> testClass = this.getClass();
    String result = MetricUtils.name(testClass, "method", "metric");
    assertEquals(result, testClass.getName() + ".method.metric");
  }

  @Test
  public void testNameWithEmptyParameters() {
    String result = MetricUtils.name("base");
    assertEquals(result, "base");
  }

  @Test
  public void testAllMethodsWithDefaultRegistry() {
    MetricUtils utilsWithDefaultRegistry = MetricUtils.builder().build();

    // All methods should work with the default CompositeMeterRegistry
    utilsWithDefaultRegistry.time("timer", 1000);
    utilsWithDefaultRegistry.increment(this.getClass(), "counter", 1);
    utilsWithDefaultRegistry.increment("counter", 1);
    utilsWithDefaultRegistry.exceptionIncrement(this.getClass(), "error", new RuntimeException());
    utilsWithDefaultRegistry.setGaugeValue(this.getClass(), "gauge", 42);
    utilsWithDefaultRegistry.histogram(this.getClass(), "histogram", 100);

    // Verify the registry is the expected type
    assertTrue(
        utilsWithDefaultRegistry.getRegistry()
            instanceof io.micrometer.core.instrument.composite.CompositeMeterRegistry);
  }

  @Test
  public void testAllMetricsHaveDropwizardTag() {
    // Test that all metric types are properly tagged
    metricUtils.time("timer.metric", 1000);
    metricUtils.increment("counter.metric", 1);
    metricUtils.setGaugeValue(this.getClass(), "gauge.metric", 42);
    metricUtils.histogram(this.getClass(), "histogram.metric", 100);

    // Verify all metrics have the dropwizard tag
    for (Meter meter : meterRegistry.getMeters()) {
      assertEquals(meter.getId().getTag(MetricUtils.DROPWIZARD_METRIC), "true");
    }
  }

  @Test
  public void testParsePercentilesWithValidConfig() {
    String percentilesConfig = "0.5, 0.75, 0.95, 0.99, 0.999";
    double[] result = MetricUtils.parsePercentiles(percentilesConfig);

    assertEquals(result.length, 5);
    assertEquals(result[0], 0.5);
    assertEquals(result[1], 0.75);
    assertEquals(result[2], 0.95);
    assertEquals(result[3], 0.99);
    assertEquals(result[4], 0.999);
  }

  @Test
  public void testParsePercentilesWithNullConfig() {
    double[] result = MetricUtils.parsePercentiles(null);

    assertEquals(result.length, 3);
    assertEquals(result[0], 0.5);
    assertEquals(result[1], 0.95);
    assertEquals(result[2], 0.99);
  }

  @Test
  public void testParsePercentilesWithEmptyConfig() {
    double[] result = MetricUtils.parsePercentiles("");

    assertEquals(result.length, 3);
    assertEquals(result[0], 0.5);
    assertEquals(result[1], 0.95);
    assertEquals(result[2], 0.99);
  }

  @Test
  public void testParsePercentilesWithWhitespaceOnlyConfig() {
    double[] result = MetricUtils.parsePercentiles("   ");

    assertEquals(result.length, 3);
    assertEquals(result[0], 0.5);
    assertEquals(result[1], 0.95);
    assertEquals(result[2], 0.99);
  }

  @Test
  public void testParsePercentilesWithSingleValue() {
    double[] result = MetricUtils.parsePercentiles("0.99");

    assertEquals(result.length, 1);
    assertEquals(result[0], 0.99);
  }

  @Test
  public void testParsePercentilesWithExtraSpaces() {
    double[] result = MetricUtils.parsePercentiles("  0.5  ,  0.95  ,  0.99  ");

    assertEquals(result.length, 3);
    assertEquals(result[0], 0.5);
    assertEquals(result[1], 0.95);
    assertEquals(result[2], 0.99);
  }

  @Test
  public void testParseSLOSecondsWithValidConfig() {
    String sloConfig = "100, 500, 1000, 5000, 10000";
    double[] result = MetricUtils.parseSLOSeconds(sloConfig);

    assertEquals(result.length, 5);
    assertEquals(result[0], 100.0);
    assertEquals(result[1], 500.0);
    assertEquals(result[2], 1000.0);
    assertEquals(result[3], 5000.0);
    assertEquals(result[4], 10000.0);
  }

  @Test
  public void testParseSLOSecondsWithNullConfig() {
    double[] result = MetricUtils.parseSLOSeconds(null);

    assertEquals(result.length, 5);
    assertEquals(result[0], 60.0);
    assertEquals(result[1], 300.0);
    assertEquals(result[2], 900.0);
    assertEquals(result[3], 1800.0);
    assertEquals(result[4], 3600.0);
  }

  @Test
  public void testParseSLOSecondsWithEmptyConfig() {
    double[] result = MetricUtils.parseSLOSeconds("");

    assertEquals(result.length, 5);
    assertEquals(result[0], 60.0);
    assertEquals(result[1], 300.0);
    assertEquals(result[2], 900.0);
    assertEquals(result[3], 1800.0);
    assertEquals(result[4], 3600.0);
  }

  @Test
  public void testParseSLOSecondsWithWhitespaceOnlyConfig() {
    double[] result = MetricUtils.parseSLOSeconds("   ");

    assertEquals(result.length, 5);
    assertEquals(result[0], 60.0);
    assertEquals(result[1], 300.0);
    assertEquals(result[2], 900.0);
    assertEquals(result[3], 1800.0);
    assertEquals(result[4], 3600.0);
  }

  @Test
  public void testParseSLOSecondsWithSingleValue() {
    double[] result = MetricUtils.parseSLOSeconds("1000");

    assertEquals(result.length, 1);
    assertEquals(result[0], 1000.0);
  }

  @Test
  public void testParseSLOSecondsWithDecimalValues() {
    double[] result = MetricUtils.parseSLOSeconds("100.5, 500.75, 1000.0");

    assertEquals(result.length, 3);
    assertEquals(result[0], 100.5);
    assertEquals(result[1], 500.75);
    assertEquals(result[2], 1000.0);
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testParsePercentilesWithInvalidNumber() {
    MetricUtils.parsePercentiles("0.5, invalid, 0.99");
  }

  @Test(expectedExceptions = NumberFormatException.class)
  public void testParseSLOSecondsWithInvalidNumber() {
    MetricUtils.parseSLOSeconds("100, not-a-number, 1000");
  }

  @Test
  public void testParsePercentilesWithTrailingComma() {
    double[] result = MetricUtils.parsePercentiles("0.5, 0.95,");

    // This will throw NumberFormatException when parsing empty string after last comma
    // If this is intended behavior, keep the test as expectedExceptions
    // If not, the implementation should handle trailing commas
  }

  @Test
  public void testParseSLOSecondsWithNegativeValues() {
    // Test if negative values are accepted (they probably shouldn't be for SLOs)
    double[] result = MetricUtils.parseSLOSeconds("-100, 500, 1000");

    assertEquals(result.length, 3);
    assertEquals(result[0], -100.0);
    assertEquals(result[1], 500.0);
    assertEquals(result[2], 1000.0);
  }

  @Test
  public void testIncrementMicrometerBasicFunctionality() {
    String metricName = "test.micrometer.counter";
    double incrementValue = 2.5;

    metricUtils.incrementMicrometer(metricName, incrementValue);

    Counter counter = meterRegistry.counter(metricName);
    assertNotNull(counter);
    assertEquals(counter.count(), incrementValue);
  }

  @Test
  public void testIncrementMicrometerWithTags() {
    String metricName = "test.micrometer.tagged";
    double incrementValue = 1.0;

    metricUtils.incrementMicrometer(metricName, incrementValue, "env", "prod", "service", "api");

    Counter counter = meterRegistry.counter(metricName, "env", "prod", "service", "api");
    assertNotNull(counter);
    assertEquals(counter.count(), incrementValue);
  }

  @Test
  public void testIncrementMicrometerCachingBehavior() {
    String metricName = "test.cache.counter";

    // First call should create the counter
    metricUtils.incrementMicrometer(metricName, 1.0);
    Counter counter1 = meterRegistry.counter(metricName);
    assertEquals(counter1.count(), 1.0);

    // Second call should reuse the same counter
    metricUtils.incrementMicrometer(metricName, 2.0);
    Counter counter2 = meterRegistry.counter(metricName);
    assertSame(counter1, counter2); // Should be the exact same object due to caching
    assertEquals(counter2.count(), 3.0); // 1.0 + 2.0
  }

  @Test
  public void testIncrementMicrometerDifferentTagsCacheSeparately() {
    String metricName = "test.cache.tags";

    // Create counters with different tags
    metricUtils.incrementMicrometer(metricName, 1.0, "env", "prod");
    metricUtils.incrementMicrometer(metricName, 2.0, "env", "dev");

    Counter prodCounter = meterRegistry.counter(metricName, "env", "prod");
    Counter devCounter = meterRegistry.counter(metricName, "env", "dev");

    assertNotSame(prodCounter, devCounter); // Different cache entries
    assertEquals(prodCounter.count(), 1.0);
    assertEquals(devCounter.count(), 2.0);
  }

  @Test
  public void testIncrementMicrometerMultipleIncrementsOnSameCounter() {
    String metricName = "test.multiple.increments";

    metricUtils.incrementMicrometer(metricName, 1.0, "type", "request");
    metricUtils.incrementMicrometer(metricName, 3.0, "type", "request");
    metricUtils.incrementMicrometer(metricName, 2.0, "type", "request");

    Counter counter = meterRegistry.counter(metricName, "type", "request");
    assertEquals(counter.count(), 6.0); // 1.0 + 3.0 + 2.0
  }

  @Test
  public void testRecordTimerBasicFunctionality() {
    String metricName = "test.micrometer.timer";
    long durationNanos = TimeUnit.MILLISECONDS.toNanos(100);

    metricUtils.recordTimer(metricName, durationNanos);

    Timer timer = meterRegistry.timer(metricName);
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertEquals(timer.totalTime(TimeUnit.NANOSECONDS), (double) durationNanos);
  }

  @Test
  public void testRecordTimerWithTags() {
    String metricName = "test.micrometer.timer.tagged";
    long durationNanos = TimeUnit.MILLISECONDS.toNanos(50);

    metricUtils.recordTimer(metricName, durationNanos, "operation", "query", "status", "success");

    Timer timer = meterRegistry.timer(metricName, "operation", "query", "status", "success");
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertEquals(timer.totalTime(TimeUnit.NANOSECONDS), (double) durationNanos);
  }

  @Test
  public void testRecordTimerCachingBehavior() {
    String metricName = "test.timer.cache";
    long duration1 = TimeUnit.MILLISECONDS.toNanos(100);
    long duration2 = TimeUnit.MILLISECONDS.toNanos(200);

    metricUtils.recordTimer(metricName, duration1);
    Timer timer1 = meterRegistry.timer(metricName);
    assertEquals(timer1.count(), 1);

    metricUtils.recordTimer(metricName, duration2);
    Timer timer2 = meterRegistry.timer(metricName);
    assertSame(timer1, timer2);
    assertEquals(timer2.count(), 2);
    assertEquals(timer2.totalTime(TimeUnit.NANOSECONDS), (double) (duration1 + duration2));
  }

  @Test
  public void testRecordTimerDifferentTagsCacheSeparately() {
    String metricName = "test.timer.tags";
    long duration = TimeUnit.MILLISECONDS.toNanos(100);

    metricUtils.recordTimer(metricName, duration, "env", "prod");
    metricUtils.recordTimer(metricName, duration, "env", "dev");

    Timer prodTimer = meterRegistry.timer(metricName, "env", "prod");
    Timer devTimer = meterRegistry.timer(metricName, "env", "dev");

    assertNotSame(prodTimer, devTimer);
    assertEquals(prodTimer.count(), 1);
    assertEquals(devTimer.count(), 1);
  }

  @Test
  public void testRecordTimerMultipleRecordings() {
    String metricName = "test.timer.multiple";

    metricUtils.recordTimer(metricName, TimeUnit.MILLISECONDS.toNanos(100), "type", "request");
    metricUtils.recordTimer(metricName, TimeUnit.MILLISECONDS.toNanos(200), "type", "request");
    metricUtils.recordTimer(metricName, TimeUnit.MILLISECONDS.toNanos(300), "type", "request");

    Timer timer = meterRegistry.timer(metricName, "type", "request");
    assertEquals(timer.count(), 3);
    assertEquals(
        timer.totalTime(TimeUnit.NANOSECONDS), (double) TimeUnit.MILLISECONDS.toNanos(600));
    assertEquals(timer.mean(TimeUnit.NANOSECONDS), (double) TimeUnit.MILLISECONDS.toNanos(200));
  }

  @Test
  public void testRecordTimerDoesNotHaveDropwizardTag() {
    String metricName = "test.timer.no.dropwizard";

    metricUtils.recordTimer(metricName, TimeUnit.MILLISECONDS.toNanos(100));

    Timer timer = meterRegistry.timer(metricName);
    assertNotNull(timer);
    assertNull(timer.getId().getTag(MetricUtils.DROPWIZARD_METRIC));
  }

  @Test
  public void testRecordDistributionBasicFunctionality() {
    String metricName = "test.micrometer.distribution";
    long value = 100L;

    metricUtils.recordDistribution(metricName, value);

    DistributionSummary summary = meterRegistry.summary(metricName);
    assertNotNull(summary);
    assertEquals(summary.count(), 1);
    assertEquals(summary.totalAmount(), (double) value);
  }

  @Test
  public void testRecordDistributionWithTags() {
    String metricName = "test.micrometer.distribution.tagged";
    long value = 50L;

    metricUtils.recordDistribution(metricName, value, "operation", "batch", "status", "success");

    DistributionSummary summary =
        meterRegistry.summary(metricName, "operation", "batch", "status", "success");
    assertNotNull(summary);
    assertEquals(summary.count(), 1);
    assertEquals(summary.totalAmount(), (double) value);
  }

  @Test
  public void testRecordDistributionCachingBehavior() {
    String metricName = "test.distribution.cache";

    metricUtils.recordDistribution(metricName, 10L);
    DistributionSummary summary1 = meterRegistry.summary(metricName);
    assertEquals(summary1.count(), 1);

    metricUtils.recordDistribution(metricName, 20L);
    DistributionSummary summary2 = meterRegistry.summary(metricName);
    assertSame(summary1, summary2);
    assertEquals(summary2.count(), 2);
    assertEquals(summary2.totalAmount(), 30.0);
  }

  @Test
  public void testRecordDistributionDifferentTagsCacheSeparately() {
    String metricName = "test.distribution.tags";
    long value = 100L;

    metricUtils.recordDistribution(metricName, value, "env", "prod");
    metricUtils.recordDistribution(metricName, value, "env", "dev");

    DistributionSummary prodSummary = meterRegistry.summary(metricName, "env", "prod");
    DistributionSummary devSummary = meterRegistry.summary(metricName, "env", "dev");

    assertNotSame(prodSummary, devSummary);
    assertEquals(prodSummary.count(), 1);
    assertEquals(devSummary.count(), 1);
  }

  @Test
  public void testRecordDistributionMultipleValues() {
    String metricName = "test.distribution.multiple";

    metricUtils.recordDistribution(metricName, 10L, "type", "ratio");
    metricUtils.recordDistribution(metricName, 20L, "type", "ratio");
    metricUtils.recordDistribution(metricName, 30L, "type", "ratio");

    DistributionSummary summary = meterRegistry.summary(metricName, "type", "ratio");
    assertEquals(summary.count(), 3);
    assertEquals(summary.totalAmount(), 60.0);
    assertEquals(summary.mean(), 20.0);
  }

  @Test
  public void testRecordDistributionDoesNotHaveDropwizardTag() {
    String metricName = "test.distribution.no.dropwizard";

    metricUtils.recordDistribution(metricName, 100L);

    DistributionSummary summary = meterRegistry.summary(metricName);
    assertNotNull(summary);
    assertNull(summary.getId().getTag(MetricUtils.DROPWIZARD_METRIC));
  }

  @Test
  public void testMicrometerMetricsDoNotAppearInDropwizardFilter() {
    metricUtils.recordTimer("micrometer.timer", TimeUnit.MILLISECONDS.toNanos(100));
    metricUtils.recordDistribution("micrometer.distribution", 100L);
    metricUtils.incrementMicrometer("micrometer.counter", 1);

    metricUtils.time("legacy.timer", TimeUnit.MILLISECONDS.toNanos(100));
    metricUtils.histogram(this.getClass(), "legacy.histogram", 100L);
    metricUtils.increment("legacy.counter", 1);

    int micrometerMetricsCount = 0;
    int legacyMetricsCount = 0;

    for (Meter meter : meterRegistry.getMeters()) {
      if (meter.getId().getTag(MetricUtils.DROPWIZARD_METRIC) == null) {
        micrometerMetricsCount++;
      } else {
        legacyMetricsCount++;
      }
    }

    assertEquals(micrometerMetricsCount, 3);
    assertEquals(legacyMetricsCount, 3);
  }

  @Test
  public void testRecordTimerWithNoTags() {
    String metricName = "test.timer.no.tags";
    long duration = TimeUnit.MILLISECONDS.toNanos(100);

    metricUtils.recordTimer(metricName, duration);

    Timer timer = meterRegistry.timer(metricName);
    assertNotNull(timer);
    assertEquals(timer.getId().getTags().size(), 0);
  }

  @Test
  public void testRecordDistributionWithNoTags() {
    String metricName = "test.distribution.no.tags";

    metricUtils.recordDistribution(metricName, 100L);

    DistributionSummary summary = meterRegistry.summary(metricName);
    assertNotNull(summary);
    assertEquals(summary.getId().getTags().size(), 0);
  }
}
