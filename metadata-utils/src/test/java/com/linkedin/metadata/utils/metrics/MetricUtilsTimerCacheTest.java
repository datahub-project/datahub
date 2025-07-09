package com.linkedin.metadata.utils.metrics;

import static org.testng.Assert.*;

import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.concurrent.TimeUnit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class MetricUtilsTimerCacheTest {

  private SimpleMeterRegistry meterRegistry;
  private MetricUtils metricUtils;

  @BeforeMethod
  public void setUp() {
    meterRegistry = new SimpleMeterRegistry();
    metricUtils = MetricUtils.builder().registry(meterRegistry).build();
  }

  @AfterMethod
  public void tearDown() {
    meterRegistry.clear();
    meterRegistry.close();
  }

  @Test
  public void testTimerCacheReusesExistingTimer() {
    String metricName = "test.cached.timer";

    // First call - should create new timer
    metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(100));

    // Get the timer instance
    Timer firstTimer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(firstTimer);
    assertEquals(firstTimer.count(), 1);

    // Second call - should reuse the same timer
    metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(200));

    // Should be the same timer instance
    Timer secondTimer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertSame(firstTimer, secondTimer);
    assertEquals(secondTimer.count(), 2);

    // Verify total time is sum of both recordings
    assertEquals(secondTimer.totalTime(TimeUnit.MILLISECONDS), 300.0, 0.1);
  }

  @Test
  public void testTimerCacheWithMultipleMetrics() {
    String metricName1 = "test.timer1";
    String metricName2 = "test.timer2";

    // Record to first timer multiple times
    metricUtils.time(metricName1, TimeUnit.MILLISECONDS.toNanos(100));
    metricUtils.time(metricName1, TimeUnit.MILLISECONDS.toNanos(150));

    // Record to second timer
    metricUtils.time(metricName2, TimeUnit.MILLISECONDS.toNanos(200));

    // Verify first timer
    Timer timer1 = meterRegistry.timer(metricName1, MetricUtils.DROPWIZARD_METRIC, "true");
    assertEquals(timer1.count(), 2);
    assertEquals(timer1.totalTime(TimeUnit.MILLISECONDS), 250.0, 0.1);

    // Verify second timer
    Timer timer2 = meterRegistry.timer(metricName2, MetricUtils.DROPWIZARD_METRIC, "true");
    assertEquals(timer2.count(), 1);
    assertEquals(timer2.totalTime(TimeUnit.MILLISECONDS), 200.0, 0.1);

    // Verify they are different instances
    assertNotSame(timer1, timer2);
  }

  @Test
  public void testTimerCacheDoesNotDuplicatePercentileMetrics() {
    String metricName = "test.percentile.timer";

    // Record multiple times - should not create duplicate percentile gauges
    for (int i = 0; i < 5; i++) {
      metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(100 * (i + 1)));
    }

    // Count all meters that start with our metric name (timer + percentile gauges)
    long meterCount =
        meterRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getName().startsWith(metricName))
            .count();

    // Should have 1 timer + 6 percentile gauges (0.5, 0.75, 0.95, 0.98, 0.99, 0.999)
    // Without caching, we would have 5 timers + 30 percentile gauges
    assertEquals(meterCount, 7);

    // Verify the timer recorded all 5 events
    Timer timer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertEquals(timer.count(), 5);
  }

  @Test
  public void testTimerCacheWorksWithConcurrentAccess() throws InterruptedException {
    String metricName = "test.concurrent.timer";
    int threadCount = 10;
    int recordsPerThread = 100;

    // Create threads that will all try to record to the same timer
    Thread[] threads = new Thread[threadCount];
    for (int i = 0; i < threadCount; i++) {
      final int threadIndex = i;
      threads[i] =
          new Thread(
              () -> {
                for (int j = 0; j < recordsPerThread; j++) {
                  metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(threadIndex * 10 + j));
                }
              });
    }

    // Start all threads
    for (Thread thread : threads) {
      thread.start();
    }

    // Wait for all threads to complete
    for (Thread thread : threads) {
      thread.join();
    }

    // Verify only one timer was created and all recordings were captured
    Timer timer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertEquals(timer.count(), threadCount * recordsPerThread);

    // Verify no duplicate meters were created
    long timerCount =
        meterRegistry.getMeters().stream()
            .filter(meter -> meter.getId().getName().equals(metricName))
            .filter(meter -> meter instanceof Timer)
            .count();
    assertEquals(timerCount, 1);
  }

  @Test
  public void testTimerCacheHandlesSpecialCharactersInName() {
    // Test various metric names that might cause issues
    String[] metricNames = {
      "test.timer-with-dash",
      "test.timer_with_underscore",
      "test.timer.with.many.dots",
      "test.timer$with$dollar",
      "test.timer@with@at"
    };

    for (String metricName : metricNames) {
      metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(100));
      metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(200));

      Timer timer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
      assertNotNull(timer, "Timer should exist for metric: " + metricName);
      assertEquals(timer.count(), 2, "Timer should have 2 recordings for metric: " + metricName);
    }
  }

  @Test
  public void testTimerCacheWithVeryLongMetricName() {
    // Test with a very long metric name
    StringBuilder longNameBuilder = new StringBuilder("test.timer");
    for (int i = 0; i < 50; i++) {
      longNameBuilder.append(".component").append(i);
    }
    String longMetricName = longNameBuilder.toString();

    metricUtils.time(longMetricName, TimeUnit.MILLISECONDS.toNanos(100));
    metricUtils.time(longMetricName, TimeUnit.MILLISECONDS.toNanos(200));

    Timer timer = meterRegistry.timer(longMetricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(timer);
    assertEquals(timer.count(), 2);
  }

  @Test
  public void testTimerPercentileValuesAreRecorded() {
    String metricName = "test.percentile.values.timer";

    // Record various values to test percentile calculation
    long[] durations = {10, 20, 30, 40, 50, 60, 70, 80, 90, 100}; // milliseconds
    for (long duration : durations) {
      metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(duration));
    }

    // Verify percentile gauges exist
    String[] expectedPercentiles = {"0.5", "0.75", "0.95", "0.98", "0.99", "0.999"};
    for (String percentile : expectedPercentiles) {
      boolean percentileExists =
          meterRegistry.getMeters().stream()
              .anyMatch(
                  meter ->
                      meter.getId().getName().equals(metricName + ".percentile")
                          && meter.getId().getTag("phi") != null
                          && meter.getId().getTag("phi").equals(percentile));
      assertTrue(percentileExists, "Percentile gauge should exist for: " + percentile);
    }
  }

  @Test
  public void testTimerCacheDoesNotLeakMemory() {
    // Test that the cache doesn't grow unbounded
    int uniqueMetrics = 1000;

    for (int i = 0; i < uniqueMetrics; i++) {
      String metricName = "test.timer.memory." + i;
      metricUtils.time(metricName, TimeUnit.MILLISECONDS.toNanos(100));
    }

    // Verify we have the expected number of timers
    long timerCount =
        meterRegistry.getMeters().stream()
            .filter(meter -> meter instanceof Timer)
            .filter(meter -> meter.getId().getName().startsWith("test.timer.memory."))
            .count();

    assertEquals(timerCount, uniqueMetrics);
  }

  @Test
  public void testTimerRecordsZeroDuration() {
    String metricName = "test.zero.duration.timer";

    metricUtils.time(metricName, 0);

    Timer timer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertEquals(timer.totalTime(TimeUnit.NANOSECONDS), 0.0);
  }

  @Test
  public void testTimerRecordsVeryLargeDuration() {
    String metricName = "test.large.duration.timer";

    // Record a very large duration (1 hour in nanoseconds)
    long largeNanos = TimeUnit.HOURS.toNanos(1);
    metricUtils.time(metricName, largeNanos);

    Timer timer = meterRegistry.timer(metricName, MetricUtils.DROPWIZARD_METRIC, "true");
    assertNotNull(timer);
    assertEquals(timer.count(), 1);
    assertEquals(timer.totalTime(TimeUnit.HOURS), 1.0, 0.001);
  }
}
