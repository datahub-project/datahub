package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class MetricUtils {
  /* Shared OpenTelemetry & Micrometer */
  public static final String DROPWIZARD_METRIC = "dwizMetric";
  public static final String DROPWIZARD_NAME = "dwizName";

  /* OpenTelemetry */
  public static final String CACHE_HIT_ATTR = "cache.hit";
  public static final String BATCH_SIZE_ATTR = "batch.size";
  public static final String QUEUE_ENQUEUED_AT_ATTR = "queue.enqueued_at";
  public static final String QUEUE_DURATION_MS_ATTR = "queue.duration_ms";
  public static final String MESSAGING_SYSTEM = "messaging.system";
  public static final String MESSAGING_DESTINATION = "messaging.destination";
  public static final String MESSAGING_DESTINATION_KIND = "messaging.destination_kind";
  public static final String MESSAGING_OPERATION = "messaging.operation";
  public static final String ERROR_TYPE = "error.type";
  public static final String CHANGE_TYPE = "aspect.change_type";
  public static final String ENTITY_TYPE = "aspect.entity_type";
  public static final String ASPECT_NAME = "aspect.name";

  @Deprecated public static final String DELIMITER = "_";

  private final MeterRegistry registry;
  private static final Map<String, Timer> legacyTimeCache = new ConcurrentHashMap<>();
  private static final Map<String, Counter> legacyCounterCache = new ConcurrentHashMap<>();
  private static final Map<String, DistributionSummary> legacyHistogramCache =
      new ConcurrentHashMap<>();
  private static final Map<String, Gauge> legacyGaugeCache = new ConcurrentHashMap<>();
  // For state-based gauges (like throttled state)
  private static final Map<String, AtomicDouble> gaugeStates = new ConcurrentHashMap<>();

  public Optional<MeterRegistry> getRegistry() {
    return Optional.ofNullable(registry);
  }

  @Deprecated
  public void time(String dropWizardMetricName, long durationNanos) {
    getRegistry()
        .ifPresent(
            meterRegistry -> {
              Timer timer =
                  legacyTimeCache.computeIfAbsent(
                      dropWizardMetricName,
                      name ->
                          Timer.builder(name)
                              .tags(DROPWIZARD_METRIC, "true")
                              .register(meterRegistry));
              timer.record(durationNanos, TimeUnit.NANOSECONDS);
            });
  }

  @Deprecated
  public void increment(Class<?> klass, String metricName, double increment) {
    String name = MetricRegistry.name(klass, metricName);
    increment(name, increment);
  }

  @Deprecated
  public void exceptionIncrement(Class<?> klass, String metricName, Throwable t) {
    String[] splitClassName = t.getClass().getName().split("[.]");
    String snakeCase =
        splitClassName[splitClassName.length - 1].replaceAll("([A-Z][a-z])", DELIMITER + "$1");

    increment(klass, metricName, 1);
    increment(klass, metricName + DELIMITER + snakeCase, 1);
  }

  @Deprecated
  public void increment(String metricName, double increment) {
    getRegistry()
        .ifPresent(
            meterRegistry -> {
              Counter counter =
                  legacyCounterCache.computeIfAbsent(
                      metricName,
                      name ->
                          Counter.builder(MetricRegistry.name(name))
                              .tag(DROPWIZARD_METRIC, "true")
                              .register(meterRegistry));
              counter.increment(increment);
            });
  }

  /**
   * Set a state-based gauge value (e.g., for binary states like throttled/not throttled). This is
   * more efficient than repeatedly calling gauge() with different suppliers.
   *
   * @param clazz The class for namespacing
   * @param metricName The metric name
   * @param value The gauge value to set
   */
  @Deprecated
  public void setGaugeValue(Class<?> clazz, String metricName, double value) {
    String name = MetricRegistry.name(clazz, metricName);

    getRegistry()
        .ifPresent(
            meterRegistry -> {
              // Get or create the state holder
              AtomicDouble state = gaugeStates.computeIfAbsent(name, k -> new AtomicDouble(0));

              // Register the gauge if not already registered
              legacyGaugeCache.computeIfAbsent(
                  name,
                  key ->
                      Gauge.builder(key, state, AtomicDouble::get)
                          .tag(DROPWIZARD_METRIC, "true")
                          .register(meterRegistry));

              // Update the value
              state.set(value);
            });
  }

  @Deprecated
  public void histogram(Class<?> clazz, String metricName, long value) {
    getRegistry()
        .ifPresent(
            meterRegistry -> {
              String name = MetricRegistry.name(clazz, metricName);
              DistributionSummary summary =
                  legacyHistogramCache.computeIfAbsent(
                      name,
                      key ->
                          DistributionSummary.builder(key)
                              .tag(DROPWIZARD_METRIC, "true")
                              .register(meterRegistry));
              summary.record(value);
            });
  }

  @Deprecated
  public static String name(String name, String... names) {
    return MetricRegistry.name(name, names);
  }

  @Deprecated
  public static String name(Class<?> clazz, String... names) {
    return MetricRegistry.name(clazz.getName(), names);
  }
}
