package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.MetricRegistry;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
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
  private final Map<String, Timer> legacyTimeCache = new ConcurrentHashMap<>();

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
                              .publishPercentiles(0.5, 0.75, 0.95, 0.98, 0.99, 0.999)
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
            meterRegistry ->
                Counter.builder(MetricRegistry.name(metricName))
                    .tag(DROPWIZARD_METRIC, "true")
                    .register(meterRegistry)
                    .increment(increment));
  }

  @Deprecated
  public void gauge(Class<?> clazz, String metricName, Supplier<? extends Number> valueSupplier) {

    getRegistry()
        .ifPresent(
            meterRegistry -> {
              String name = com.codahale.metrics.MetricRegistry.name(clazz, metricName);

              Gauge.builder(name, valueSupplier, supplier -> supplier.get().doubleValue())
                  .tag(DROPWIZARD_METRIC, "true")
                  .register(meterRegistry);
            });
  }

  @Deprecated
  public void histogram(Class<?> clazz, String metricName, long value) {
    getRegistry()
        .ifPresent(
            meterRegistry ->
                DistributionSummary.builder(MetricRegistry.name(clazz, metricName))
                    .tag(DROPWIZARD_METRIC, "true")
                    .register(meterRegistry)
                    .record(value));
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
