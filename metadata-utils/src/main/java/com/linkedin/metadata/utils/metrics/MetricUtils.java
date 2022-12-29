package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
public class MetricUtils {
  private MetricUtils() {
  }

  public static final String DELIMITER = "_";

  public static final String NAME = "default";

  // CodaHale data
  private static final MetricRegistry REGISTRY = SharedMetricRegistries.getOrCreate(NAME);
  private static final Map<UUID, com.codahale.metrics.Timer.Context> TIMER_CONTEXT = new HashMap<>();

  // Micrometer data
  private static final PrometheusMeterRegistry PROMETHEUS_METER_REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  private static final Map<String, DistributionSummary> HISTOGRAM_MAP = new HashMap<>();
  private static final Map<String, Timer> TIMER_MAP = new HashMap<>();
  private static final Map<UUID, Timer.Sample> SMAPLE_MAP = new HashMap<>();

  static {
    final JmxReporter reporter = JmxReporter.forRegistry(REGISTRY).build();
    reporter.start();
  }

  public static MetricRegistry getCodaHaleRegistry() {
    return REGISTRY;
  }

  public static PrometheusMeterRegistry getMicrometerRegistry() {
    return PROMETHEUS_METER_REGISTRY;
  }

  public static String buildName(String name, String... names) {
    return MetricRegistry.name(name, names);
  }

  public static String scrapePrometheusData() {
    return PROMETHEUS_METER_REGISTRY.scrape();
  }

  // Counter

  public static void counterInc(long value, String name, String...names) {
    String finalName = buildName(name, names);
    REGISTRY.counter(finalName).inc(value);
    PROMETHEUS_METER_REGISTRY.counter(finalName).increment(value);
  }

  public static void counterInc(String name, String...names) {
    String finalName = buildName(name, names);
    REGISTRY.counter(finalName).inc();
    PROMETHEUS_METER_REGISTRY.counter(finalName).increment();
  }

  // Histogram

  public static void updateHistogram(long value, String name, String...names) {
    String finalName = buildName(name, names);
    REGISTRY.histogram(finalName).update(value);
    histogram(finalName).record(value);
  }

  private static DistributionSummary histogram(String name) {
    if (!HISTOGRAM_MAP.containsKey(name)) {
      HISTOGRAM_MAP.put(name, DistributionSummary.builder(name)
              .publishPercentiles(0.75, 0.95, 0.98, 0.99, 0.999)
              .register(PROMETHEUS_METER_REGISTRY));
    }
    return HISTOGRAM_MAP.get(name);
  }

  public static DistributionSummary getMicrometerHistogram(String name) {
    return HISTOGRAM_MAP.get(name);
  }

  // Timer

  public static UUID timerStart(String name, String...names) {
    String finalName = buildName(name, names);
    UUID uuid = UUID.randomUUID();
    TIMER_CONTEXT.put(uuid, REGISTRY.timer(finalName).time());
    SMAPLE_MAP.put(uuid, Timer.start());
    return uuid;
  }

  public static void timerStop(UUID uuid, String name, String...names) {
    String finalName = buildName(name, names);
    if (TIMER_CONTEXT.containsKey(uuid)) {
      TIMER_CONTEXT.remove(uuid).stop();
    } else {
      log.error("Trying to stop non-existent timer: " + buildName(name, names));
    }
    SMAPLE_MAP.get(uuid).stop(timer(finalName));
  }

  private static Timer timer(String name) {
    if (!TIMER_MAP.containsKey(name)) {
      TIMER_MAP.put(name, Timer.builder(name)
              .publishPercentiles(0.75, 0.95, 0.99)
              .register(PROMETHEUS_METER_REGISTRY));
    }
    return TIMER_MAP.get(name);
  }

  public static Timer getMicrometerTimer(String name) {
    return TIMER_MAP.get(name);
  }

  // Exception

  public static void exceptionCounter(Throwable t, String name, String...names) {
    String[] splitClassName = t.getClass().getName().split("[.]");
    String snakeCase = splitClassName[splitClassName.length - 1].replaceAll("([A-Z][a-z])", DELIMITER + "$1");

    counterInc(name, names);
    counterInc(name, names + DELIMITER + snakeCase);
  }
}
