package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.MetricRegistry;
import com.google.common.util.concurrent.AtomicDouble;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
public class MetricUtils {
  /* Shared OpenTelemetry & Micrometer */
  public static final String DROPWIZARD_METRIC = "dwizMetric";
  public static final String DROPWIZARD_NAME = "dwizName";

  /* Micrometer. See https://prometheus.io/docs/practices/naming/ */
  public static final String KAFKA_MESSAGE_QUEUE_TIME = "kafka.message.queue.time";
  public static final String DATAHUB_REQUEST_HOOK_QUEUE_TIME = "datahub.request.hook.queue.time";
  public static final String DATAHUB_REQUEST_COUNT = "datahub_request_count";

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

  @Builder.Default @NonNull private final MeterRegistry registry = new CompositeMeterRegistry();
  private static final Map<String, Timer> legacyTimeCache = new ConcurrentHashMap<>();
  private static final Map<String, Counter> legacyCounterCache = new ConcurrentHashMap<>();
  private static final Map<String, DistributionSummary> legacyHistogramCache =
      new ConcurrentHashMap<>();
  private static final Map<String, Gauge> legacyGaugeCache = new ConcurrentHashMap<>();
  private static final Map<String, Counter> micrometerCounterCache = new ConcurrentHashMap<>();
  private static final Map<String, Timer> micrometerTimerCache = new ConcurrentHashMap<>();
  private static final Map<String, DistributionSummary> micrometerDistributionCache =
      new ConcurrentHashMap<>();
  // For state-based gauges (like throttled state)
  private static final Map<String, AtomicDouble> gaugeStates = new ConcurrentHashMap<>();

  public MeterRegistry getRegistry() {
    return registry;
  }

  @Deprecated
  public void time(String dropWizardMetricName, long durationNanos) {
    Timer timer =
        legacyTimeCache.computeIfAbsent(
            dropWizardMetricName,
            name -> Timer.builder(name).tags(DROPWIZARD_METRIC, "true").register(registry));
    timer.record(durationNanos, TimeUnit.NANOSECONDS);
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
    Counter counter =
        legacyCounterCache.computeIfAbsent(
            metricName,
            name ->
                Counter.builder(MetricRegistry.name(name))
                    .tag(DROPWIZARD_METRIC, "true")
                    .register(registry));
    counter.increment(increment);
  }

  /**
   * Increment a counter using Micrometer metrics library.
   *
   * @param metricName The name of the metric
   * @param increment The value to increment by
   * @param tags The tags to associate with the metric (can be empty)
   */
  public void incrementMicrometer(String metricName, double increment, String... tags) {
    // Create a cache key that includes both metric name and tags
    String cacheKey = createCacheKey(metricName, tags);
    Counter counter =
        micrometerCounterCache.computeIfAbsent(cacheKey, key -> registry.counter(metricName, tags));
    counter.increment(increment);
  }

  /**
   * Record a timer measurement using Micrometer metrics library.
   *
   * @param metricName The name of the metric
   * @param durationNanos The duration in nanoseconds
   * @param tags The tags to associate with the metric (can be empty)
   */
  public void recordTimer(String metricName, long durationNanos, String... tags) {
    String cacheKey = createCacheKey(metricName, tags);
    Timer timer =
        micrometerTimerCache.computeIfAbsent(
            cacheKey, key -> Timer.builder(metricName).tags(tags).register(registry));
    timer.record(durationNanos, TimeUnit.NANOSECONDS);
  }

  /**
   * Record a distribution summary (histogram) using Micrometer metrics library.
   *
   * @param metricName The name of the metric
   * @param value The value to record
   * @param tags The tags to associate with the metric (can be empty)
   */
  public void recordDistribution(String metricName, long value, String... tags) {
    String cacheKey = createCacheKey(metricName, tags);
    DistributionSummary summary =
        micrometerDistributionCache.computeIfAbsent(
            cacheKey, key -> DistributionSummary.builder(metricName).tags(tags).register(registry));
    summary.record(value);
  }

  /**
   * Creates a cache key for a metric with its tags.
   *
   * <p>Examples:
   *
   * <ul>
   *   <li>No tags: {@code createCacheKey("datahub.request.count")} returns {@code
   *       "datahub.request.count"}
   *   <li>With tags: {@code createCacheKey("datahub.request.count", "user_category", "regular",
   *       "agent_class", "browser")} returns {@code
   *       "datahub.request.count|user_category=regular|agent_class=browser"}
   * </ul>
   *
   * @param metricName the name of the metric
   * @param tags the tags to associate with the metric (key-value pairs)
   * @return a string key that uniquely identifies this metric+tags combination
   */
  private String createCacheKey(String metricName, String... tags) {
    if (tags.length == 0) {
      return metricName;
    }

    StringBuilder keyBuilder = new StringBuilder(metricName);
    for (int i = 0; i < tags.length; i += 2) {
      if (i + 1 < tags.length) {
        keyBuilder.append("|").append(tags[i]).append("=").append(tags[i + 1]);
      }
    }
    return keyBuilder.toString();
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

    // Get or create the state holder
    AtomicDouble state = gaugeStates.computeIfAbsent(name, k -> new AtomicDouble(0));

    // Register the gauge if not already registered
    legacyGaugeCache.computeIfAbsent(
        name,
        key ->
            Gauge.builder(key, state, AtomicDouble::get)
                .tag(DROPWIZARD_METRIC, "true")
                .register(registry));

    // Update the value
    state.set(value);
  }

  @Deprecated
  public void histogram(Class<?> clazz, String metricName, long value) {
    String name = MetricRegistry.name(clazz, metricName);
    DistributionSummary summary =
        legacyHistogramCache.computeIfAbsent(
            name,
            key ->
                DistributionSummary.builder(key).tag(DROPWIZARD_METRIC, "true").register(registry));
    summary.record(value);
  }

  @Deprecated
  public static String name(String name, String... names) {
    return MetricRegistry.name(name, names);
  }

  @Deprecated
  public static String name(Class<?> clazz, String... names) {
    return MetricRegistry.name(clazz.getName(), names);
  }

  public static double[] parsePercentiles(String percentilesConfig) {
    if (percentilesConfig == null || percentilesConfig.trim().isEmpty()) {
      // Default percentiles
      return new double[] {0.5, 0.95, 0.99};
    }

    return commaDelimitedDoubles(percentilesConfig);
  }

  public static double[] parseSLOSeconds(String sloConfig) {
    if (sloConfig == null || sloConfig.trim().isEmpty()) {
      // Default SLO seconds
      return new double[] {60, 300, 900, 1800, 3600};
    }

    return commaDelimitedDoubles(sloConfig);
  }

  private static double[] commaDelimitedDoubles(String value) {
    String[] parts = value.split(",");
    double[] result = new double[parts.length];
    for (int i = 0; i < parts.length; i++) {
      result[i] = Double.parseDouble(parts[i].trim());
    }
    return result;
  }
}
