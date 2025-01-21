package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.jmx.JmxReporter;

public class MetricUtils {
  public static final String DROPWIZARD_METRIC = "dwizMetric";
  public static final String DROPWIZARD_NAME = "dwizName";
  public static final String CACHE_HIT_ATTR = "cache.hit";
  public static final String BATCH_SIZE_ATTR = "batch.size";
  public static final String QUEUE_ENQUEUED_AT_ATTR = "queue.enqueued_at";
  public static final String QUEUE_DURATION_MS_ATTR = "queue.duration_ms";
  public static final String MESSAGING_SYSTEM = "messaging.system";
  public static final String MESSAGING_DESTINATION = "messaging.destination";
  public static final String MESSAGING_DESTINATION_KIND = "messaging.destination_kind";
  public static final String MESSAGING_OPERATION = "messaging.operation";
  public static final String ERROR_TYPE = "error.type";

  private MetricUtils() {}

  public static final String DELIMITER = "_";

  public static final String NAME = "default";
  private static final MetricRegistry REGISTRY = SharedMetricRegistries.getOrCreate(NAME);

  static {
    final JmxReporter reporter = JmxReporter.forRegistry(REGISTRY).build();
    reporter.start();
  }

  public static MetricRegistry get() {
    return REGISTRY;
  }

  public static Counter counter(Class<?> klass, String metricName) {
    return REGISTRY.counter(MetricRegistry.name(klass, metricName));
  }

  public static void exceptionCounter(Class<?> klass, String metricName, Throwable t) {
    String[] splitClassName = t.getClass().getName().split("[.]");
    String snakeCase =
        splitClassName[splitClassName.length - 1].replaceAll("([A-Z][a-z])", DELIMITER + "$1");

    counter(klass, metricName).inc();
    counter(klass, metricName + DELIMITER + snakeCase).inc();
  }

  public static Counter counter(String metricName) {
    return REGISTRY.counter(MetricRegistry.name(metricName));
  }

  public static String name(String name, String... names) {
    return MetricRegistry.name(name, names);
  }

  public static String name(Class<?> clazz, String... names) {
    return MetricRegistry.name(clazz.getName(), names);
  }

  public static <T extends Gauge<?>> T gauge(
      Class<?> clazz, String metricName, MetricRegistry.MetricSupplier<T> supplier) {
    return REGISTRY.gauge(MetricRegistry.name(clazz, metricName), supplier);
  }
}
