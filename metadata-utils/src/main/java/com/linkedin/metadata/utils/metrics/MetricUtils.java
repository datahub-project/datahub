package com.linkedin.metadata.utils.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jmx.JmxReporter;


public class MetricUtils {
  private MetricUtils() {
  }

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
    String snakeCase = splitClassName[splitClassName.length - 1].replaceAll("([A-Z][a-z])", DELIMITER + "$1");

    counter(klass, metricName).inc();
    counter(klass, metricName + DELIMITER + snakeCase).inc();
  }

  public static Counter counter(String metricName) {
    return REGISTRY.counter(MetricRegistry.name(metricName));
  }

  public static Timer timer(Class<?> klass, String metricName) {
    return REGISTRY.timer(MetricRegistry.name(klass, metricName));
  }

  public static Timer timer(String metricName) {
    return REGISTRY.timer(MetricRegistry.name(metricName));
  }
}
