package com.linkedin.metadata.utils.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.LongTaskTimer;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;

import java.util.HashMap;
import java.util.Map;

public class MetricUtils {
  private MetricUtils() {
  }

  public static final String DELIMITER = "_";

  private static final PrometheusMeterRegistry PROMETHEUS_METER_REGISTRY = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
  private static final Map<String, LongTaskTimer> timerMap = new HashMap<>();
  private static final Map<String, DistributionSummary> histogramMap = new HashMap<>();

  public static Counter counter(Class<?> klass, String... subNames) {
    return counter(klass.toString(), subNames);
  }

  public static Counter counter(String metricName, String... subNames) {
    String name = buildName(metricName, subNames);
    return PROMETHEUS_METER_REGISTRY.counter(name);
  }

  public static LongTaskTimer timer(Class<?> klass, String... subNames) {
    return timer(klass.toString(), subNames);
  }

  public static LongTaskTimer timer(String metricName, String... subNames) {
    String name = buildName(metricName, subNames);
    if (!timerMap.containsKey(name)) {
      timerMap.put(name, LongTaskTimer.builder(name).register(PROMETHEUS_METER_REGISTRY));
    }
    return timerMap.get(name);
  }

  public static DistributionSummary histogram(Class<?> klass, String... subNames) {
    return histogram(klass.toString(), subNames);
  }

  public static DistributionSummary histogram(String metricName, String... subNames) {
    String name = buildName(metricName, subNames);
    if (!histogramMap.containsKey(name)) {
      histogramMap.put(name, DistributionSummary.builder(name).register(PROMETHEUS_METER_REGISTRY));
    }
    return histogramMap.get(name);
  }

  public static void exceptionCounter(Class<?> klass, String metricName, Throwable t) {
    String[] splitClassName = t.getClass().getName().split("[.]");
    String snakeCase = splitClassName[splitClassName.length - 1].replaceAll("([A-Z][a-z])", DELIMITER + "$1");
    counter(klass, metricName).increment();
    counter(klass, metricName + DELIMITER + snakeCase).increment();
  }

  public static String buildName(Class<?> klass, String... subNames) {
    return buildName(klass.toString(), subNames);
  }

  public static String buildName(String name, String... names) {
    final StringBuilder builder = new StringBuilder();
    builder.append(name);
    if (names != null) {
      for (String s : names) {
        if (s != null && !s.isEmpty()) {
          builder.append('.');
          builder.append(s);
        }
      }
    }
    return builder.toString();
  }

  public static String scrapePrometheusData() {
    return PROMETHEUS_METER_REGISTRY.scrape();
  }
}
