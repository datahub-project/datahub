package com.linkedin.metadata.config.usage.metric;

import java.util.Map;
import javax.annotation.Nonnull;

/** Validates metric registry YAML manifest families. */
public final class MetricRegistryManifestValidation {

  private MetricRegistryManifestValidation() {}

  public static <D extends MetricRegistryYamlDefinition> void validate(
      @Nonnull Map<String, Map<String, D>> metricRegistry) {
    if (metricRegistry.isEmpty()) {
      throw new IllegalStateException("metric_registry manifest must define at least one family");
    }
    metricRegistry.forEach(
        (family, metrics) -> {
          if (metrics == null || metrics.isEmpty()) {
            throw new IllegalStateException("metric_registry." + family + " is empty");
          }
          metrics.forEach(
              (name, def) -> {
                if (def.getMergeKind() == null || def.getEmitWhen() == null) {
                  throw new IllegalStateException(
                      "metric_registry." + family + "." + name + " missing required fields");
                }
                if ("distinct".equals(def.getMergeKind()) && def.getDistinctKey() == null) {
                  throw new IllegalStateException(
                      "metric_registry."
                          + family
                          + "."
                          + name
                          + " distinct metric missing distinct_key");
                }
              });
        });
  }
}
