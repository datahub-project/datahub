package com.linkedin.metadata.config.usage.manifest;

import com.linkedin.metadata.config.usage.metric.MetricRegistryYamlDefinition;
import java.util.Map;
import lombok.Data;

/** YAML shape for {@code usage_metric_registry.yaml}. */
@Data
public class UsageMetricRegistryManifest {

  private Map<String, Map<String, MetricFamilyEntry>> metricRegistry;

  @Data
  public static class MetricFamilyEntry implements MetricRegistryYamlDefinition {
    private String mergeKind;
    private String distinctKey;
    private String valueUnit;
    private String emitWhen;
  }
}
