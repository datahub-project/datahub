package com.linkedin.metadata.config.usage.metric;

import javax.annotation.Nullable;

/** Shared YAML shape for metric registry entries. */
public interface MetricRegistryYamlDefinition {

  @Nullable
  String getMergeKind();

  @Nullable
  String getDistinctKey();

  @Nullable
  String getValueUnit();

  @Nullable
  String getEmitWhen();
}
