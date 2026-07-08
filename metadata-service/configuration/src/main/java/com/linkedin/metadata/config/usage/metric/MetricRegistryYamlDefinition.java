package com.linkedin.metadata.config.usage.metric;

import javax.annotation.Nullable;

/**
 * Shared YAML shape for metric registry entries. Implemented by OSS and commercial overlay manifest
 * types.
 */
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
