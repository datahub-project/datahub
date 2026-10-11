package com.linkedin.metadata.config.usage.metric;

import java.util.List;
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

  /**
   * Optional allowlist of {@code request_api} labels (e.g. {@code openapi}, {@code restli}, {@code
   * graphql}). Empty/null means all request APIs.
   */
  @Nullable
  default List<String> getRequestApis() {
    return null;
  }
}
