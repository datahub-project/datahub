package com.linkedin.metadata.config.usage.overlay;

import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import java.util.Map;
import javax.annotation.Nullable;

/** Optional runtime overrides merged into OSS {@code usage_operations.yaml}. */
public interface UsageConfigurationOverlay {

  @Nullable
  Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides();

  @Nullable
  Map<String, UsageOperationsManifest.GraphqlClassification> getGraphqlClassificationOverrides();

  static UsageConfigurationOverlay empty() {
    return EmptyUsageConfigurationOverlay.INSTANCE;
  }
}
