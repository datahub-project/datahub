package com.linkedin.metadata.config.usage.overlay;

import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import java.util.Map;
import javax.annotation.Nullable;

final class EmptyUsageConfigurationOverlay implements UsageConfigurationOverlay {

  static final UsageConfigurationOverlay INSTANCE = new EmptyUsageConfigurationOverlay();

  private EmptyUsageConfigurationOverlay() {}

  @Override
  @Nullable
  public Map<String, ? extends UsageOperationCostOverride> getUsageOperationOverrides() {
    return Map.of();
  }

  @Override
  @Nullable
  public Map<String, UsageOperationsManifest.GraphqlClassification>
      getGraphqlClassificationOverrides() {
    return Map.of();
  }
}
