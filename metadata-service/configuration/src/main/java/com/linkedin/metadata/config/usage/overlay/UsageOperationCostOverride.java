package com.linkedin.metadata.config.usage.overlay;

import javax.annotation.Nullable;

/** Optional {@code default_cost_units} override for a usage operation key. */
public interface UsageOperationCostOverride {

  @Nullable
  Integer getDefaultCostUnits();
}
