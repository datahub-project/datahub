package com.linkedin.metadata.usage.registry.metrics;

import java.util.Map;
import javax.annotation.Nonnull;

/** Extension point — additional metric definitions register at startup via Spring beans. */
public interface UsageMetricContributor {

  void contribute(@Nonnull Map<String, Map<String, UsageMetricRegistry.MetricDefinition>> families);
}
