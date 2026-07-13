package com.linkedin.metadata.config;

import lombok.Data;

/** Neutral usage-related configuration under {@code datahub.usage}. */
@Data
public class UsageConfiguration {
  private UsageAggregationConfiguration aggregation;
}
