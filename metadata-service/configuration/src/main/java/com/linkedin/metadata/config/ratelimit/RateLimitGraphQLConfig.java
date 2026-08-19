package com.linkedin.metadata.config.ratelimit;

import lombok.Data;
import lombok.EqualsAndHashCode;

/** GraphQL POST adaptive capacity pool — nested under {@link RateLimitProperties.Capacity}. */
@Data
@EqualsAndHashCode(callSuper = true)
public class RateLimitGraphQLConfig extends CapacityLimitConfig {
  private String pathPattern;
  private boolean operationRulesEnabled;
}
