package com.linkedin.metadata.ratelimit.model;

import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import com.netflix.concurrency.limits.Limiter;
import javax.annotation.Nullable;
import lombok.Builder;
import lombok.Value;

@Value
@Builder
public class RateLimitDecision {
  boolean allowed;
  @Nullable String capacityRuleId;
  @Nullable String endpointRuleId;
  @Nullable String denyingRuleId;
  @Nullable RateLimitRuleType denyingType;
  RateLimitSource source;
  @Nullable String graphqlOperation;
  @Nullable Integer retryAfterSeconds;
  @Nullable Limiter.Listener capacityListener;

  public static RateLimitDecision disabled(RateLimitSource source) {
    return RateLimitDecision.builder().allowed(true).source(source).build();
  }
}
