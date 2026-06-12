package com.linkedin.metadata.ratelimit.model;

import com.netflix.concurrency.limits.Limiter;
import javax.annotation.Nullable;
import lombok.Value;

@Value
public class RateLimitLease {
  @Nullable Limiter.Listener capacityListener;
  @Nullable String capacityRuleId;
  @Nullable String endpointRuleId;
  long startTimeNanos;

  public static RateLimitLease empty() {
    return new RateLimitLease(null, null, null, System.nanoTime());
  }
}
