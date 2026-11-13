package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.netflix.concurrency.limits.Limiter;
import com.netflix.concurrency.limits.limit.Gradient2Limit;
import com.netflix.concurrency.limits.limiter.SimpleLimiter;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class AdaptiveCapacityLimiter {
  private final Map<String, Limiter<Void>> limiters = new HashMap<>();

  AdaptiveCapacityLimiter(@Nonnull Map<String, CapacityLimitConfig> configs) {
    configs.forEach(
        (ruleId, config) -> {
          Gradient2Limit gradientLimit =
              Gradient2Limit.newBuilder()
                  .initialLimit(config.getInitialLimit())
                  .minLimit(config.getMinLimit())
                  .maxConcurrency(config.getMaxLimit())
                  .build();
          limiters.put(
              ruleId, SimpleLimiter.newBuilder().named(ruleId).limit(gradientLimit).build());
        });
  }

  @Nonnull
  Optional<Limiter.Listener> tryAcquire(@Nonnull String ruleId) {
    Limiter<Void> limiter = limiters.get(ruleId);
    if (limiter == null) {
      return Optional.empty();
    }
    return limiter.acquire(null);
  }

  void release(@Nullable Limiter.Listener listener, boolean success) {
    if (listener == null) {
      return;
    }
    if (success) {
      listener.onSuccess();
    } else {
      listener.onIgnore();
    }
  }

  int getLimit(@Nonnull String ruleId) {
    Limiter<Void> limiter = limiters.get(ruleId);
    if (limiter instanceof SimpleLimiter<?> simpleLimiter) {
      return simpleLimiter.getLimit();
    }
    return -1;
  }

  int getInflight(@Nonnull String ruleId) {
    Limiter<Void> limiter = limiters.get(ruleId);
    if (limiter instanceof SimpleLimiter<?> simpleLimiter) {
      return simpleLimiter.getInflight();
    }
    return -1;
  }
}
