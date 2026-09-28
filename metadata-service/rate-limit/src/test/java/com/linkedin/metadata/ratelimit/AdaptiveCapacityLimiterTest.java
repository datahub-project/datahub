package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.netflix.concurrency.limits.Limiter;
import java.util.Map;
import org.testng.annotations.Test;

public class AdaptiveCapacityLimiterTest {

  @Test
  public void testAcquireReleaseSuccessAndFailure() {
    CapacityLimitConfig config = new CapacityLimitConfig();
    config.setInitialLimit(1);
    config.setMinLimit(1);
    config.setMaxLimit(1);

    AdaptiveCapacityLimiter limiter = new AdaptiveCapacityLimiter(Map.of("rule-a", config));

    Limiter.Listener listener =
        limiter.tryAcquire("rule-a").orElseThrow(() -> new AssertionError("expected acquire"));
    assertFalse(limiter.tryAcquire("rule-a").isPresent());

    limiter.release(listener, false);
    assertTrue(limiter.tryAcquire("rule-a").isPresent());
  }

  @Test
  public void testUnknownRuleReturnsEmptyAcquire() {
    AdaptiveCapacityLimiter limiter = new AdaptiveCapacityLimiter(Map.of());
    assertFalse(limiter.tryAcquire("missing").isPresent());
  }

  @Test
  public void testLimitAndInflightMetrics() {
    CapacityLimitConfig config = new CapacityLimitConfig();
    config.setInitialLimit(5);
    config.setMinLimit(1);
    config.setMaxLimit(10);

    AdaptiveCapacityLimiter limiter = new AdaptiveCapacityLimiter(Map.of("rule-a", config));

    assertTrue(limiter.getLimit("rule-a") > 0);
    assertEquals(limiter.getInflight("rule-a"), 0);
    assertEquals(limiter.getLimit("missing"), -1);
    assertEquals(limiter.getInflight("missing"), -1);
  }

  @Test
  public void testReleaseNullListenerIsNoOp() {
    AdaptiveCapacityLimiter limiter = new AdaptiveCapacityLimiter(Map.of());
    limiter.release(null, true);
  }
}
