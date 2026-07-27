package com.linkedin.metadata.ratelimit;

import java.util.Random;
import javax.annotation.Nonnull;

final class EndpointRetryAfterCalculator {

  private EndpointRetryAfterCalculator() {}

  static int computeSeconds(
      int minBackoffSeconds, long nanosToWaitForRefill, int jitterPercent, @Nonnull Random random) {
    long waitSec = (long) Math.ceil(nanosToWaitForRefill / 1_000_000_000.0);
    int base = (int) Math.max(minBackoffSeconds, waitSec);
    if (jitterPercent <= 0) {
      return base;
    }
    int maxJitter = (int) Math.ceil(base * (jitterPercent / 100.0));
    return base + (maxJitter > 0 ? random.nextInt(maxJitter + 1) : 0);
  }
}
