package com.linkedin.metadata.entity;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.testng.annotations.Test;

public class TransactionContextTest {

  @Test
  public void testNextBackoffMs_zeroForNoFailures() {
    assertEquals(TransactionContext.nextBackoffMs(0), 0);
    assertEquals(TransactionContext.nextBackoffMs(-1), 0);
  }

  @Test
  public void testNextBackoffMs_growsExponentiallyWithJitterBounds() {
    for (int attempt = 1; attempt <= 5; attempt++) {
      long expectedExponential =
          Math.min(
              TransactionContext.RETRY_BACKOFF_MAX_MS,
              TransactionContext.RETRY_BACKOFF_BASE_MS * (1L << (attempt - 1)));
      for (int i = 0; i < 100; i++) {
        long backoff = TransactionContext.nextBackoffMs(attempt);
        assertTrue(
            backoff >= expectedExponential / 2,
            String.format("attempt %d: backoff %d below jitter floor", attempt, backoff));
        assertTrue(
            backoff
                <= Math.min(TransactionContext.RETRY_BACKOFF_MAX_MS, expectedExponential * 3 / 2),
            String.format("attempt %d: backoff %d above jitter ceiling", attempt, backoff));
      }
    }
  }

  @Test
  public void testNextBackoffMs_cappedAtMax() {
    for (int i = 0; i < 100; i++) {
      assertTrue(TransactionContext.nextBackoffMs(30) <= TransactionContext.RETRY_BACKOFF_MAX_MS);
    }
  }
}
