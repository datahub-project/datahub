package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Random;
import org.testng.annotations.Test;

public class EndpointRetryAfterCalculatorTest {

  @Test
  public void testUsesMinBackoffWhenRefillWaitIsShorter() {
    assertEquals(
        EndpointRetryAfterCalculator.computeSeconds(60, 2_000_000_000L, 0, new Random(0)), 60);
  }

  @Test
  public void testUsesRefillWaitWhenLongerThanMinBackoff() {
    assertEquals(
        EndpointRetryAfterCalculator.computeSeconds(60, 120_000_000_000L, 0, new Random(0)), 120);
  }

  @Test
  public void testCeilsSubSecondRefillWait() {
    assertEquals(
        EndpointRetryAfterCalculator.computeSeconds(1, 1_500_000_000L, 0, new Random(0)), 2);
  }

  @Test
  public void testZeroJitterReturnsExactBase() {
    Random random = new Random(0);
    assertEquals(EndpointRetryAfterCalculator.computeSeconds(60, 120_000_000_000L, 0, random), 120);
    assertEquals(EndpointRetryAfterCalculator.computeSeconds(60, 120_000_000_000L, 0, random), 120);
  }

  @Test
  public void testJitterAddsBoundedOffset() {
    Random random = new Random(0);
    int retryAfter = EndpointRetryAfterCalculator.computeSeconds(100, 0, 10, random);
    assertTrue(retryAfter >= 100);
    assertTrue(retryAfter <= 110);
  }
}
