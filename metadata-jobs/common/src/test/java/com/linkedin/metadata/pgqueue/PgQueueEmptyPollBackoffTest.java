package com.linkedin.metadata.pgqueue;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

import org.testng.annotations.Test;

public class PgQueueEmptyPollBackoffTest {

  @Test
  public void doublesUntilMaxThenStaysCapped() {
    PgQueueEmptyPollBackoff backoff = new PgQueueEmptyPollBackoff(1000, 5000);
    assertEquals(backoff.nextSleepMillis(), 1000L);
    assertEquals(backoff.nextSleepMillis(), 2000L);
    assertEquals(backoff.nextSleepMillis(), 4000L);
    assertEquals(backoff.nextSleepMillis(), 5000L);
    assertEquals(backoff.nextSleepMillis(), 5000L);
  }

  @Test
  public void resetReturnsToMin() {
    PgQueueEmptyPollBackoff backoff = new PgQueueEmptyPollBackoff(1000, 5000);
    backoff.nextSleepMillis();
    backoff.nextSleepMillis();
    backoff.reset();
    assertEquals(backoff.nextSleepMillis(), 1000L);
  }

  @Test
  public void minAboveMaxClampsToMax() {
    PgQueueEmptyPollBackoff backoff = new PgQueueEmptyPollBackoff(8000, 5000);
    assertEquals(backoff.nextSleepMillis(), 5000L);
    assertEquals(backoff.nextSleepMillis(), 5000L);
  }

  @Test
  public void rejectsNonPositiveBounds() {
    assertThrows(IllegalArgumentException.class, () -> new PgQueueEmptyPollBackoff(0, 1000));
    assertThrows(IllegalArgumentException.class, () -> new PgQueueEmptyPollBackoff(1000, 0));
  }
}
