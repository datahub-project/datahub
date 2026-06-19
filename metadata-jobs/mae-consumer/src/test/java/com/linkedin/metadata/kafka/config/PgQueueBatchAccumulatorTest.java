package com.linkedin.metadata.kafka.config;

import static org.testng.Assert.*;

import com.linkedin.metadata.pgqueue.PgQueueBatchAccumulator;
import com.linkedin.metadata.queue.PgQueuePayloadCompression;
import com.linkedin.metadata.queue.QueueMessageHandle;
import com.linkedin.metadata.queue.QueueReceivedMessage;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import org.testng.annotations.Test;

public class PgQueueBatchAccumulatorTest {

  private static QueueReceivedMessage message(int payloadSize) {
    return new QueueReceivedMessage(
        new QueueMessageHandle(1, Instant.now(), 0, 0, 1),
        0,
        new byte[payloadSize],
        Optional.empty(),
        PgQueuePayloadCompression.NONE,
        List.of(),
        "key",
        "owner");
  }

  @Test
  public void testFlushOnMessageCount() {
    PgQueueBatchAccumulator acc = new PgQueueBatchAccumulator(3, 1_000_000, 60_000);
    assertFalse(acc.shouldFlush());
    assertTrue(acc.isEmpty());

    acc.addAll(List.of(message(10), message(10)));
    assertFalse(acc.shouldFlush());
    assertEquals(acc.messageCount(), 2);

    acc.addAll(List.of(message(10)));
    assertTrue(acc.shouldFlush());
    assertEquals(acc.messageCount(), 3);

    List<QueueReceivedMessage> drained = acc.drain();
    assertEquals(drained.size(), 3);
    assertTrue(acc.isEmpty());
    assertFalse(acc.shouldFlush());
  }

  @Test
  public void testFlushOnByteThreshold() {
    PgQueueBatchAccumulator acc = new PgQueueBatchAccumulator(1000, 50, 60_000);
    acc.addAll(List.of(message(20), message(20)));
    assertFalse(acc.shouldFlush());
    assertEquals(acc.byteCount(), 40);

    acc.addAll(List.of(message(15)));
    assertTrue(acc.shouldFlush());
    assertEquals(acc.byteCount(), 55);
  }

  @Test
  public void testFlushOnAge() {
    Clock fixedClock = Clock.fixed(Instant.ofEpochMilli(1000), ZoneId.of("UTC"));
    PgQueueBatchAccumulator acc = new PgQueueBatchAccumulator(1000, 1_000_000, 100, fixedClock);

    acc.addAll(List.of(message(10)));
    assertFalse(acc.shouldFlush());
    assertFalse(acc.isExpired());

    // Advance clock past the linger threshold
    Clock advancedClock = Clock.fixed(Instant.ofEpochMilli(1100), ZoneId.of("UTC"));
    PgQueueBatchAccumulator acc2 = new PgQueueBatchAccumulator(1000, 1_000_000, 100, advancedClock);
    acc2.addAll(List.of(message(10)));

    // Simulate passage of time by creating an accumulator where the clock is past age
    // The accumulator records oldestMessageTimeMs = clock.millis() at addAll time,
    // and checks clock.millis() - oldestMessageTimeMs >= maxAgeMs at shouldFlush time.
    // Since we can't advance the clock on the same instance, verify with a mock clock.
  }

  @Test
  public void testFlushOnAgeWithMockClock() {
    MutableClock clock = new MutableClock(1000);
    PgQueueBatchAccumulator acc = new PgQueueBatchAccumulator(1000, 1_000_000, 200, clock);

    acc.addAll(List.of(message(10)));
    assertFalse(acc.isExpired());

    clock.advance(199);
    assertFalse(acc.isExpired());

    clock.advance(1);
    assertTrue(acc.isExpired());
    assertTrue(acc.shouldFlush());

    acc.drain();
    assertFalse(acc.isExpired());
    assertFalse(acc.shouldFlush());
  }

  @Test
  public void testDrainResetsCounters() {
    PgQueueBatchAccumulator acc = new PgQueueBatchAccumulator(10, 1000, 60_000);
    acc.addAll(List.of(message(100), message(100)));
    assertEquals(acc.messageCount(), 2);
    assertEquals(acc.byteCount(), 200);

    acc.drain();
    assertEquals(acc.messageCount(), 0);
    assertEquals(acc.byteCount(), 0);
    assertTrue(acc.isEmpty());
  }

  @Test
  public void testEmptyAddAllIsNoOp() {
    PgQueueBatchAccumulator acc = new PgQueueBatchAccumulator(1, 1, 1);
    acc.addAll(List.of());
    assertTrue(acc.isEmpty());
    assertFalse(acc.shouldFlush());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidMaxMessages() {
    new PgQueueBatchAccumulator(0, 1, 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidMaxBytes() {
    new PgQueueBatchAccumulator(1, 0, 1);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testInvalidMaxAge() {
    new PgQueueBatchAccumulator(1, 1, 0);
  }

  /** Mutable clock for testing age-based flush behavior. */
  private static class MutableClock extends Clock {
    private long millis;

    MutableClock(long initialMillis) {
      this.millis = initialMillis;
    }

    void advance(long ms) {
      this.millis += ms;
    }

    @Override
    public ZoneId getZone() {
      return ZoneId.of("UTC");
    }

    @Override
    public Clock withZone(ZoneId zone) {
      return this;
    }

    @Override
    public Instant instant() {
      return Instant.ofEpochMilli(millis);
    }
  }
}
