package com.linkedin.metadata.kafka;

import static org.testng.Assert.*;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.testng.annotations.Test;

public class KafkaProducerInitializationRetryTest {

  @Test
  public void testSuccessOnFirstAttempt() {
    KafkaProducerInitializationRetry.Settings settings = settings(3, 100, 4000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    Producer<String, String> producer =
        KafkaProducerInitializationRetry.createWithRetry(
            minimalProps(),
            settings,
            props -> {
              attemptCount.incrementAndGet();
              return mockProducer();
            });

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 1);
  }

  @Test
  public void testSuccessAfterRetries() {
    KafkaProducerInitializationRetry.Settings settings = settings(3, 1, 4000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    Producer<String, String> producer =
        KafkaProducerInitializationRetry.createWithRetry(
            minimalProps(),
            settings,
            props -> {
              if (attemptCount.incrementAndGet() < 3) {
                throw new KafkaException("Failed to construct kafka producer");
              }
              return mockProducer();
            });

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 3);
  }

  @Test(expectedExceptions = KafkaException.class)
  public void testFailsAfterAllRetries() {
    KafkaProducerInitializationRetry.Settings settings = settings(3, 1, 4000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            attemptCount.incrementAndGet();
            throw new KafkaException("Failed to construct kafka producer");
          });
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 3);
      throw e;
    }
  }

  @Test
  public void testSingleAttemptConfig() {
    KafkaProducerInitializationRetry.Settings settings = settings(1, 1, 4000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            attemptCount.incrementAndGet();
            throw new KafkaException("Failed to construct kafka producer");
          });
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1);
    }
  }

  @Test
  public void testZeroAttemptConfigClampsToOne() {
    KafkaProducerInitializationRetry.Settings settings = settings(0, 1, 4000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            attemptCount.incrementAndGet();
            throw new KafkaException("Failed to construct kafka producer");
          });
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1, "retry count below 1 should clamp to one attempt");
    }
  }

  @Test
  public void testPreservesLastException() {
    KafkaProducerInitializationRetry.Settings settings = settings(2, 1, 4000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            int attempt = attemptCount.incrementAndGet();
            if (attempt == 1) {
              throw new KafkaException("First failure");
            }
            throw new KafkaException("Second failure");
          });
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 2);
      assertEquals(e.getMessage(), "Second failure");
    }
  }

  @Test
  public void testMaxBackoffCap() {
    KafkaProducerInitializationRetry.Settings settings = settings(4, 1000, 1500, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);

    long start = System.currentTimeMillis();
    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            attemptCount.incrementAndGet();
            throw new KafkaException("Failed to construct kafka producer");
          });
      fail("Should have thrown");
    } catch (KafkaException e) {
      long elapsed = System.currentTimeMillis() - start;
      assertEquals(attemptCount.get(), 4);
      // sleeps should be 1000 + 1500 + 1500, not 1000 + 2000 + 4000
      assertTrue(elapsed < 6000, "Expected capped cumulative backoff, elapsed=" + elapsed);
    }
  }

  @Test
  public void testMaxTotalWaitCapStopsEarly() {
    KafkaProducerInitializationRetry.Settings settings = settings(10, 1000, 4000, 2500);
    AtomicInteger attemptCount = new AtomicInteger(0);

    long start = System.currentTimeMillis();
    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            attemptCount.incrementAndGet();
            throw new KafkaException("Failed to construct kafka producer");
          });
      fail("Should have thrown");
    } catch (KafkaException e) {
      long elapsed = System.currentTimeMillis() - start;
      assertTrue(attemptCount.get() < 10, "Should stop before exhausting all attempts");
      assertTrue(elapsed < 5000, "Should respect total wait cap, elapsed=" + elapsed);
    }
  }

  @Test(expectedExceptions = KafkaException.class)
  public void testInterruptedDuringSleep() {
    KafkaProducerInitializationRetry.Settings settings = settings(3, 5000, 5000, 15000);
    AtomicInteger attemptCount = new AtomicInteger(0);
    Thread testThread = Thread.currentThread();

    try {
      KafkaProducerInitializationRetry.createWithRetry(
          minimalProps(),
          settings,
          props -> {
            int attempt = attemptCount.incrementAndGet();
            if (attempt == 1) {
              new Thread(
                      () -> {
                        try {
                          Thread.sleep(100);
                          testThread.interrupt();
                        } catch (InterruptedException ignored) {
                        }
                      })
                  .start();
              throw new KafkaException("Failed to construct kafka producer");
            }
            fail("Should not reach second attempt after interrupt");
            return mockProducer();
          });
      fail("Should have thrown KafkaException due to interrupt");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1);
      assertTrue(Thread.interrupted());
      throw e;
    }
  }

  private static KafkaProducerInitializationRetry.Settings settings(
      int maxAttempts, long initialBackoffMs, long maxBackoffMs, long maxTotalWaitMs) {
    return KafkaProducerInitializationRetry.Settings.builder()
        .maxAttempts(maxAttempts)
        .initialBackoffMs(initialBackoffMs)
        .maxBackoffMs(maxBackoffMs)
        .maxTotalWaitMs(maxTotalWaitMs)
        .build();
  }

  private static Map<String, Object> minimalProps() {
    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }

  @SuppressWarnings("unchecked")
  private static <K, V> Producer<K, V> mockProducer() {
    return (Producer<K, V>) org.mockito.Mockito.mock(Producer.class);
  }
}
