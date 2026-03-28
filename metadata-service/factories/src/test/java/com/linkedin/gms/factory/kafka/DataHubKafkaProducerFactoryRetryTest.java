package com.linkedin.gms.factory.kafka;

import static org.testng.Assert.*;

import com.linkedin.metadata.config.kafka.ProducerConfiguration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;
import org.testng.annotations.Test;

public class DataHubKafkaProducerFactoryRetryTest {

  @Test
  public void testSuccessOnFirstAttempt() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(100);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          return createMockProducer();
        };

    Producer<String, String> producer =
        DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 1, "Producer should be created on first attempt");
  }

  @Test
  public void testSuccessAfterRetries() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(1);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt < 3) {
            throw new KafkaException("Failed to construct kafka producer");
          }
          return createMockProducer();
        };

    Producer<String, String> producer =
        DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 3, "Should have taken 3 attempts to succeed");
  }

  @Test(expectedExceptions = KafkaException.class)
  public void testFailsAfterAllRetries() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(1);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 3, "Should have attempted 3 times before giving up");
      throw e;
    }
  }

  @Test
  public void testSingleRetryConfig() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(1);
    config.setInitializationRetryBackoffMs(1);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1, "With retryCount=1, should only attempt once");
    }
  }

  @Test
  public void testPreservesLastException() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(2);
    config.setInitializationRetryBackoffMs(1);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt == 1) {
            throw new KafkaException("First failure");
          }
          throw new KafkaException("Second failure");
        };

    try {
      DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 2);
      assertEquals(e.getMessage(), "Second failure", "Should throw the last exception encountered");
    }
  }

  @Test
  public void testZeroRetryCount() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(0);
    config.setInitializationRetryBackoffMs(1);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);
      fail("Should have thrown");
    } catch (KafkaException | NullPointerException e) {
      assertEquals(attemptCount.get(), 0, "With retryCount=0, should not attempt at all");
    }
  }

  @Test(expectedExceptions = KafkaException.class)
  public void testInterruptedDuringSleep() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(5000);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);
    Thread testThread = Thread.currentThread();

    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt == 1) {
            // Schedule interrupt before throwing so it fires during the subsequent sleep
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
          return createMockProducer();
        };

    try {
      DataHubKafkaProducerFactory.createProducerWithRetry(props, config, mockFactory);
      fail("Should have thrown KafkaException due to interrupt");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1, "Should only attempt once before being interrupted");
      assertTrue(
          Thread.interrupted(),
          "Thread interrupt flag should be set after InterruptedException handling");
      throw e;
    }
  }

  private Map<String, Object> createMinimalProducerProps() {
    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }

  @SuppressWarnings("unchecked")
  private <K, V> Producer<K, V> createMockProducer() {
    return (Producer<K, V>) org.mockito.Mockito.mock(Producer.class);
  }
}
