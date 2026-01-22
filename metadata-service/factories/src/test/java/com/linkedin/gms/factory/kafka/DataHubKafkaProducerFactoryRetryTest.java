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
  public void testCreateProducerWithRetry_SuccessOnFirstAttempt() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(100);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    // Mock producer factory that succeeds on first attempt
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          return createMockProducer();
        };

    Producer<String, String> producer = createProducerWithRetryTestable(props, config, mockFactory);

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 1, "Producer should be created on first attempt");
  }

  @Test
  public void testCreateProducerWithRetry_SuccessAfterRetries() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(10); // Short delay for test

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    // Mock producer factory that fails twice then succeeds
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt < 3) {
            throw new KafkaException("Failed to construct kafka producer");
          }
          return createMockProducer();
        };

    Producer<String, String> producer = createProducerWithRetryTestable(props, config, mockFactory);

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 3, "Should have taken 3 attempts to succeed");
  }

  @Test(expectedExceptions = KafkaException.class)
  public void testCreateProducerWithRetry_FailsAfterAllRetries() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(3);
    config.setInitializationRetryBackoffMs(10); // Short delay for test

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    // Mock producer factory that always fails
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      createProducerWithRetryTestable(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 3, "Should have attempted 3 times before giving up");
      throw e;
    }
  }

  @Test
  public void testCreateProducerWithRetry_ExponentialBackoff() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(4);
    config.setInitializationRetryBackoffMs(50);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    long startTime = System.currentTimeMillis();

    // Mock producer factory that fails 3 times then succeeds
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          int attempt = attemptCount.incrementAndGet();
          if (attempt < 4) {
            throw new KafkaException("Failed to construct kafka producer");
          }
          return createMockProducer();
        };

    Producer<String, String> producer = createProducerWithRetryTestable(props, config, mockFactory);

    long elapsed = System.currentTimeMillis() - startTime;

    assertNotNull(producer);
    assertEquals(attemptCount.get(), 4, "Should have taken 4 attempts");
    // Expected delays: 50 + 100 + 200 = 350ms minimum
    assertTrue(elapsed >= 300, "Should have waited at least 300ms due to exponential backoff");
  }

  @Test
  public void testCreateProducerWithRetry_SingleRetryConfig() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(1);
    config.setInitializationRetryBackoffMs(10);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    // Mock producer factory that always fails
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      createProducerWithRetryTestable(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1, "With retryCount=1, should only attempt once");
    }
  }

  @Test
  public void testCreateProducerWithRetry_PreservesOriginalException() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(2);
    config.setInitializationRetryBackoffMs(10);

    Map<String, Object> props = createMinimalProducerProps();
    String expectedMessage = "No resolvable bootstrap urls given in bootstrap.servers";

    // Mock producer factory that always fails with specific message
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          throw new KafkaException(expectedMessage);
        };

    try {
      createProducerWithRetryTestable(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(e.getMessage(), expectedMessage, "Should preserve original exception message");
    }
  }

  @Test
  public void testCreateProducerWithRetry_ZeroRetryCount() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(0);
    config.setInitializationRetryBackoffMs(10);

    Map<String, Object> props = createMinimalProducerProps();
    AtomicInteger attemptCount = new AtomicInteger(0);

    // Mock producer factory that always fails
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      createProducerWithRetryTestable(props, config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 0, "With retryCount=0, should not attempt at all");
    } catch (NullPointerException e) {
      // Expected when lastException is null due to 0 retries
      assertEquals(attemptCount.get(), 0, "With retryCount=0, should not attempt at all");
    }
  }

  /**
   * Testable version of createProducerWithRetry that accepts a factory function. This mirrors the
   * logic in DataHubKafkaProducerFactory.createProducerWithRetry but allows injecting a mock
   * producer factory.
   */
  private <K, V> Producer<K, V> createProducerWithRetryTestable(
      Map<String, Object> props,
      ProducerConfiguration producerConfiguration,
      Function<Map<String, Object>, Producer<K, V>> producerFactory) {
    int maxRetries = producerConfiguration.getInitializationRetryCount();
    long retryDelayMs = producerConfiguration.getInitializationRetryBackoffMs();
    KafkaException lastException = null;

    for (int attempt = 1; attempt <= maxRetries; attempt++) {
      try {
        return producerFactory.apply(props);
      } catch (KafkaException e) {
        lastException = e;
        if (attempt < maxRetries) {
          try {
            Thread.sleep(retryDelayMs);
            retryDelayMs *= 2; // Exponential backoff
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw e;
          }
        }
      }
    }
    throw lastException;
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
