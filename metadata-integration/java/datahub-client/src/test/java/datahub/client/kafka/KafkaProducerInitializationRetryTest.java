package datahub.client.kafka;

import static org.junit.Assert.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.KafkaException;
import org.junit.Test;

public class KafkaProducerInitializationRetryTest {

  @Test
  public void testZeroAttemptConfigClampsToOne() {
    KafkaProducerInitializationRetry.Settings settings =
        KafkaProducerInitializationRetry.Settings.builder()
            .maxAttempts(0)
            .initialBackoffMs(1)
            .build();
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
      assertEquals(1, attemptCount.get());
    }
  }

  @Test
  public void testMaxTotalWaitCapStopsEarly() {
    KafkaProducerInitializationRetry.Settings settings =
        KafkaProducerInitializationRetry.Settings.builder()
            .maxAttempts(10)
            .initialBackoffMs(1000)
            .maxBackoffMs(4000)
            .maxTotalWaitMs(2500)
            .build();
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
      // Backoff 1000ms, then 1500ms (capped from 2000ms by the 2500ms budget), then the budget
      // is exhausted: three attempts, not maxAttempts.
      assertEquals(3, attemptCount.get());
    }
  }

  private static Properties minimalProps() {
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }
}
