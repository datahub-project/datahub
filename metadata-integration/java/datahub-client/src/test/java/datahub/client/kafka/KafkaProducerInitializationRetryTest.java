package datahub.client.kafka;

import static org.testng.Assert.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.common.KafkaException;
import org.testng.annotations.Test;

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
      assertEquals(attemptCount.get(), 1);
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
      assertTrue(attemptCount.get() < 10);
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
