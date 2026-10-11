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
  public void testFactoryDelegatesInitializationSettings() {
    ProducerConfiguration config = new ProducerConfiguration();
    config.setInitializationRetryCount(0);
    config.setInitializationRetryBackoffMs(1);
    config.setInitializationRetryMaxBackoffMs(4000);
    config.setInitializationRetryMaxTotalWaitMs(15000);

    AtomicInteger attemptCount = new AtomicInteger(0);
    Function<Map<String, Object>, Producer<String, String>> mockFactory =
        p -> {
          attemptCount.incrementAndGet();
          throw new KafkaException("Failed to construct kafka producer");
        };

    try {
      DataHubKafkaProducerFactory.createProducerWithRetry(minimalProps(), config, mockFactory);
      fail("Should have thrown KafkaException");
    } catch (KafkaException e) {
      assertEquals(attemptCount.get(), 1, "Factory should pass through clamped attempt count");
    }
  }

  private Map<String, Object> minimalProps() {
    Map<String, Object> props = new HashMap<>();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    return props;
  }
}
