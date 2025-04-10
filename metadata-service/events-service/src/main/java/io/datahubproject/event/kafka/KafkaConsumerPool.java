package io.datahubproject.event.kafka;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.core.ConsumerFactory;

public class KafkaConsumerPool {

  private final BlockingQueue<KafkaConsumer<String, GenericRecord>> consumerPool;
  private final ConsumerFactory<String, GenericRecord> consumerFactory;
  private final int initialPoolSize;
  private final int maxPoolSize;

  public KafkaConsumerPool(
      final ConsumerFactory<String, GenericRecord> consumerFactory,
      final int initialPoolSize,
      final int maxPoolSize) {
    this.consumerFactory = consumerFactory;
    this.initialPoolSize = initialPoolSize;
    this.maxPoolSize = maxPoolSize;
    this.consumerPool = new LinkedBlockingQueue<>();

    // Initialize the pool with initial consumers
    for (int i = 0; i < initialPoolSize; i++) {
      consumerPool.add(createConsumer());
    }
  }

  // Create a new consumer when required
  private KafkaConsumer<String, GenericRecord> createConsumer() {
    return (KafkaConsumer<String, GenericRecord>) consumerFactory.createConsumer();
  }

  // Borrow a consumer from the pool
  public KafkaConsumer<String, GenericRecord> borrowConsumer() throws InterruptedException {
    KafkaConsumer<String, GenericRecord> consumer = consumerPool.poll();

    // If no consumer is available, create a new one if we haven't hit the max pool size
    if (consumer == null && consumerPool.size() < maxPoolSize) {
      consumer = createConsumer();
    }

    // If pool size reached max, wait for an available consumer
    if (consumer == null) {
      consumer = consumerPool.take(); // Blocking until a consumer is available
    }
    return consumer;
  }

  // Return the consumer to the pool after use
  public void returnConsumer(KafkaConsumer<String, GenericRecord> consumer) {
    consumerPool.offer(consumer);
  }

  // Shutdown all consumers
  public void shutdownPool() {
    for (KafkaConsumer<String, GenericRecord> consumer : consumerPool) {
      consumer.close();
    }
  }
}
