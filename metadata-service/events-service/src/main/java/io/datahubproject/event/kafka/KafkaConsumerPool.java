package io.datahubproject.event.kafka;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;
import lombok.Getter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.core.ConsumerFactory;

public class KafkaConsumerPool {

  private final BlockingQueue<KafkaConsumer<String, GenericRecord>> consumerPool;
  private final ConsumerFactory<String, GenericRecord> consumerFactory;
  private final int maxPoolSize;
  @Getter private final AtomicInteger totalConsumersCreated = new AtomicInteger(0);
  @Getter private final Set<KafkaConsumer<String, GenericRecord>> activeConsumers = new HashSet<>();
  @Getter private volatile boolean shuttingDown = false;

  public KafkaConsumerPool(
      final ConsumerFactory<String, GenericRecord> consumerFactory,
      final int initialPoolSize,
      final int maxPoolSize) {
    this.consumerFactory = consumerFactory;
    this.maxPoolSize = maxPoolSize;
    this.consumerPool = new LinkedBlockingQueue<>();

    // Initialize the pool with initial consumers
    for (int i = 0; i < initialPoolSize; i++) {
      consumerPool.add(createConsumer());
    }
  }

  // Create a new consumer when required
  private KafkaConsumer<String, GenericRecord> createConsumer() {
    totalConsumersCreated.incrementAndGet();
    KafkaConsumer<String, GenericRecord> consumer =
        (KafkaConsumer<String, GenericRecord>) consumerFactory.createConsumer();
    synchronized (activeConsumers) {
      activeConsumers.add(consumer);
    }
    return consumer;
  }

  // Borrow a consumer from the pool
  @Nullable
  public KafkaConsumer<String, GenericRecord> borrowConsumer(long time, TimeUnit timeUnit)
      throws InterruptedException {
    if (shuttingDown) {
      return null;
    }

    KafkaConsumer<String, GenericRecord> consumer = consumerPool.poll();

    // If no consumer is available, create a new one if we haven't hit the max pool size
    if (consumer == null) {
      if (totalConsumersCreated.get() < maxPoolSize) {
        consumer = createConsumer();
      } else {
        // Wait for a consumer to be returned
        consumer = consumerPool.poll(time, timeUnit);
      }
    }

    return consumer;
  }

  // Return the consumer to the pool after use
  public void returnConsumer(KafkaConsumer<String, GenericRecord> consumer) {
    if (consumer == null) {
      return;
    }

    // Verify this is actually one of our consumers
    synchronized (activeConsumers) {
      if (!activeConsumers.contains(consumer)) {
        // Not our consumer, don't add to pool
        return;
      }
    }

    if (shuttingDown) {
      // Pool is shutting down, close the consumer instead of returning it
      consumer.close();
      return;
    }

    // Try to return to pool, if it fails close the consumer
    if (!consumerPool.offer(consumer)) {
      consumer.close();
      synchronized (activeConsumers) {
        activeConsumers.remove(consumer);
      }
    }
  }

  // Shutdown all consumers
  public void shutdownPool() {
    shuttingDown = true;

    synchronized (activeConsumers) {
      // Close all consumers (both in pool and borrowed)
      for (KafkaConsumer<String, GenericRecord> consumer : activeConsumers) {
        try {
          consumer.close();
        } catch (Exception e) {
          // Log but continue closing others
        }
      }
      activeConsumers.clear();
    }

    consumerPool.clear();
  }
}
