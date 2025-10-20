package io.datahubproject.event.kafka;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
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

  private final ReentrantLock activeConsumersLock = new ReentrantLock();

  private final ReentrantLock poolManagementLock = new ReentrantLock();

  public KafkaConsumerPool(
      final ConsumerFactory<String, GenericRecord> consumerFactory,
      final int initialPoolSize,
      final int maxPoolSize) {
    this.consumerFactory = consumerFactory;
    this.maxPoolSize = maxPoolSize;
    this.consumerPool = new LinkedBlockingQueue<>(maxPoolSize);

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

    activeConsumersLock.lock();
    try {
      activeConsumers.add(consumer);
    } finally {
      activeConsumersLock.unlock();
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
      poolManagementLock.lock();
      try {

        if (totalConsumersCreated.get() < maxPoolSize && !shuttingDown) {
          consumer = createConsumer();
        }
      } finally {
        poolManagementLock.unlock();
      }

      // If still null, wait for a consumer to be returned
      if (consumer == null) {
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
    boolean isOurConsumer;
    activeConsumersLock.lock();
    try {
      isOurConsumer = activeConsumers.contains(consumer);
    } finally {
      activeConsumersLock.unlock();
    }

    if (!isOurConsumer) {
      // Not our consumer, don't add to pool
      return;
    }

    if (shuttingDown) {
      // Pool is shutting down, close the consumer instead of returning it
      closeAndRemoveConsumer(consumer);
      return;
    }

    // Try to return to pool, if it fails close the consumer
    if (!consumerPool.offer(consumer)) {
      closeAndRemoveConsumer(consumer);
    }
  }

  private void closeAndRemoveConsumer(KafkaConsumer<String, GenericRecord> consumer) {
    try {
      consumer.close();
    } finally {
      activeConsumersLock.lock();
      try {
        activeConsumers.remove(consumer);
        totalConsumersCreated.decrementAndGet();
      } finally {
        activeConsumersLock.unlock();
      }
    }
  }

  // Shutdown all consumers
  public void shutdownPool() {
    poolManagementLock.lock();
    try {
      shuttingDown = true;
    } finally {
      poolManagementLock.unlock();
    }

    activeConsumersLock.lock();
    try {
      // Close all consumers (both in pool and borrowed)
      for (KafkaConsumer<String, GenericRecord> consumer : activeConsumers) {
        closeAndRemoveConsumer(consumer);
      }
      activeConsumers.clear();
    } finally {
      activeConsumersLock.unlock();
    }

    consumerPool.clear();
  }
}
