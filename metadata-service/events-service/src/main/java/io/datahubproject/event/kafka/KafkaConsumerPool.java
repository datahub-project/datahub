package io.datahubproject.event.kafka;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.core.ConsumerFactory;

@Slf4j
public class KafkaConsumerPool {

  private final BlockingQueue<CheckedConsumer> consumerPool;
  private final ConsumerFactory<String, GenericRecord> consumerFactory;
  private final int maxPoolSize;
  private final Duration validationTimeout;
  private final Duration validationCacheInterval;
  @Nullable private final MetricUtils metricUtils;
  @Getter private final AtomicInteger totalConsumersCreated = new AtomicInteger(0);
  @Getter private volatile boolean shuttingDown = false;

  private final Set<CheckedConsumer> allConsumers = ConcurrentHashMap.newKeySet();
  private final ReentrantLock poolManagementLock = new ReentrantLock();

  public KafkaConsumerPool(
      final ConsumerFactory<String, GenericRecord> consumerFactory,
      final int initialPoolSize,
      final int maxPoolSize,
      final Duration validationTimeout,
      final Duration validationCacheInterval,
      @Nullable final MetricUtils metricUtils) {
    this.consumerFactory = consumerFactory;
    this.maxPoolSize = maxPoolSize;
    this.validationTimeout = validationTimeout;
    this.validationCacheInterval = validationCacheInterval;
    this.metricUtils = metricUtils;
    this.consumerPool = new LinkedBlockingQueue<>(maxPoolSize);

    // Initialize the pool with initial consumers
    for (int i = 0; i < initialPoolSize; i++) {
      consumerPool.add(createConsumer());
    }
  }

  // Create a new consumer when required
  private CheckedConsumer createConsumer() {
    poolManagementLock.lock();
    try {
      if (totalConsumersCreated.get() >= maxPoolSize || shuttingDown) {
        return null;
      }
      totalConsumersCreated.incrementAndGet();
    } finally {
      poolManagementLock.unlock();
    }

    try {
      KafkaConsumer<String, GenericRecord> consumer =
          (KafkaConsumer<String, GenericRecord>) consumerFactory.createConsumer();
      CheckedConsumer checkedConsumer =
          new CheckedConsumer(consumer, validationTimeout, validationCacheInterval, metricUtils);
      allConsumers.add(checkedConsumer);
      return checkedConsumer;
    } catch (Exception e) {
      poolManagementLock.lock();
      try {
        totalConsumersCreated.decrementAndGet();
      } finally {
        poolManagementLock.unlock();
      }
      throw e;
    }
  }

  // Borrow a consumer from the pool
  @Nullable
  public CheckedConsumer borrowConsumer(long time, TimeUnit timeUnit, @Nonnull String topic)
      throws InterruptedException {
    if (shuttingDown) {
      return null;
    }
    if (topic == null || topic.isEmpty()) {
      throw new IllegalArgumentException("Topic must be non-null and non-empty");
    }

    CheckedConsumer checkedConsumer = null;
    long remainingTime = timeUnit.toMillis(time);
    long startTime = System.currentTimeMillis();
    int consecutiveInvalidCount = 0;
    final int maxConsecutiveInvalid = maxPoolSize + 1;

    while (checkedConsumer == null && remainingTime > 0) {
      CheckedConsumer candidate = consumerPool.poll();

      if (candidate != null) {
        if (candidate.isValid(topic)) {
          checkedConsumer = candidate;
          checkedConsumer.setState(CheckedConsumer.ConsumerState.BORROWED);
          consecutiveInvalidCount = 0;
        } else {
          log.warn("Found invalid consumer in pool, closing and removing it");
          closeAndRemoveConsumer(candidate);
          consecutiveInvalidCount++;
        }
      }

      if (checkedConsumer == null) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        remainingTime = timeUnit.toMillis(time) - elapsedTime;

        boolean canCreateMore = false;
        poolManagementLock.lock();
        try {
          canCreateMore = totalConsumersCreated.get() < maxPoolSize && !shuttingDown;
        } finally {
          poolManagementLock.unlock();
        }

        if (canCreateMore) {
          if (consecutiveInvalidCount >= maxConsecutiveInvalid) {
            log.error(
                "Too many consecutive invalid consumers ({}), possible Kafka connectivity issue. "
                    + "Waiting before retrying.",
                consecutiveInvalidCount);
            if (remainingTime > 0) {
              try {
                Thread.sleep(Math.min(1000, remainingTime));
              } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
              }
              consecutiveInvalidCount = 0;
            }
          } else {
            CheckedConsumer newCheckedConsumer = createConsumer();
            if (newCheckedConsumer != null && newCheckedConsumer.isValid(topic)) {
              checkedConsumer = newCheckedConsumer;
              checkedConsumer.setState(CheckedConsumer.ConsumerState.BORROWED);
              consecutiveInvalidCount = 0;
            } else if (newCheckedConsumer != null) {
              log.warn("Newly created consumer is invalid, closing and removing it");
              closeAndRemoveConsumer(newCheckedConsumer);
              consecutiveInvalidCount++;
            }
          }
        }

        if (checkedConsumer == null && !canCreateMore) {
          if (remainingTime > 0) {
            candidate = consumerPool.poll(remainingTime, TimeUnit.MILLISECONDS);
            if (candidate != null && candidate.isValid(topic)) {
              checkedConsumer = candidate;
              checkedConsumer.setState(CheckedConsumer.ConsumerState.BORROWED);
              consecutiveInvalidCount = 0;
            } else if (candidate != null) {
              log.warn("Found invalid consumer while waiting, closing and removing it");
              closeAndRemoveConsumer(candidate);
              consecutiveInvalidCount++;
            }
          }
        }
      }
    }

    return checkedConsumer;
  }

  public void returnConsumer(CheckedConsumer checkedConsumer) {
    if (checkedConsumer == null) {
      return;
    }

    if (shuttingDown) {
      closeAndRemoveConsumer(checkedConsumer);
      return;
    }

    checkedConsumer.setState(CheckedConsumer.ConsumerState.AVAILABLE);
    if (!consumerPool.offer(checkedConsumer)) {
      closeAndRemoveConsumer(checkedConsumer);
    }
  }

  private void closeAndRemoveConsumer(CheckedConsumer checkedConsumer) {
    checkedConsumer.setState(CheckedConsumer.ConsumerState.CLOSED);
    checkedConsumer.close();
    allConsumers.remove(checkedConsumer);
    poolManagementLock.lock();
    try {
      totalConsumersCreated.decrementAndGet();
    } finally {
      poolManagementLock.unlock();
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

    Set<CheckedConsumer> consumersToClose = new HashSet<>(allConsumers);
    allConsumers.clear();
    consumerPool.clear();

    poolManagementLock.lock();
    try {
      for (CheckedConsumer consumer : consumersToClose) {
        consumer.setState(CheckedConsumer.ConsumerState.CLOSED);
        consumer.close();
        totalConsumersCreated.decrementAndGet();
      }
    } finally {
      poolManagementLock.unlock();
    }
  }
}
