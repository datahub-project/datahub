package io.datahubproject.event.kafka;

import com.linkedin.metadata.utils.metrics.MetricUtils;
import java.time.Duration;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;

@Slf4j
@Getter
public class CheckedConsumer {
  public enum ConsumerState {
    AVAILABLE,
    BORROWED,
    CLOSED
  }

  private final KafkaConsumer<String, GenericRecord> consumer;
  private volatile long lastValidationTime;
  private volatile ConsumerState state;
  private final Duration validationTimeout;
  private final Duration validationCacheInterval;
  @Nullable private final MetricUtils metricUtils;

  public CheckedConsumer(
      KafkaConsumer<String, GenericRecord> consumer,
      Duration validationTimeout,
      Duration validationCacheInterval,
      @Nullable MetricUtils metricUtils) {
    this.consumer = consumer;
    this.validationTimeout = validationTimeout;
    this.validationCacheInterval = validationCacheInterval;
    this.metricUtils = metricUtils;
    this.lastValidationTime = 0;
    this.state = ConsumerState.AVAILABLE;
  }

  public void setState(ConsumerState newState) {
    this.state = newState;
  }

  public ConsumerState getState() {
    return state;
  }

  public boolean isValid(@Nonnull String topic) {
    if (consumer == null || topic == null || topic.isEmpty()) {
      return false;
    }

    long currentTime = System.currentTimeMillis();
    long cacheIntervalMillis = validationCacheInterval.toMillis();

    if (lastValidationTime > 0 && (currentTime - lastValidationTime) < cacheIntervalMillis) {
      return true;
    }

    boolean isValid = performValidation(topic);
    if (isValid) {
      lastValidationTime = currentTime;
    } else {
      lastValidationTime = 0;
    }

    return isValid;
  }

  private boolean performValidation(@Nonnull String topic) {
    try {
      consumer.assignment();
      consumer.partitionsFor(topic, validationTimeout);
      return true;
    } catch (IllegalStateException e) {
      log.debug("Consumer validation failed: consumer is closed or invalid", e);
      recordInvalidConsumer();
      return false;
    } catch (KafkaException e) {
      log.debug(
          "Consumer validation encountered Kafka exception (may be transient): {}", e.getMessage());
      return true;
    } catch (Exception e) {
      log.debug("Consumer validation encountered exception (may be transient): {}", e.getMessage());
      return true;
    }
  }

  private void recordInvalidConsumer() {
    if (metricUtils != null) {
      metricUtils.increment(this.getClass(), "invalid_consumer_found", 1);
    }
  }

  public void close() {
    try {
      consumer.close();
    } catch (Exception e) {
      log.warn("Error closing consumer", e);
    }
  }
}
