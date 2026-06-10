package com.linkedin.metadata.kafka;

import java.util.Map;
import java.util.function.Function;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.KafkaException;

/** Retries Kafka producer construction for transient initialization failures (e.g. DNS races). */
@Slf4j
public final class KafkaProducerInitializationRetry {

  private KafkaProducerInitializationRetry() {}

  @Value
  @Builder
  public static class Settings {
    /** Total construction attempts, including the first try. Values below 1 are treated as 1. */
    @Builder.Default int maxAttempts = 5;

    @Builder.Default long initialBackoffMs = 500;

    /** Upper bound for a single sleep between attempts. */
    @Builder.Default long maxBackoffMs = 4000;

    /** Upper bound on cumulative sleep time across all retries. */
    @Builder.Default long maxTotalWaitMs = 15000;

    public int effectiveMaxAttempts() {
      return Math.max(1, maxAttempts);
    }
  }

  public static <K, V> Producer<K, V> createWithRetry(
      Map<String, Object> props,
      Settings settings,
      Function<Map<String, Object>, Producer<K, V>> producerFactory) {
    int maxAttempts = settings.effectiveMaxAttempts();
    long nextBackoffMs = settings.getInitialBackoffMs();
    long maxBackoffMs = Math.max(0, settings.getMaxBackoffMs());
    long maxTotalWaitMs = Math.max(0, settings.getMaxTotalWaitMs());
    long totalWaitMs = 0;
    KafkaException lastException = null;

    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        return producerFactory.apply(props);
      } catch (KafkaException e) {
        lastException = e;
        if (attempt >= maxAttempts) {
          break;
        }

        long sleepMs = Math.min(nextBackoffMs, maxBackoffMs);
        if (maxTotalWaitMs > 0 && totalWaitMs + sleepMs > maxTotalWaitMs) {
          sleepMs = maxTotalWaitMs - totalWaitMs;
        }
        if (sleepMs <= 0) {
          log.warn(
              "Failed to construct Kafka producer and retry wait budget exhausted (attempt {}/{}):"
                  + " {}",
              attempt,
              maxAttempts,
              e.getMessage());
          break;
        }

        log.warn(
            "Failed to construct Kafka producer, retrying in {}ms (attempt {}/{}): {}",
            sleepMs,
            attempt,
            maxAttempts,
            e.getMessage());
        try {
          Thread.sleep(sleepMs);
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          throw e;
        }
        totalWaitMs += sleepMs;
        nextBackoffMs = Math.min(nextBackoffMs * 2, maxBackoffMs);
      }
    }

    log.error("Failed to construct Kafka producer after {} attempts", maxAttempts);
    throw lastException;
  }
}
