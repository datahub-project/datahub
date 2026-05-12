package com.linkedin.gms.factory.kafka;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.event.ConsumerRetryAuthEvent;
import org.springframework.kafka.event.ConsumerRetryAuthSuccessfulEvent;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

/**
 * Bounds the number of consecutive authentication/authorization retries for Kafka consumers.
 *
 * <p>Without this guard, {@code authExceptionRetryInterval} causes consumers to retry indefinitely
 * on auth failures — the container stays "running" and the health check never detects the problem.
 *
 * <p>This mirrors how the GMS producer bounds retries via {@code delivery.timeout.ms} and {@code
 * retries}: after a fixed number of consecutive failures the operation fails loudly instead of
 * retrying forever. On the consumer side, "failing loudly" means stopping the container so that
 * {@link io.datahubproject.metadata.jobs.common.health.kafka.KafkaHealthIndicator} reports DOWN and
 * Kubernetes liveness probes can act on it.
 *
 * <p>Transient failures (e.g., MSK IAM credential rotation) typically recover on the first retry.
 * Reaching the max retry threshold signals a permanent auth failure that requires operator
 * intervention.
 *
 * <p>Configurable via {@code kafka.consumer.maxAuthExceptionRetries} / {@code
 * KAFKA_CONSUMER_MAX_AUTH_EXCEPTION_RETRIES}. Set to 0 to disable (unbounded retries).
 */
@Component
@Slf4j
public class KafkaConsumerAuthExceptionGuard {

  private final int maxConsecutiveAuthRetries;

  private final ConcurrentHashMap<Object, AtomicInteger> consecutiveFailures =
      new ConcurrentHashMap<>();

  public KafkaConsumerAuthExceptionGuard(
      @Qualifier("configurationProvider") ConfigurationProvider configurationProvider) {
    this.maxConsecutiveAuthRetries =
        configurationProvider.getKafka().getConsumer().getMaxAuthExceptionRetries();
  }

  @EventListener
  void onAuthRetry(ConsumerRetryAuthEvent event) {
    if (maxConsecutiveAuthRetries <= 0) {
      return;
    }

    MessageListenerContainer container = event.getContainer(MessageListenerContainer.class);
    int failures =
        consecutiveFailures.computeIfAbsent(container, k -> new AtomicInteger(0)).incrementAndGet();

    log.warn(
        "Kafka consumer auth retry {}/{} for container [{}] (reason: {})",
        failures,
        maxConsecutiveAuthRetries,
        container.getListenerId(),
        event.getReason());

    if (failures >= maxConsecutiveAuthRetries) {
      log.error(
          "Kafka consumer [{}] exhausted auth retries ({}/{}), stopping container."
              + " This indicates a permanent authentication/authorization failure"
              + " — not a transient credential rotation.",
          container.getListenerId(),
          failures,
          maxConsecutiveAuthRetries);
      consecutiveFailures.remove(container);
      // Stop asynchronously: the event fires on the consumer thread, and a synchronous
      // stop() would deadlock (the thread cannot join itself).
      CompletableFuture.runAsync(() -> container.stop());
    }
  }

  @EventListener
  void onAuthRetrySuccess(ConsumerRetryAuthSuccessfulEvent event) {
    MessageListenerContainer container = event.getContainer(MessageListenerContainer.class);
    if (consecutiveFailures.remove(container) != null) {
      log.info(
          "Kafka consumer [{}] recovered from auth failure — credential rotation succeeded",
          container.getListenerId());
    }
  }
}
