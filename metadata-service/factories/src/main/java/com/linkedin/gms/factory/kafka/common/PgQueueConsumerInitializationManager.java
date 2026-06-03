package com.linkedin.gms.factory.kafka.common;

import com.linkedin.metadata.config.messaging.PgQueueMessagingTransportCondition;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Starts pgQueue consumer pollers after Spring wiring. In GMS this is invoked from {@link
 * com.linkedin.metadata.boot.OnBootApplicationListener}; in standalone consumer pods (MCE/MAE) that
 * do not scan {@code com.linkedin.metadata.boot}, the {@link ReadinessState#ACCEPTING_TRAFFIC}
 * event serves as the fallback trigger (same pattern as {@link KafkaInitializationManager}).
 */
@Slf4j
@Component
@Conditional(PgQueueMessagingTransportCondition.class)
public class PgQueueConsumerInitializationManager {

  private final AtomicBoolean started = new AtomicBoolean(false);
  private final List<Runnable> starters = new CopyOnWriteArrayList<>();

  /**
   * Registers a callback that creates and starts a poller thread (or equivalent). If {@link
   * #initialize} already ran, the starter runs immediately.
   */
  public void whenStarted(Runnable starter) {
    starters.add(starter);
    if (started.get()) {
      starter.run();
    }
  }

  @EventListener
  public void onStateChange(AvailabilityChangeEvent<ReadinessState> event) {
    if (event.getState() == ReadinessState.ACCEPTING_TRAFFIC) {
      initialize(this.getClass().getSimpleName());
    }
  }

  public void initialize(String initializerName) {
    if (started.compareAndSet(false, true)) {
      log.info(
          "Starting {} pgQueue consumer pollers (initialized by {})",
          starters.size(),
          initializerName);
      starters.forEach(Runnable::run);
    }
  }

  public boolean isInitialized() {
    return started.get();
  }
}
