package com.linkedin.gms.factory.kafka.common;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaInitializationManager {
  /**
   * Jetty graceful shutdown is {@code SmartLifecycle.DEFAULT_PHASE - 1024} and stops first. One
   * below that, so {@code connector.shutdown()} runs before this registry's {@code stop()} waits on
   * in-flight records.
   */
  static final int SHUTDOWN_PHASE = SmartLifecycle.DEFAULT_PHASE - 1024 - 1;

  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private final KafkaListenerEndpointRegistry registry;

  @Autowired
  public KafkaInitializationManager(KafkaListenerEndpointRegistry registry) {
    this.registry = registry;
    movePhaseAfterJetty(registry);
    log.info(
        "Created KafkaInitializationManager. Waiting for application to be ready to enable kafka consumers.");
  }

  @EventListener
  public void onStateChange(AvailabilityChangeEvent<ReadinessState> event) {
    if (event.getState() == ReadinessState.ACCEPTING_TRAFFIC) {
      initialize(this.getClass().getSimpleName());
    }
  }

  @Order(Ordered.HIGHEST_PRECEDENCE)
  @EventListener(ContextClosedEvent.class)
  public void onContextClosed(ContextClosedEvent event) {
    int paused = 0;
    for (MessageListenerContainer container : registry.getAllListenerContainers()) {
      if (container.isRunning()) {
        // pause not stop: stop() waits on the current records. Registry phase is after Jetty,
        // so without pause those polls continue during the HTTP drain.
        container.pause();
        paused++;
      }
    }
    log.info("Paused {} kafka listener container(s) on context close.", paused);
  }

  public void initialize(String initializerName) {
    if (isInitialized.compareAndSet(false, true)) {
      int containerCount = registry.getAllListenerContainers().size();
      log.info("Starting {} kafka consumers. Initialized by {}", containerCount, initializerName);
      registry
          .getAllListenerContainers()
          .forEach(
              container -> {
                if (!container.isRunning()) {
                  container.start();
                  log.info("Started container: {}", container.getListenerId());
                }
              });
      log.info("All {} kafka containers started.", containerCount);
    }
  }

  public boolean isInitialized() {
    return isInitialized.get();
  }

  /**
   * No setter. {@code autoStartup=false} means the registry never copies a factory phase and stays
   * at {@code DEFAULT_PHASE - 100}, which stops Kafka before Jetty. Skip Mockito subclasses; the
   * production bean is the exact class.
   */
  static void movePhaseAfterJetty(KafkaListenerEndpointRegistry registry) {
    if (registry.getClass() != KafkaListenerEndpointRegistry.class) {
      return;
    }
    try {
      java.lang.reflect.Field phase = KafkaListenerEndpointRegistry.class.getDeclaredField("phase");
      phase.setAccessible(true);
      phase.setInt(registry, SHUTDOWN_PHASE);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException(
          "Kafka listener registry must stop after Jetty has refused new HTTP", e);
    }
  }
}
