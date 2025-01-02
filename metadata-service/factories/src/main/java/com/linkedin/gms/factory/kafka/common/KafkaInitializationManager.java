package com.linkedin.gms.factory.kafka.common;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.availability.AvailabilityChangeEvent;
import org.springframework.boot.availability.ReadinessState;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaInitializationManager {
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private final KafkaListenerEndpointRegistry registry;

  @Autowired
  public KafkaInitializationManager(KafkaListenerEndpointRegistry registry) {
    this.registry = registry;
    log.info(
        "Created KafkaInitializationManager. Waiting for application to be ready to enable kafka consumers.");
  }

  @EventListener
  public void onStateChange(AvailabilityChangeEvent<ReadinessState> event) {
    if (event.getState() == ReadinessState.ACCEPTING_TRAFFIC) {
      initialize(this.getClass().getSimpleName());
    }
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
}
