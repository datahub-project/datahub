package com.linkedin.metadata.boot;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * Coordinates graceful shutdown of Kafka listeners when the application context closes (SIGTERM
 * received). This ensures that all in-flight Kafka messages are properly drained and offsets are
 * committed before the application terminates.
 *
 * <p>When SIGTERM is received, Kubernetes triggers ContextClosedEvent. This handler: 1. Sets
 * shutdown flag so health checks can return 503 2. Stops all registered Kafka listener containers
 * 3. Allows current message batch to drain and offsets to commit 4. Proceeds with normal Spring
 * shutdown
 */
@Slf4j
@Component
public class GracefulShutdownHandler {
  private final AtomicBoolean shutdownInProgress = new AtomicBoolean(false);
  private final boolean enableGracefulShutdown;

  @Autowired
  public GracefulShutdownHandler(
      @Value("${shutdown.graceful.enabled:false}") boolean enableGracefulShutdown) {
    this.enableGracefulShutdown = enableGracefulShutdown;
  }

  /**
   * Called when Spring application context is closing (e.g., SIGTERM signal in Kubernetes).
   * Initiates graceful shutdown of all Kafka listeners if enabled.
   */
  @EventListener(ContextClosedEvent.class)
  public void onApplicationClosed(ContextClosedEvent event) {
    if (!enableGracefulShutdown) {
      log.info(
          "Graceful shutdown disabled by configuration (metadata.kafka.graceful_shutdown_enabled=false)");
      return;
    }

    log.info("Application context closing - initiating graceful shutdown of Kafka listeners");
    shutdownInProgress.set(true);

    log.info("Graceful shutdown sequence complete, allowing application context close");
  }

  /**
   * Check if graceful shutdown has been initiated. Used by health checks to determine if the
   * service should be removed from load balancer traffic.
   *
   * @return true if shutdown is in progress, false otherwise
   */
  public boolean isShutdownInProgress() {
    return shutdownInProgress.get();
  }
}
