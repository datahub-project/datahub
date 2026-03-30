package com.linkedin.metadata.boot;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * When SIGTERM is received, Kubernetes triggers ContextClosedEvent. This handler: 1. Sets shutdown
 * flag so health checks can return 503 2. Allows current message batch to drain and offsets to
 * commit 4. Proceeds with normal Spring shutdown
 */
@Slf4j
@Component
@ConditionalOnProperty(
    name = "shutdown.graceful.enabled",
    havingValue = "true",
    matchIfMissing = false)
public class GracefulShutdownHandler {
  private final AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

  public GracefulShutdownHandler() {}

  /**
   * Called when Spring application context is closing (e.g., SIGTERM signal in Kubernetes).
   * Initiates graceful shutdown of all Kafka listeners.
   */
  @EventListener(ContextClosedEvent.class)
  public void onApplicationClosed(ContextClosedEvent event) {
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
