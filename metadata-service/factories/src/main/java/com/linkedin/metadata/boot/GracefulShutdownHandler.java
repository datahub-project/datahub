package com.linkedin.metadata.boot;

import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 * When SIGTERM is received, Kubernetes triggers ContextClosedEvent. This handler: 1. Sets shutdown
 * flag so health checks can return 503 2. Proceeds with normal Spring shutdown
 *
 * <p>Only registered when server.shutdown=graceful (or SERVER_SHUTDOWN_MODE=graceful env var).
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "server.shutdown", havingValue = "graceful", matchIfMissing = false)
public class GracefulShutdownHandler {
  private final AtomicBoolean shutdownInProgress = new AtomicBoolean(false);

  /**
   * Called when Spring application context is closing (e.g., SIGTERM signal in Kubernetes). This
   * handler only exists when server.shutdown=graceful is configured (see @ConditionalOnProperty).
   */
  @EventListener(ContextClosedEvent.class)
  public void onApplicationClosed(ContextClosedEvent event) {
    log.info("Application context closing - initiating graceful shutdown");
    shutdownInProgress.set(true);
    log.info("Shutdown flag set - health checks will now return 503");
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
