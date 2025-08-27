package com.linkedin.metadata.boot;

import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * Responsible for coordinating boot-time logic.
 *
 * <p>Executes bootstrap steps in two phases: 1. BLOCKING steps - Critical steps that must complete
 * before the service is ready for traffic (e.g., loading admin policies, essential configurations,
 * restoring search indices) 2. ASYNC steps - Background optimization steps that can run after
 * service is ready (e.g., retention policies, background indexing tasks)
 *
 * <p>The health check endpoint uses areBlockingStepsComplete() to determine when the service is
 * ready to accept traffic, ensuring critical functionality is available before routing requests.
 */
@Slf4j
@Component
public class BootstrapManager {

  private final ExecutorService _asyncExecutor = Executors.newFixedThreadPool(5);
  private final List<BootstrapStep> _bootSteps;
  private final AtomicBoolean _blockingStepsComplete = new AtomicBoolean(false);

  public BootstrapManager(final List<BootstrapStep> bootSteps) {
    _bootSteps = bootSteps;
  }

  public void start(@Nonnull OperationContext systemOperationContext) {
    log.info("Starting Bootstrap Process...");

    List<BootstrapStep> stepsToExecute = _bootSteps;

    for (int i = 0; i < stepsToExecute.size(); i++) {
      final BootstrapStep step = stepsToExecute.get(i);
      if (step.getExecutionMode() == BootstrapStep.ExecutionMode.BLOCKING) {
        log.info(
            "Executing bootstrap step {}/{} with name {}...",
            i + 1,
            stepsToExecute.size(),
            step.name());
        try {
          step.execute(systemOperationContext);
        } catch (Exception e) {
          log.error(
              String.format(
                  "Caught exception while executing bootstrap step %s. Exiting...", step.name()),
              e);
          System.exit(1);
        }
      } else { // Async
        log.info(
            "Starting asynchronous bootstrap step {}/{} with name {}...",
            i + 1,
            stepsToExecute.size(),
            step.name());
        CompletableFuture.runAsync(
            () -> {
              try {
                step.execute(systemOperationContext);
              } catch (Exception e) {
                log.error(
                    String.format(
                        "Caught exception while executing bootstrap step %s. Continuing...",
                        step.name()),
                    e);
              }
            },
            _asyncExecutor);
      }
    }

    // Mark blocking steps as complete
    _blockingStepsComplete.set(true);
    log.info("Bootstrap blocking steps completed. Service is ready for traffic.");
  }

  /**
   * Returns true if all blocking bootstrap steps have completed. This is used by health checks to
   * determine if the service is ready for traffic.
   */
  public boolean areBlockingStepsComplete() {
    return _blockingStepsComplete.get();
  }
}
