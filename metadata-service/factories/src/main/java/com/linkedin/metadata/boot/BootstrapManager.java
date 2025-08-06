package com.linkedin.metadata.boot;

import io.datahubproject.metadata.context.OperationContext;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
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
 * <p>Bootstrap consists of two phases: 1. BLOCKING steps - Critical steps that must complete before
 * the service is ready for traffic - IngestPoliciesStep: Loads default access policies (CRITICAL
 * for admin privileges) - IngestDataPlatformInstancesStep: Loads data platform configurations -
 * IngestSettingsStep: Loads default global settings - RestoreGlossaryIndicesStep: Restores glossary
 * search indices - IndexDataPlatformsStep: Indexes data platforms for search -
 * RestoreColumnLineageIndicesStep: Restores column lineage indices - IngestEntityTypesStep: Loads
 * entity type definitions - RestoreFormInfoIndicesStep: Restores form metadata indices - And other
 * critical infrastructure setup steps
 *
 * <p>2. ASYNC steps - Background optimization steps that can run after service is ready -
 * IngestRetentionPoliciesStep: Loads retention policies (non-critical for basic functionality)
 *
 * <p>The health check endpoint waits for BLOCKING steps to complete before marking the service as
 * ready. This ensures that critical functionality (like admin authentication) is available before
 * traffic is routed.
 */
@Slf4j
@Component
public class BootstrapManager {

  private final ExecutorService _asyncExecutor = Executors.newFixedThreadPool(5);
  private final List<BootstrapStep> _bootSteps;

  // Bootstrap completion tracking
  private final AtomicBoolean _blockingStepsComplete = new AtomicBoolean(false);
  private final AtomicBoolean _allStepsComplete = new AtomicBoolean(false);
  private final List<CompletableFuture<Void>> _asyncFutures = new ArrayList<>();

  public BootstrapManager(final List<BootstrapStep> bootSteps) {
    _bootSteps = bootSteps;
  }

  public void start(@Nonnull OperationContext systemOperationContext) {
    log.info("Starting Bootstrap Process...");

    List<BootstrapStep> stepsToExecute = _bootSteps;

    // Phase 1: Execute all BLOCKING steps synchronously
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
      }
    }

    // Mark blocking steps as complete - service is now ready for traffic
    _blockingStepsComplete.set(true);
    log.info("✅ Bootstrap BLOCKING steps completed. Service is ready for traffic.");

    // Phase 2: Start all ASYNC steps in background
    for (int i = 0; i < stepsToExecute.size(); i++) {
      final BootstrapStep step = stepsToExecute.get(i);
      if (step.getExecutionMode() == BootstrapStep.ExecutionMode.ASYNC) {
        log.info(
            "Starting asynchronous bootstrap step {}/{} with name {}...",
            i + 1,
            stepsToExecute.size(),
            step.name());
        CompletableFuture<Void> asyncFuture =
            CompletableFuture.runAsync(
                () -> {
                  try {
                    step.execute(systemOperationContext);
                    log.info("✅ Completed async bootstrap step: {}", step.name());
                  } catch (Exception e) {
                    log.error(
                        String.format(
                            "Caught exception while executing bootstrap step %s. Continuing...",
                            step.name()),
                        e);
                  }
                },
                _asyncExecutor);
        _asyncFutures.add(asyncFuture);
      }
    }

    // Set up completion tracking for all async steps
    if (!_asyncFutures.isEmpty()) {
      CompletableFuture.allOf(_asyncFutures.toArray(new CompletableFuture[0]))
          .whenComplete(
              (result, throwable) -> {
                _allStepsComplete.set(true);
                log.info("✅ Bootstrap ALL steps completed (including async background tasks).");
              });
    } else {
      // No async steps, so all steps are complete
      _allStepsComplete.set(true);
      log.info("✅ Bootstrap ALL steps completed (no async steps configured).");
    }
  }

  /**
   * Returns true if all BLOCKING bootstrap steps have completed.
   *
   * <p>When this returns true, the service is ready to accept traffic because: - Admin policies are
   * loaded (authentication works) - Essential configurations are in place - Search indices are
   * restored - Core functionality is available
   *
   * <p>This is the recommended readiness check for health endpoints and load balancers.
   */
  public boolean areBlockingStepsComplete() {
    return _blockingStepsComplete.get();
  }

  /**
   * Returns true if ALL bootstrap steps (including async background tasks) have completed.
   *
   * <p>This is typically not needed for readiness checks, as async steps are optimizations that
   * don't affect core functionality. Use areBlockingStepsComplete() for health checks.
   */
  public boolean areAllStepsComplete() {
    return _allStepsComplete.get();
  }

  /** Returns a list of all configured bootstrap steps for debugging/monitoring purposes. */
  public List<String> getBootstrapStepNames() {
    return _bootSteps.stream().map(BootstrapStep::name).toList();
  }
}
