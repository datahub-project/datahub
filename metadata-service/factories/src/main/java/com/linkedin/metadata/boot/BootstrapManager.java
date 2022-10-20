package com.linkedin.metadata.boot;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;


/**
 * Responsible for coordinating boot-time logic.
 */
@Slf4j
@Component
public class BootstrapManager {

  private final ExecutorService _asyncExecutor = Executors.newFixedThreadPool(5);
  private final List<BootstrapStep> _bootSteps;

  public BootstrapManager(final List<BootstrapStep> bootSteps) {
    _bootSteps = bootSteps;
  }

  public void start() {
    log.info("Starting Bootstrap Process...");

    List<BootstrapStep> stepsToExecute = _bootSteps;

    for (int i = 0; i < stepsToExecute.size(); i++) {
      final BootstrapStep step = stepsToExecute.get(i);
      if (step.getExecutionMode() == BootstrapStep.ExecutionMode.BLOCKING) {
        log.info("Executing bootstrap step {}/{} with name {}...", i + 1, stepsToExecute.size(), step.name());
        try {
          step.execute();
        } catch (Exception e) {
          log.error(String.format("Caught exception while executing bootstrap step %s. Exiting...", step.name()), e);
          System.exit(1);
        }
      } else { // Async
        log.info("Starting asynchronous bootstrap step {}/{} with name {}...", i + 1, stepsToExecute.size(), step.name());
        CompletableFuture.runAsync(() -> {
          try {
            step.execute();
          } catch (Exception e) {
            log.error(String.format("Caught exception while executing bootstrap step %s. Continuing...", step.name()), e);
          }
        }, _asyncExecutor);
      }
    }
  }
}
