package com.linkedin.metadata.boot;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;


/**
 * Responsible for coordinating boot-time logic.
 */
@Slf4j
@Component
public class BootstrapManager {

  private final List<BootstrapStep> _bootSteps;

  public BootstrapManager(final List<BootstrapStep> bootSteps) {
    _bootSteps = bootSteps;
  }

  public void start() {
    // Once the application has been set up, apply boot steps.
    log.info("Starting Bootstrap Process...");
    for (int i = 0; i < _bootSteps.size(); i++) {
      final BootstrapStep step = _bootSteps.get(i);
      if (step.getExecutionMode() == BootstrapStep.ExecutionMode.BLOCKING) {
        log.info("Executing bootstrap step {}/{} with name {}...", i + 1, _bootSteps.size(), step.name());
        try {
          step.execute();
        } catch (Exception e) {
          log.error(String.format("Caught exception while executing bootstrap step %s. Exiting...", step.name()), e);
          System.exit(1);
        }
      } else { // Async
        log.info("Starting asynchronous bootstrap step {}/{} with name {}...", i + 1, _bootSteps.size(), step.name());
        CompletableFuture.runAsync(() -> {
          try {
            step.execute();
          } catch (Exception e) {
            log.error(String.format("Caught exception while executing bootstrap step %s. Exiting...", step.name()), e);
          }
        });
      }
    }
  }
}
