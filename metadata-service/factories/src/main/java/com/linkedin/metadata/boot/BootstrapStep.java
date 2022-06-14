package com.linkedin.metadata.boot;

import javax.annotation.Nonnull;


/**
 * A single step in the Bootstrap process.
 */
public interface BootstrapStep {

  /**
   * A human-readable name for the boot step.
   */
  String name();

  /**
   * Execute a boot-time step, or throw an exception on failure.
   */
  void execute() throws Exception;

  /**
   * Return the execution mode of this step
   */
  @Nonnull
  default ExecutionMode getExecutionMode() {
    return ExecutionMode.BLOCKING;
  }

  /**
   * Return the execution time of the step
   */
  @Nonnull
  default ExecutionTime getExecutionTime() {
    return ExecutionTime.ON_BOOT;
  }

  enum ExecutionMode {
    // Block service from starting up while running the step
    BLOCKING,
    // Start the step asynchronously without waiting for it to end
    ASYNC,
  }

  enum ExecutionTime {
    // A step which runs when the application first boots.
    ON_BOOT,
    // A step which runs when the application is ready.
    ON_READY,
  }
}
