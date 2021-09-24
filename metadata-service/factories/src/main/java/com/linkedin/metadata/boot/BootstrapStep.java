package com.linkedin.metadata.boot;

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

}
