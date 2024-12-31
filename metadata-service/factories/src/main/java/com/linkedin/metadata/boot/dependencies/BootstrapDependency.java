package com.linkedin.metadata.boot.dependencies;

/** Empty interface for passing named bean references to bootstrap steps */
public interface BootstrapDependency {

  /**
   * Execute any dependent methods, avoids increasing module dependencies
   *
   * @return true if the dependency has successfully executed its expected methods, false otherwise
   */
  boolean waitForBootstrap();
}
