/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

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
