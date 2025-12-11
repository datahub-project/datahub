/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "tests" configuration block in application.yaml.on.yml */
@Data
public class TestsConfiguration {
  /** Whether tests are enabled */
  public boolean enabled;
}
