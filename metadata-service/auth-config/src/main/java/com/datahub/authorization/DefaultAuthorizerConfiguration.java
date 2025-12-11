/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.datahub.authorization;

import lombok.Data;

@Data
public class DefaultAuthorizerConfiguration {
  /** Whether authorization via DataHub policies is enabled. */
  private boolean enabled;

  /** The duration between policies cache refreshes. */
  private int cacheRefreshIntervalSecs;
}
