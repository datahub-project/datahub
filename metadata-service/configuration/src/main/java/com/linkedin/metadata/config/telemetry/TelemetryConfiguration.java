/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config.telemetry;

import lombok.Data;

/** POJO representing the "telemetry" configuration block in application.yaml. */
@Data
public class TelemetryConfiguration {
  /** Whether cli telemetry is enabled */
  public boolean enabledCli;

  /** Whether reporting telemetry is enabled */
  public boolean enabledIngestion;

  /** Whether or not third party logging should be enabled for this instance */
  public boolean enableThirdPartyLogging;

  /** Whether or not server telemetry should be enabled */
  public boolean enabledServer;
}
