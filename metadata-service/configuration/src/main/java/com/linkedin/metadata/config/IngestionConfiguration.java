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

/** POJO representing the "ingestion" configuration block in application.yaml. */
@Data
public class IngestionConfiguration {
  /** Whether managed ingestion is enabled */
  private boolean enabled;

  /** The default CLI version to use in managed ingestion */
  private String defaultCliVersion;

  private Integer batchRefreshCount;
}
