/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config.kafka;

import com.linkedin.metadata.config.MetricsOptions;
import lombok.Data;

@Data
public class ConsumerConfiguration {

  private int maxPartitionFetchBytes;
  private boolean stopOnDeserializationError;
  private boolean healthCheckEnabled;

  private ConsumerOptions mcp;
  private ConsumerOptions mcl;
  private ConsumerOptions pe;

  private MetricsOptions metrics;

  @Data
  public static class ConsumerOptions {
    private String autoOffsetReset;
    private boolean fineGrainedLoggingEnabled;
    private String aspectsToDrop;
  }
}
