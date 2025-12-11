/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.Accessors;

/**
 * Configuration for Metadata Change Log (MCL) processing. Similar to MetadataChangeProposalConfig
 * but for MCL events.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
@Accessors(chain = true)
public class MetadataChangeLogConfig {

  /** Consumer configuration for MCL processing */
  private ConsumerBatchConfig consumer;

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class ConsumerBatchConfig {
    /** Batch processing configuration */
    private BatchConfig batch;
  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  @Builder(toBuilder = true)
  @Accessors(chain = true)
  public static class BatchConfig {
    /** Whether batch processing is enabled */
    private boolean enabled;

    /** Maximum batch size in bytes */
    private Integer size;
  }
}
