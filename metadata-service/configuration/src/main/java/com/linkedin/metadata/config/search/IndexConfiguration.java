/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class IndexConfiguration {
  private String prefix;
  private DocIdsConfiguration docIds;
  private int minSearchFilterLength;

  // Reindex configuration flags
  private boolean enableSettingsReindex;
  private boolean enableMappingsReindex;

  // Index structure configuration
  private int numShards;
  private int numReplicas;
  private int numRetries;
  private int refreshIntervalSeconds;
  private int maxReindexHours;
  private String mainTokenizer;

  // Index limits
  private int maxArrayLength;
  private int maxObjectKeys;
  private int maxValueLength;

  public String getFinalPrefix() {
    if (prefix == null || prefix.isEmpty()) {
      return "";
    } else {
      return prefix + "_";
    }
  }
}
