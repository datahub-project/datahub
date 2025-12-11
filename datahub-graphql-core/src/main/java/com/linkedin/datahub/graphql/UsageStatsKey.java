/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql;

import com.linkedin.usage.UsageTimeRange;
import lombok.Data;

@Data
public class UsageStatsKey {
  private String resource;
  private UsageTimeRange range;

  public UsageStatsKey(String resource, UsageTimeRange range) {
    this.resource = resource;
    this.range = range;
  }
}
