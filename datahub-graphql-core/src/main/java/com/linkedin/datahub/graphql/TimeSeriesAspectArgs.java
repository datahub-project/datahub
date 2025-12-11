/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.datahub.graphql;

import com.linkedin.datahub.graphql.generated.TimeRange;
import lombok.Data;

@Data
public class TimeSeriesAspectArgs {
  private String aspectName;
  private String urn;
  private Long count;
  private TimeRange timeRange;

  public TimeSeriesAspectArgs(String urn, String aspectName, Long count, TimeRange timeRange) {
    this.urn = urn;
    this.aspectName = aspectName;
    this.count = count;
    this.timeRange = timeRange;
  }
}
