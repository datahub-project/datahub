/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config.search;

import com.linkedin.metadata.utils.ParseUtils;
import java.time.Duration;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Data
@Builder(toBuilder = true)
@AllArgsConstructor
@NoArgsConstructor
public class BulkDeleteConfiguration {
  private int batchSize;
  private String slices;
  private int pollInterval;
  private String pollIntervalUnit;
  private int timeout;
  private String timeoutUnit;
  private int numRetries;

  public Duration getPollDuration() {
    return ParseUtils.parseDuration(pollInterval, pollIntervalUnit);
  }

  public Duration getTimeoutDuration() {
    return ParseUtils.parseDuration(timeout, timeoutUnit);
  }
}
