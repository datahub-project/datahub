/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.dao.throttle;

import java.util.concurrent.TimeUnit;

public class APIThrottleException extends RuntimeException {
  private final long durationMs;

  public APIThrottleException(long durationMs, String message) {
    super(message);
    this.durationMs = durationMs;
  }

  public long getDurationMs() {
    return durationMs;
  }

  public long getDurationSeconds() {
    return TimeUnit.MILLISECONDS.toSeconds(durationMs);
  }
}
