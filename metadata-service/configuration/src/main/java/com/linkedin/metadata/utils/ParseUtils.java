/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.utils;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

public class ParseUtils {
  private ParseUtils() {}

  public static Duration parseDuration(int value, String unit) {
    TimeUnit timeUnit = unit != null ? TimeUnit.valueOf(unit.toUpperCase()) : TimeUnit.SECONDS;
    return Duration.of(value, timeUnit.toChronoUnit());
  }
}
