/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.timeline;

import lombok.Builder;
import lombok.Getter;

@Builder
public class SemanticVersion {
  @Getter private int majorVersion;
  @Getter private int minorVersion;
  @Getter private int patchVersion;
  @Getter private String qualifier;

  public String toString() {
    return String.format(
        String.format("%d.%d.%d-%s", majorVersion, minorVersion, patchVersion, qualifier));
  }
}
