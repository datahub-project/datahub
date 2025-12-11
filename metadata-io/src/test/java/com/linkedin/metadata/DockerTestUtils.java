/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata;

import com.github.dockerjava.api.DockerClient;

public class DockerTestUtils {

  private static final int MIN_MEMORY_NEEDED_GB = 7;

  public static void checkContainerEngine(DockerClient dockerClient) {
    final long dockerEngineMemoryBytes = dockerClient.infoCmd().exec().getMemTotal();
    final long dockerEngineMemoryGB = dockerEngineMemoryBytes / 1000 / 1000 / 1000;
    if (dockerEngineMemoryGB < MIN_MEMORY_NEEDED_GB) {
      final String error =
          String.format(
              "Total Docker memory configured: %s GB (%d bytes) is below the minimum threshold "
                  + "of %d GB",
              dockerEngineMemoryGB, dockerEngineMemoryBytes, MIN_MEMORY_NEEDED_GB);
      throw new IllegalStateException(error);
    }
  }

  private DockerTestUtils() {}
}
