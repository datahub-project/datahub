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
