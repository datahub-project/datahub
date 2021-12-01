package com.linkedin.metadata;

import com.github.dockerjava.api.DockerClient;

public class DockerTestUtils {

    final private static int MIN_MEMORY_NEEDED_GB = 8;

    public static void checkContainerEngine(DockerClient dockerClient) {
        final long dockerEngineMemory = dockerClient.infoCmd().exec().getMemTotal() /1024/1024/1024;
        if ( dockerEngineMemory < MIN_MEMORY_NEEDED_GB) {
            final String error = String.format("Total Docker memory configured: %s GB is below the minimum threshold of %d GB", dockerEngineMemory, MIN_MEMORY_NEEDED_GB);
            throw new IllegalStateException(error);
        }
    }
}
