package com.linkedin.metadata.config;

import java.util.Optional;

public final class BootstrapConfigurationSupport {

  private BootstrapConfigurationSupport() {}

  public static int requireAsyncWorkerThreads(DataHubAppConfiguration configuration) {
    Integer threads =
        Optional.ofNullable(configuration)
            .map(DataHubAppConfiguration::getBootstrap)
            .map(BootstrapConfiguration::getAsync)
            .map(BootstrapConfiguration.BootstrapAsyncConfiguration::getWorkerThreads)
            .orElse(null);
    if (threads == null) {
      throw new IllegalStateException(
          "bootstrap.async.workerThreads must be set in application.yaml");
    }
    if (threads < 1) {
      throw new IllegalStateException("bootstrap.async.workerThreads must be >= 1, got " + threads);
    }
    return threads;
  }
}
