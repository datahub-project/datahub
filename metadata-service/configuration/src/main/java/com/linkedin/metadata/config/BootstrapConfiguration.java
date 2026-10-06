package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class BootstrapConfiguration {

  private BootstrapAsyncConfiguration async;

  @Data
  public static class BootstrapAsyncConfiguration {
    private Integer workerThreads;
  }
}
