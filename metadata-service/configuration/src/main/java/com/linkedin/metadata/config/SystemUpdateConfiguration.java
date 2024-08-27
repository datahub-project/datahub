package com.linkedin.metadata.config;

import lombok.Data;

@Data
public class SystemUpdateConfiguration {

  private String initialBackOffMs;
  private String maxBackOffs;
  private String backOffFactor;
  private boolean waitForSystemUpdate;
}
