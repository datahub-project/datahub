package com.linkedin.metadata.config;

import lombok.Data;

@Data
@SuppressWarnings("JavadocLinkAsPlainText")
public class SystemUpdateConfiguration {

  private String initialBackOffMs;
  private String maxBackOffs;
  private String backOffFactor;
  private boolean waitForSystemUpdate;
  private boolean cdcMode;

  /** Entity consistency checking configuration */
  private EntityConsistencyConfiguration entityConsistency;
}
