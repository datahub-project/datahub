package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "controlPlane" configuration block in application.yaml. */
@Data
public class ControlPlaneConfiguration {
  /** The base URL of the control plane service */
  private String url;

  /** The API key for authenticating with the control plane service */
  private String apiKey;

  /** The cache TTL in minutes for trial expiration data (default: 15 minutes) */
  private Long cacheTtlMinutes = 15L;
}
