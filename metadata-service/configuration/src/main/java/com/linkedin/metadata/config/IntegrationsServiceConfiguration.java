package com.linkedin.metadata.config;

import lombok.Data;

/**
 * Configurations related to accessing the Acryl-only Integrations service.
 */
@Data
public class IntegrationsServiceConfiguration {
  /**
   * The host used to communicate with the integrations service.
   */
  public String host;

  /**
   * The port used to communication with the integrations service.
   */
  public int port;

  /**
   * Whether SSL should be used in communicating with the integrations service.
   */
  public boolean useSsl;
}