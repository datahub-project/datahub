/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * This file is unmodified from its original version developed by Acryl Data, Inc.,
 * and is now included as part of a repository maintained by the National Digital Twin Programme.
 * All support, maintenance and further development of this code is now the responsibility
 * of the National Digital Twin Programme.
 */

package com.linkedin.metadata.config;

import lombok.Data;

/** POJO representing the "datahub.gms" configuration block in application.yaml. */
@Data
public class GMSConfiguration {
  /** The host where GMS is running */
  private String host;

  /** The port where GMS is running */
  private Integer port;

  /** The base path for GMS endpoints */
  private String basePath;

  /** Whether base path is enabled for GMS */
  private Boolean basePathEnabled;

  /** Whether to use SSL for GMS connections */
  private Boolean useSSL;

  /** Truststore configuration for SSL */
  private TruststoreConfiguration truststore;

  /** Async request configuration */
  private AsyncConfiguration async;

  /** The complete URI for GMS (takes precedence over host/port/useSSL) */
  private String uri;

  /** SSL context protocol */
  private String sslContext;

  @Data
  public static class TruststoreConfiguration {
    /** Path to the truststore file */
    private String path;

    /** Password for the truststore */
    private String password;

    /** Type of the truststore (e.g., PKCS12) */
    private String type;
  }

  @Data
  public static class AsyncConfiguration {
    /** Request timeout in milliseconds */
    private Long requestTimeoutMs;
  }
}
