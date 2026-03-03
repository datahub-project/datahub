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
