package com.linkedin.metadata.config.search;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Per-cluster TLS material ({@code elasticsearch.clusters.<name>.sslContext}). Required only when
 * the cluster URI uses https and the cluster presents a certificate that is not already trusted by
 * the JVM.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder(toBuilder = true)
public class SslContextSettings {
  private String protocol;
  private String secureRandomImplementation;
  private String trustStoreFile;
  private String trustStoreType;
  private String trustStorePassword;
  private String keyStoreFile;
  private String keyStoreType;
  private String keyStorePassword;
  private String keyPassword;

  /**
   * Unset TLS settings read as null rather than the empty string, so that "not configured" is
   * distinguishable from "configured with an empty value" when deciding whether to load a store.
   */
  public String getProtocol() {
    return SearchClusterSettings.blankToNull(protocol);
  }

  public String getSecureRandomImplementation() {
    return SearchClusterSettings.blankToNull(secureRandomImplementation);
  }

  public String getTrustStoreFile() {
    return SearchClusterSettings.blankToNull(trustStoreFile);
  }

  public String getTrustStoreType() {
    return SearchClusterSettings.blankToNull(trustStoreType);
  }

  public String getTrustStorePassword() {
    return SearchClusterSettings.blankToNull(trustStorePassword);
  }

  public String getKeyStoreFile() {
    return SearchClusterSettings.blankToNull(keyStoreFile);
  }

  public String getKeyStoreType() {
    return SearchClusterSettings.blankToNull(keyStoreType);
  }

  public String getKeyStorePassword() {
    return SearchClusterSettings.blankToNull(keyStorePassword);
  }

  public String getKeyPassword() {
    return SearchClusterSettings.blankToNull(keyPassword);
  }

  /** True when nothing is configured, in which case the JVM default trust material is used. */
  public boolean isEmpty() {
    return getProtocol() == null
        && getSecureRandomImplementation() == null
        && getTrustStoreFile() == null
        && getKeyStoreFile() == null;
  }
}
