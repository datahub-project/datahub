package com.linkedin.metadata.restli;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import org.apache.commons.lang3.StringUtils;

/**
 * Optional TLS material for Rest.li HTTP clients: custom truststore and/or client keystore (mTLS).
 */
@Immutable
public final class RestliClientSslConfig {

  private static final RestliClientSslConfig EMPTY =
      new RestliClientSslConfig(null, null, null, null, null, null, null);

  @Nullable private final String truststorePath;
  @Nullable private final String truststorePassword;
  @Nullable private final String truststoreType;
  @Nullable private final String keystorePath;
  @Nullable private final String keystorePassword;
  @Nullable private final String keystoreType;
  @Nullable private final String keyPassword;

  private RestliClientSslConfig(
      @Nullable String truststorePath,
      @Nullable String truststorePassword,
      @Nullable String truststoreType,
      @Nullable String keystorePath,
      @Nullable String keystorePassword,
      @Nullable String keystoreType,
      @Nullable String keyPassword) {
    this.truststorePath = truststorePath;
    this.truststorePassword = truststorePassword;
    this.truststoreType = truststoreType;
    this.keystorePath = keystorePath;
    this.keystorePassword = keystorePassword;
    this.keystoreType = keystoreType;
    this.keyPassword = keyPassword;
  }

  @Nonnull
  public static RestliClientSslConfig empty() {
    return EMPTY;
  }

  /**
   * Builds a config from nullable configuration strings (e.g. Spring {@code @Value} or Play
   * config). Blank strings are treated as unset.
   */
  @Nonnull
  public static RestliClientSslConfig fromNullableStrings(
      @Nullable String truststorePath,
      @Nullable String truststorePassword,
      @Nullable String truststoreType,
      @Nullable String keystorePath,
      @Nullable String keystorePassword,
      @Nullable String keystoreType,
      @Nullable String keyPassword) {
    return new RestliClientSslConfig(
        blankToNull(truststorePath),
        blankToNull(truststorePassword),
        blankToNull(truststoreType),
        blankToNull(keystorePath),
        blankToNull(keystorePassword),
        blankToNull(keystoreType),
        blankToNull(keyPassword));
  }

  @Nullable
  private static String blankToNull(@Nullable String s) {
    return StringUtils.isBlank(s) ? null : s.trim();
  }

  public boolean hasCustomSslMaterial() {
    return StringUtils.isNotBlank(truststorePath) || StringUtils.isNotBlank(keystorePath);
  }

  @Nullable
  public String getTruststorePath() {
    return truststorePath;
  }

  @Nullable
  public String getTruststorePassword() {
    return truststorePassword;
  }

  @Nullable
  public String getTruststoreType() {
    return truststoreType;
  }

  @Nullable
  public String getKeystorePath() {
    return keystorePath;
  }

  @Nullable
  public String getKeystorePassword() {
    return keystorePassword;
  }

  @Nullable
  public String getKeystoreType() {
    return keystoreType;
  }

  @Nullable
  public String getKeyPassword() {
    return keyPassword;
  }
}
