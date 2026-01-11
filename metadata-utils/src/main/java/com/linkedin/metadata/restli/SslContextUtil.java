package com.linkedin.metadata.restli;

import java.io.FileInputStream;
import java.security.KeyStore;
import javax.annotation.Nullable;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public final class SslContextUtil {

  public static SSLContext buildSslContext(
      @Nullable String keystorePath,
      @Nullable String keystorePassword,
      @Nullable String keystoreType,
      @Nullable String truststorePath,
      @Nullable String truststorePassword,
      @Nullable String truststoreType)
      throws Exception {

    KeyManagerFactory keyManagerFactory = null;

    // --- Load keystore (client certificate) ---
    if (keystorePath != null && !keystorePath.isBlank()) {
      KeyStore keyStore =
          KeyStore.getInstance(keystoreType != null ? keystoreType : KeyStore.getDefaultType());
      try (FileInputStream fis = new FileInputStream(keystorePath)) {
        keyStore.load(fis, keystorePassword != null ? keystorePassword.toCharArray() : null);
      }

      keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      keyManagerFactory.init(
          keyStore, keystorePassword != null ? keystorePassword.toCharArray() : null);
    }

    // --- Load truststore ---
    TrustManagerFactory trustManagerFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

    if (truststorePath != null && !truststorePath.isBlank()) {
      KeyStore trustStore =
          KeyStore.getInstance(truststoreType != null ? truststoreType : KeyStore.getDefaultType());
      try (FileInputStream fis = new FileInputStream(truststorePath)) {
        trustStore.load(fis, truststorePassword != null ? truststorePassword.toCharArray() : null);
      }
      trustManagerFactory.init(trustStore);
    } else {
      trustManagerFactory.init((KeyStore) null); // default JRE trusted CAs
    }

    // --- Build SSLContext ---
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        keyManagerFactory != null ? keyManagerFactory.getKeyManagers() : null,
        trustManagerFactory.getTrustManagers(),
        null);

    return sslContext;
  }

  private SslContextUtil() {}
}
