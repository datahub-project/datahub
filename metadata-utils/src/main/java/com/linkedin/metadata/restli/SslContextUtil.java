package com.linkedin.metadata.restli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.annotation.Nonnull;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import org.apache.commons.lang3.StringUtils;

/** Builds client {@link SSLContext} instances for Rest.li / R2 HTTP transport. */
public final class SslContextUtil {

  private static final String DEFAULT_STORE_TYPE = "PKCS12";

  private SslContextUtil() {}

  /**
   * Returns the JVM default context when {@link RestliClientSslConfig#hasCustomSslMaterial()} is
   * false. Otherwise loads optional truststore and/or keystore (mTLS). If only a keystore is set,
   * platform default trust anchors are used ({@code trustManagers == null} in {@link
   * SSLContext#init}).
   */
  @Nonnull
  public static SSLContext buildClientSslContext(@Nonnull RestliClientSslConfig config)
      throws GeneralSecurityException, IOException {
    if (!config.hasCustomSslMaterial()) {
      return SSLContext.getDefault();
    }

    KeyManager[] keyManagers = null;
    TrustManager[] trustManagers = null;

    if (StringUtils.isNotBlank(config.getTruststorePath())) {
      if (StringUtils.isBlank(config.getTruststorePassword())) {
        throw new IllegalArgumentException(
            "datahub.gms.truststore.password is required when truststore.path is set");
      }
      String trustType =
          StringUtils.isNotBlank(config.getTruststoreType())
              ? config.getTruststoreType()
              : DEFAULT_STORE_TYPE;
      trustManagers =
          loadTrustManagers(config.getTruststorePath(), config.getTruststorePassword(), trustType);
    }

    if (StringUtils.isNotBlank(config.getKeystorePath())) {
      if (StringUtils.isBlank(config.getKeystorePassword())) {
        throw new IllegalArgumentException(
            "datahub.gms.keystore.password is required when keystore.path is set");
      }
      String keyStoreType =
          StringUtils.isNotBlank(config.getKeystoreType())
              ? config.getKeystoreType()
              : DEFAULT_STORE_TYPE;
      String keyPass =
          StringUtils.isNotBlank(config.getKeyPassword())
              ? config.getKeyPassword()
              : config.getKeystorePassword();
      keyManagers =
          loadKeyManagers(
              config.getKeystorePath(),
              config.getKeystorePassword(),
              keyStoreType,
              keyPass.toCharArray());
    }

    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(keyManagers, trustManagers, null);
    return sslContext;
  }

  private static TrustManager[] loadTrustManagers(String path, String password, String type)
      throws IOException, KeyStoreException, CertificateException, NoSuchAlgorithmException {
    KeyStore trustStore = KeyStore.getInstance(type);
    try (InputStream in = new FileInputStream(path)) {
      trustStore.load(in, password.toCharArray());
    }
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    return tmf.getTrustManagers();
  }

  private static KeyManager[] loadKeyManagers(
      String path, String storePassword, String type, char[] keyPassword)
      throws IOException,
          KeyStoreException,
          CertificateException,
          NoSuchAlgorithmException,
          UnrecoverableKeyException {
    KeyStore keyStore = KeyStore.getInstance(type);
    try (InputStream in = new FileInputStream(path)) {
      keyStore.load(in, storePassword.toCharArray());
    }
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
    kmf.init(keyStore, keyPassword);
    return kmf.getKeyManagers();
  }
}
