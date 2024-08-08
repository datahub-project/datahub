package com.linkedin.gms.factory.common;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import javax.annotation.Nonnull;
import javax.net.ssl.SSLContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ElasticsearchSSLContextFactory {

  @Value("${elasticsearch.sslContext.protocol}")
  private String sslProtocol;

  @Value("${elasticsearch.sslContext.secureRandomImplementation}")
  private String sslSecureRandomImplementation;

  @Value("${elasticsearch.sslContext.trustStoreFile}")
  private String sslTrustStoreFile;

  @Value("${elasticsearch.sslContext.trustStoreType}")
  private String sslTrustStoreType;

  @Value("${elasticsearch.sslContext.trustStorePassword}")
  private String sslTrustStorePassword;

  @Value("${elasticsearch.sslContext.keyStoreFile}")
  private String sslKeyStoreFile;

  @Value("${elasticsearch.sslContext.keyStoreType}")
  private String sslKeyStoreType;

  @Value("${elasticsearch.sslContext.keyStorePassword}")
  private String sslKeyStorePassword;

  @Value("${elasticsearch.sslContext.keyPassword}")
  private String sslKeyPassword;

  @Bean(name = "elasticSearchSSLContext")
  public SSLContext createInstance() {
    final SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
    if (sslProtocol != null) {
      sslContextBuilder.useProtocol(sslProtocol);
    }

    if (sslTrustStoreFile != null && sslTrustStoreType != null && sslTrustStorePassword != null) {
      loadTrustStore(
          sslContextBuilder, sslTrustStoreFile, sslTrustStoreType, sslTrustStorePassword);
    }

    if (sslKeyStoreFile != null
        && sslKeyStoreType != null
        && sslKeyStorePassword != null
        && sslKeyPassword != null) {
      loadKeyStore(
          sslContextBuilder, sslKeyStoreFile, sslKeyStoreType, sslKeyStorePassword, sslKeyPassword);
    }

    final SSLContext sslContext;
    try {
      if (sslSecureRandomImplementation != null) {
        sslContextBuilder.setSecureRandom(SecureRandom.getInstance(sslSecureRandomImplementation));
      }
      sslContext = sslContextBuilder.build();
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to build SSL Context", e);
    }
    return sslContext;
  }

  private void loadKeyStore(
      @Nonnull SSLContextBuilder sslContextBuilder,
      @Nonnull String path,
      @Nonnull String type,
      @Nonnull String password,
      @Nonnull String keyPassword) {
    try (InputStream identityFile = new FileInputStream(path)) {
      final KeyStore keystore = KeyStore.getInstance(type);
      keystore.load(identityFile, password.toCharArray());
      sslContextBuilder.loadKeyMaterial(keystore, keyPassword.toCharArray());
    } catch (IOException
        | CertificateException
        | NoSuchAlgorithmException
        | KeyStoreException
        | UnrecoverableKeyException e) {
      throw new RuntimeException("Failed to load key store: " + path, e);
    }
  }

  private void loadTrustStore(
      @Nonnull SSLContextBuilder sslContextBuilder,
      @Nonnull String path,
      @Nonnull String type,
      @Nonnull String password) {
    try (InputStream identityFile = new FileInputStream(path)) {
      final KeyStore keystore = KeyStore.getInstance(type);
      keystore.load(identityFile, password.toCharArray());
      sslContextBuilder.loadTrustMaterial(keystore, null);
    } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to load key store: " + path, e);
    }
  }
}
