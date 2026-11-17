package com.linkedin.gms.factory.common;

import com.linkedin.gms.factory.config.ConfigurationProvider;
import com.linkedin.metadata.config.search.ElasticSearchConfiguration;
import com.linkedin.metadata.config.search.SslContextSettings;
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
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import org.apache.http.ssl.SSLContextBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Builds SSL contexts for search cluster connections.
 *
 * <p>TLS material is per cluster, since a second cluster may sit behind a different certificate
 * authority than the primary. The {@code elasticSearchSSLContext} bean remains the primary
 * cluster's context so existing injection points are unaffected.
 */
@Configuration
public class ElasticsearchSSLContextFactory {

  @Bean(name = "elasticSearchSSLContext")
  public SSLContext createInstance(final ConfigurationProvider configurationProvider) {
    ElasticSearchConfiguration esConfig = configurationProvider.getElasticSearch();
    return buildSSLContext(esConfig.getPrimaryCluster().getSslContext());
  }

  /**
   * Builds an {@link SSLContext} from one cluster's settings. A null or empty block yields the JVM
   * default trust material, which is what an unconfigured cluster should use.
   */
  @Nonnull
  public static SSLContext buildSSLContext(@Nullable SslContextSettings settings) {
    final SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
    if (settings != null) {
      if (settings.getProtocol() != null) {
        sslContextBuilder.useProtocol(settings.getProtocol());
      }

      if (settings.getTrustStoreFile() != null
          && settings.getTrustStoreType() != null
          && settings.getTrustStorePassword() != null) {
        loadTrustStore(
            sslContextBuilder,
            settings.getTrustStoreFile(),
            settings.getTrustStoreType(),
            settings.getTrustStorePassword());
      }

      if (settings.getKeyStoreFile() != null
          && settings.getKeyStoreType() != null
          && settings.getKeyStorePassword() != null
          && settings.getKeyPassword() != null) {
        loadKeyStore(
            sslContextBuilder,
            settings.getKeyStoreFile(),
            settings.getKeyStoreType(),
            settings.getKeyStorePassword(),
            settings.getKeyPassword());
      }
    }

    final SSLContext sslContext;
    try {
      if (settings != null && settings.getSecureRandomImplementation() != null) {
        sslContextBuilder.setSecureRandom(
            SecureRandom.getInstance(settings.getSecureRandomImplementation()));
      }
      sslContext = sslContextBuilder.build();
    } catch (NoSuchAlgorithmException | KeyManagementException e) {
      throw new RuntimeException("Failed to build SSL Context", e);
    }
    return sslContext;
  }

  private static void loadKeyStore(
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

  private static void loadTrustStore(
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
