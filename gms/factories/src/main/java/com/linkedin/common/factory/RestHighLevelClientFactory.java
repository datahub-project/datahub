package com.linkedin.common.factory;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLContextBuilder;
import org.apache.http.impl.nio.reactor.IOReactorConfig;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;


@Slf4j
@Configuration
public class RestHighLevelClientFactory {

  @Value("${ELASTICSEARCH_HOST:localhost}")
  private String host;

  @Value("${ELASTICSEARCH_PORT:9200}")
  private Integer port;

  @Value("${ELASTICSEARCH_THREAD_COUNT:1}")
  private Integer threadCount;

  @Value("${ELASTICSEARCH_CONNECTION_REQUEST_TIMEOUT:0}")
  private Integer connectionRequestTimeout;

  @Value("${ELASTICSEARCH_USE_SSL:false}")
  private boolean useSSL;

  @Value("${ELASTICSEARCH_SSL_PROTOCOL:#{null}}")
  private String sslProtocol;

  @Value("${ELASTICSEARCH_SSL_KEYSTORE_FILE:#{null}}")
  private String sslKeystoreFile;

  @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_FILE:#{null}}")
  private String sslTruststoreFile;

  @Value("${ELASTICSEARCH_SSL_SECURE_RANDOM_IMPL:#{null}}")
  private String sslSecureRandomImplementation;

  @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_FILE:#{null}}")
  private String sslTrustStoreFile;

  @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_TYPE:#{null}}")
  private String sslTrustStoreType;

  @Value("${ELASTICSEARCH_SSL_TRUSTSTORE_PASSWORD:#{null}}")
  private String sslTrustStorePassword;

  @Value("${ELASTICSEARCH_SSL_KEYSTORE_TYPE:#{null}}")
  private String sslKeyStoreType;

  @Value("${ELASTICSEARCH_SSL_KEYSTORE_PASSWORD:#{null}}")
  private String sslKeyStorePassword;

  @Bean(name = "elasticSearchRestHighLevelClient")
  @Nonnull
  protected RestHighLevelClient createInstance() {
    try {
      RestClient restClient;
      if (useSSL) {
        restClient = loadRestHttpsClient(host, port, threadCount, connectionRequestTimeout, sslProtocol,
            sslSecureRandomImplementation, sslTrustStoreFile, sslTrustStoreType, sslTrustStorePassword, sslKeystoreFile,
            sslKeyStoreType, sslKeyStorePassword);
      } else {
        restClient = loadRestHttpClient(host, port, threadCount, connectionRequestTimeout);
      }

      return new RestHighLevelClient(restClient);
    } catch (Exception e) {
      throw new RuntimeException("Error: RestClient is not properly initialized. " + e.toString());
    }
  }

  @Nonnull
  private static RestClient loadRestHttpClient(@Nonnull String host, int port, int threadCount,
      int connectionRequestTimeout) throws Exception {
    RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "http"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultIOReactorConfig(
            IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
        setConnectionRequestTimeout(connectionRequestTimeout));

    return builder.build();
  }

  @Nonnull
  private static RestClient loadRestHttpsClient(@Nonnull String host, int port, int threadCount,
      int connectionRequestTimeout, @Nullable String sslProtocol, @Nullable String sslSecureRandomImplementation,
      @Nullable String sslTrustStoreFile, @Nullable String sslTrustStoreType, @Nullable String sslTrustStorePassword,
      @Nullable String sslKeystoreFile, @Nullable String sslKeyStoreType, @Nullable String sslKeyStorePassword) {
    final SSLContextBuilder sslContextBuilder = new SSLContextBuilder();
    if (sslProtocol != null) {
      sslContextBuilder.useProtocol(sslProtocol);
    }

    if (sslTrustStoreFile != null && sslTrustStoreType != null && sslTrustStorePassword != null) {
      loadKeyStore(sslContextBuilder, sslTrustStoreFile, sslTrustStoreType, sslTrustStorePassword);
    }

    if (sslKeystoreFile != null && sslKeyStoreType != null && sslKeyStorePassword != null) {
      loadKeyStore(sslContextBuilder, sslKeystoreFile, sslKeyStoreType, sslKeyStorePassword);
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

    // set ssl context and configure number of threads for clients
    final RestClientBuilder builder = RestClient.builder(new HttpHost(host, port, "https"))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setSSLContext(sslContext)
            .setSSLHostnameVerifier(new NoopHostnameVerifier())
            .setDefaultIOReactorConfig(IOReactorConfig.custom().setIoThreadCount(threadCount).build()));

    builder.setRequestConfigCallback(requestConfigBuilder -> requestConfigBuilder.
        setConnectionRequestTimeout(connectionRequestTimeout));

    return builder.build();
  }

  @Nonnull
  private static void loadKeyStore(@Nonnull SSLContextBuilder sslContextBuilder, @Nonnull String path,
      @Nonnull String type, @Nonnull String password) {
    try (InputStream identityFile = new FileInputStream(path)) {
      final KeyStore keystore = KeyStore.getInstance(type);
      keystore.load(identityFile, password.toCharArray());
      sslContextBuilder.loadTrustMaterial(keystore, null);
    } catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
      throw new RuntimeException("Failed to load key store: " + path, e);
    }
  }
}

