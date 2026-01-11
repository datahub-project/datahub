package utils;

import com.linkedin.metadata.restli.SslContextUtil;
import java.net.http.HttpClient;
import javax.net.ssl.SSLContext;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHttpClientFactory {
  private static final Logger log =
      LoggerFactory.getLogger(CustomHttpClientFactory.class.getName());

  public static SSLContext getSslContext(
      String keyStorePath,
      String keyStorePass,
      String keyStoreType,
      String trustStorePath,
      String trustStorePass,
      String trustStoreType)
      throws Exception {
    return createSslContextWithKeyStore(
        keyStorePath, keyStorePass, keyStoreType, trustStorePath, trustStorePass, trustStoreType);
  }

  public static HttpClient getJavaHttpClient(
      String keyStorePath,
      String keyStorePass,
      String keyStoreType,
      String trustStorePath,
      String trustStorePass,
      String trustStoreType) {
    try {
      log.info(
          "Initializing Java HttpClient with custom truststore at '{}' and trustStoreType '{}'",
          trustStorePath,
          trustStoreType);
      return HttpClient.newBuilder()
          .sslContext(
              getSslContext(
                  keyStorePath,
                  keyStorePass,
                  keyStoreType,
                  trustStorePath,
                  trustStorePass,
                  trustStoreType))
          .build();
    } catch (Exception e) {
      log.warn(
          "Failed to initialize Java HttpClient with custom truststore at '{}'. Falling back to default HttpClient. Reason: {}",
          trustStorePath,
          e.getMessage(),
          e);
      return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }
  }

  public static CloseableHttpClient getApacheHttpClient(
      String keyStorePath,
      String keyStorePass,
      String keyStoreType,
      String trustStorePath,
      String trustStorePass,
      String trustStoreType) {
    try {
      log.info(
          "Initializing Apache CloseableHttpClient with custom truststore at '{}' and trustStoreType '{}'",
          trustStorePath,
          trustStoreType);
      return HttpClients.custom()
          .setSSLSocketFactory(
              new SSLConnectionSocketFactory(
                  getSslContext(
                      keyStorePath,
                      keyStorePass,
                      keyStoreType,
                      trustStorePath,
                      trustStorePass,
                      trustStoreType)))
          .build();
    } catch (Exception e) {
      log.warn(
          "Failed to initialize Apache HttpClient with custom truststore at '{}'. Falling back to default HttpClient. Reason: {}",
          trustStorePath,
          e.getMessage(),
          e);
      return HttpClients.createDefault();
    }
  }

  /**
   * Creates an SSLContext that uses a keystore (for client certificate + private key) and/or a
   * truststore (for validating server certificates). If keystorePath is null/blank, no client
   * certificate is loaded (one-way TLS).
   *
   * @param keystorePath Path to the keystore file (PKCS12/JKS) containing client cert + key
   * @param keystorePassword Keystore password
   * @param keystoreType Keystore type (PKCS12 or JKS)
   * @param truststorePath Path to the truststore file containing server cert/CA certs
   * @param truststorePassword Truststore password
   * @param truststoreType Truststore type (PKCS12 or JKS)
   * @return Configured SSLContext for TLS/mTLS
   * @throws Exception on load/init error
   */
  public static SSLContext createSslContextWithKeyStore(
      String keystorePath,
      String keystorePassword,
      String keystoreType,
      String truststorePath,
      String truststorePassword,
      String truststoreType)
      throws Exception {

    log.info(
        "Creating SSLContext with keystore='{}' and truststore='{}'", keystorePath, truststorePath);

    // --- Create SSLContext ---
    return SslContextUtil.buildSslContext(
        keystorePath,
        keystorePassword,
        keystoreType,
        truststorePath,
        truststorePassword,
        truststoreType);
  }
}
