package utils;

import java.io.FileInputStream;
import java.net.http.HttpClient;
import java.security.KeyStore;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CustomHttpClientFactory {
  private static final Logger log =
      LoggerFactory.getLogger(CustomHttpClientFactory.class.getName());

  public static SSLContext getSslContext(String path, String pass, String type) throws Exception {
    return createSslContext(path, pass, type, ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS);
  }

  public static HttpClient getJavaHttpClient(String path, String pass, String type) {
    return getJavaHttpClient(path, pass, type, ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS);
  }

  public static HttpClient getJavaHttpClient(
      String path, String pass, String type, List<String> sslEnabledProtocols) {
    try {
      log.info(
          "Initializing Java HttpClient with custom truststore at '{}' and type '{}'", path, type);
      return HttpClient.newBuilder()
          .sslContext(createSslContext(path, pass, type, sslEnabledProtocols))
          .build();
    } catch (Exception e) {
      log.warn(
          "Failed to initialize Java HttpClient with custom truststore at '{}'. Falling back to default HttpClient. Reason: {}",
          path,
          e.getMessage());
      log.debug("Truststore load failure detail", e);
      return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }
  }

  public static CloseableHttpClient getApacheHttpClient(String path, String pass, String type) {
    return getApacheHttpClient(path, pass, type, ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS);
  }

  public static CloseableHttpClient getApacheHttpClient(
      String path, String pass, String type, List<String> sslEnabledProtocols) {
    try {
      log.info(
          "Initializing Apache CloseableHttpClient with custom truststore at '{}' and type '{}'",
          path,
          type);
      String[] protocols = sslEnabledProtocols.toArray(new String[0]);
      return HttpClients.custom()
          .setSSLSocketFactory(
              new SSLConnectionSocketFactory(
                  createSslContext(path, pass, type, sslEnabledProtocols),
                  protocols,
                  null,
                  SSLConnectionSocketFactory.getDefaultHostnameVerifier()))
          .build();
    } catch (Exception e) {
      log.warn(
          "Failed to initialize Apache HttpClient with custom truststore at '{}'. Falling back to default HttpClient. Reason: {}",
          path,
          e.getMessage());
      log.debug("Truststore load failure detail", e);
      return HttpClients.createDefault();
    }
  }

  public static SSLContext createSslContext(
      String truststorePath, String truststorePassword, String truststoreType) throws Exception {
    return createSslContext(
        truststorePath,
        truststorePassword,
        truststoreType,
        ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS);
  }

  public static SSLContext createSslContext(
      String truststorePath,
      String truststorePassword,
      String truststoreType,
      List<String> sslEnabledProtocols)
      throws Exception {
    log.info(
        "Creating SSLContext with truststore at '{}' and type '{}'",
        truststorePath,
        truststoreType);
    KeyStore trustStore = KeyStore.getInstance(truststoreType);
    try (FileInputStream fis = new FileInputStream(truststorePath)) {
      trustStore.load(fis, truststorePassword.toCharArray());
    }
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);
    String protocol =
        sslEnabledProtocols.isEmpty()
            ? ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS.get(0)
            : sslEnabledProtocols.get(0);
    SSLContext sslContext = SSLContext.getInstance(protocol);
    sslContext.init(null, tmf.getTrustManagers(), null);
    return sslContext;
  }
}
