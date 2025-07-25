package utils;

import java.io.FileInputStream;
import java.net.http.HttpClient;
import java.security.KeyStore;
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
    return createSslContext(path, pass, type);
  }

  public static HttpClient getJavaHttpClient(String path, String pass, String type) {
    try {
      log.info(
          "Initializing Java HttpClient with custom truststore at '{}' and type '{}'", path, type);
      return HttpClient.newBuilder().sslContext(getSslContext(path, pass, type)).build();
    } catch (Exception e) {
      log.warn(
          "Failed to initialize Java HttpClient with custom truststore at '{}'. Falling back to default HttpClient. Reason: {}",
          path,
          e.getMessage(),
          e);
      return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build();
    }
  }

  public static CloseableHttpClient getApacheHttpClient(String path, String pass, String type) {
    try {
      log.info(
          "Initializing Apache CloseableHttpClient with custom truststore at '{}' and type '{}'",
          path,
          type);
      return HttpClients.custom()
          .setSSLSocketFactory(new SSLConnectionSocketFactory(getSslContext(path, pass, type)))
          .build();
    } catch (Exception e) {
      log.warn(
          "Failed to initialize Apache HttpClient with custom truststore at '{}'. Falling back to default HttpClient. Reason: {}",
          path,
          e.getMessage(),
          e);
      return HttpClients.createDefault();
    }
  }

  public static SSLContext createSslContext(
      String truststorePath, String truststorePassword, String truststoreType) throws Exception {
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
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(null, tmf.getTrustManagers(), null);
    return sslContext;
  }
}
