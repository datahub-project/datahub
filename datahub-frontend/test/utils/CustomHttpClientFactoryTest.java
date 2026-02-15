package utils;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class CustomHttpClientFactoryTest {

  @TempDir Path tempDir;

  private static final String TRUSTSTORE_PASSWORD = "testpassword";
  private static final String TRUSTSTORE_TYPE = "PKCS12";

  /**
   * Generate a temp PKCS12 truststore using keytool. Skips (assumeTrue) if keytool is unavailable
   * or fails, so tests do not fail in environments where keytool is missing or broken.
   */
  Path generateTempTruststore() throws IOException, InterruptedException {
    Path truststorePath = tempDir.resolve("test-truststore-" + System.nanoTime() + ".p12");
    String keytoolPath = System.getenv("KEYTOOL_PATH");
    if (keytoolPath == null) {
      keytoolPath = System.getProperty("java.home") + "/bin/keytool";
    }
    File keytoolFile = new File(keytoolPath);
    if (!keytoolFile.exists()) {
      keytoolPath = "keytool";
    }
    List<String> command =
        List.of(
            keytoolPath,
            "-genkeypair",
            "-alias",
            "testcert",
            "-keyalg",
            "RSA",
            "-keysize",
            "2048",
            "-storetype",
            "PKCS12",
            "-keystore",
            truststorePath.toString(),
            "-validity",
            "3650",
            "-storepass",
            TRUSTSTORE_PASSWORD,
            "-dname",
            "CN=Test, OU=Dev, O=Example, L=Test, S=Test, C=US");
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes());
    int exitCode = process.waitFor();
    assumeTrue(
        exitCode == 0, "keytool failed or unavailable (exit " + exitCode + "). Output: " + output);
    return truststorePath;
  }

  @Test
  void testCreateSslContextWithValidTruststore() throws Exception {
    Path truststorePath = generateTempTruststore();
    SSLContext context =
        CustomHttpClientFactory.createSslContext(
            truststorePath.toString(), TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE);
    assertNotNull(context);
    assertEquals("TLSv1.2", context.getProtocol());
  }

  @Test
  void testCreateSslContextWithCustomProtocolsUsesFirst() throws Exception {
    Path truststorePath = generateTempTruststore();
    SSLContext context =
        CustomHttpClientFactory.createSslContext(
            truststorePath.toString(),
            TRUSTSTORE_PASSWORD,
            TRUSTSTORE_TYPE,
            List.of("TLSv1.3", "TLSv1.2"));
    assertNotNull(context);
    assertEquals("TLSv1.3", context.getProtocol());
  }

  @Test
  void testCreateSslContextWithEmptyListUsesDefaultProtocol() throws Exception {
    Path truststorePath = generateTempTruststore();
    SSLContext context =
        CustomHttpClientFactory.createSslContext(
            truststorePath.toString(), TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE, List.of());
    assertNotNull(context);
    assertEquals(ConfigUtil.DEFAULT_CLIENT_SSL_ENABLED_PROTOCOLS.get(0), context.getProtocol());
  }

  @Test
  void testGetApacheHttpClientWithCustomProtocols() throws Exception {
    Path truststorePath = generateTempTruststore();
    CloseableHttpClient client =
        CustomHttpClientFactory.getApacheHttpClient(
            truststorePath.toString(),
            TRUSTSTORE_PASSWORD,
            TRUSTSTORE_TYPE,
            List.of("TLSv1.3", "TLSv1.2"));
    assertNotNull(client);
  }

  @Test
  void testCreateSslContextWithInvalidTruststoreThrows() {
    assertThrows(
        Exception.class,
        () ->
            CustomHttpClientFactory.createSslContext(
                "doesnotexist.p12", "wrongpassword", TRUSTSTORE_TYPE));
  }

  @Test
  void testGetJavaHttpClientWithValidTruststore() throws Exception {
    Path truststorePath = generateTempTruststore();
    HttpClient client =
        CustomHttpClientFactory.getJavaHttpClient(
            truststorePath.toString(), TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE);
    assertNotNull(client);
  }

  @Test
  void testGetJavaHttpClientWithInvalidTruststoreFallsBack() {
    HttpClient client =
        CustomHttpClientFactory.getJavaHttpClient(
            "doesnotexist.p12", "wrongpassword", TRUSTSTORE_TYPE);
    assertNotNull(client);
  }

  @Test
  void testGetApacheHttpClientWithValidTruststore() throws Exception {
    Path truststorePath = generateTempTruststore();
    CloseableHttpClient client =
        CustomHttpClientFactory.getApacheHttpClient(
            truststorePath.toString(), TRUSTSTORE_PASSWORD, TRUSTSTORE_TYPE);
    assertNotNull(client);
  }

  @Test
  void testGetApacheHttpClientWithInvalidTruststoreFallsBack() {
    CloseableHttpClient client =
        CustomHttpClientFactory.getApacheHttpClient(
            "doesnotexist.p12", "wrongpassword", TRUSTSTORE_TYPE);
    assertNotNull(client);
  }
}
