package com.linkedin.metadata.restli;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.restli.client.RestClient;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.security.InvalidParameterException;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class DefaultRestliClientFactoryTest {

  @TempDir Path tempDir;

  private static final String TRUSTSTORE_PASSWORD = "testpassword";
  private static final String TRUSTSTORE_TYPE = "PKCS12";

  // Helper: Generate a temp PKCS12 truststore using keytool
  Path generateTempTruststore() throws IOException, InterruptedException {
    Path truststorePath = tempDir.resolve("test-truststore-" + System.nanoTime() + ".p12");
    String keytoolPath = System.getenv("KEYTOOL_PATH");
    if (keytoolPath == null) {
      keytoolPath = System.getProperty("java.home") + "/bin/keytool";
    }
    File keytoolFile = new File(keytoolPath);
    if (!keytoolFile.exists()) {
      keytoolPath = "keytool"; // fallback to system path
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
    if (exitCode != 0)
      throw new RuntimeException(
          "Could not generate truststore, exit code: " + exitCode + ", output: " + output);
    return truststorePath;
  }

  @Test
  public void testInvalidHostThrowsException() {
    URI invalidUri = URI.create("http://:0");
    assertThrows(
        InvalidParameterException.class,
        () -> DefaultRestliClientFactory.getRestLiClient(invalidUri, null, null, null, null, null));
  }

  @Test
  public void testHttpClientCreated() {
    URI uri = URI.create("http://localhost:8080");
    RestClient client =
        DefaultRestliClientFactory.getRestLiClient(uri, null, null, null, null, null);
    Assertions.assertNotNull(client, "RestClient should not be null");
  }

  @Test
  public void testHttpClientCreatedWithPort() {
    RestClient client =
        DefaultRestliClientFactory.getRestLiClient(
            "localhost", 8080, false, null, null, null, null);
    Assertions.assertNotNull(client, "RestClient should not be null");
  }

  @Test
  public void testHttpsClientWithDefaultSSLContext() {
    URI uri = URI.create("https://localhost:8443");
    RestClient client =
        DefaultRestliClientFactory.getRestLiClient(uri, null, null, null, null, null);
    Assertions.assertNotNull(client);
  }

  @Test
  public void testHttpsClientWithCustomSslProtocol() {
    URI uri = URI.create("https://localhost:8443");
    RestClient client =
        DefaultRestliClientFactory.getRestLiClient(uri, null, null, null, null, null);
    Assertions.assertNotNull(client);
  }

  @Test
  public void testHttpsClientWithTruststore() throws Exception {
    // create a dummy keystore file in tmpdir
    Path truststorePath = generateTempTruststore();

    URI uri = URI.create("https://localhost:8443");
    assertDoesNotThrow(
        () ->
            DefaultRestliClientFactory.getRestLiClient(
                uri, "TLSv1.2", null, truststorePath.toString(), "password", null));
  }

  @Test
  public void testHttpsClientWithNullTruststoreTypeDefaultsToPKCS12() throws Exception {
    // create a dummy keystore file in tmpdir
    Path truststorePath = generateTempTruststore();

    URI uri = URI.create("https://localhost:8443");
    assertDoesNotThrow(
        () ->
            DefaultRestliClientFactory.getRestLiClient(
                uri,
                "TLSv1.2",
                null,
                truststorePath.toString(),
                TRUSTSTORE_PASSWORD,
                TRUSTSTORE_TYPE));
  }

  @Test
  public void testD2ClientCreation() {
    RestClient client = DefaultRestliClientFactory.getRestLiD2Client("localhost:2181", "/d2");
    Assertions.assertNotNull(client);
  }
}
