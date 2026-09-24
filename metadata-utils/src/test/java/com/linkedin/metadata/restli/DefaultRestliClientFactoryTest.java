package com.linkedin.metadata.restli;

import static org.testng.Assert.*;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class DefaultRestliClientFactoryTest {

  private static final String PASS = "testpassword";
  private static final String TYPE = "PKCS12";

  private Path generateTempPkcs12(Path dir) throws Exception {
    Path p = dir.resolve("restli-ssl-" + System.nanoTime() + ".p12");
    String keytoolPath = System.getenv("KEYTOOL_PATH");
    if (keytoolPath == null) {
      keytoolPath = System.getProperty("java.home") + "/bin/keytool";
    }
    if (!new File(keytoolPath).exists()) {
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
            p.toString(),
            "-validity",
            "3650",
            "-storepass",
            PASS,
            "-dname",
            "CN=Test, OU=Dev, O=Example, L=Test, S=Test, C=US");
    Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
    String output = new String(process.getInputStream().readAllBytes());
    int exit = process.waitFor();
    if (exit != 0) {
      throw new SkipException("keytool unavailable (exit " + exit + "): " + output);
    }
    return p;
  }

  @Test
  public void getRestLiClientHttpUri() {
    com.linkedin.restli.client.RestClient client =
        DefaultRestliClientFactory.getRestLiClient(URI.create("http://localhost:8080/"), null);
    assertNotNull(client);
  }

  @Test
  public void getRestLiClientHttpsWithDefaultSslContext() {
    com.linkedin.restli.client.RestClient client =
        DefaultRestliClientFactory.getRestLiClient(URI.create("https://localhost:8443/"), null);
    assertNotNull(client);
  }

  @Test
  public void getRestLiClientHttpsWithCustomTruststore() throws Exception {
    Path dir = Path.of(System.getProperty("java.io.tmpdir"));
    Path p = generateTempPkcs12(dir);
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(p.toString(), PASS, TYPE, null, null, null, null);
    com.linkedin.restli.client.RestClient client =
        DefaultRestliClientFactory.getRestLiClient(
            URI.create("https://localhost:8443/"), null, null, cfg);
    assertNotNull(client);
  }

  @Test
  public void getRestLiClientHostPortHttpsWithSslConfig() throws Exception {
    Path dir = Path.of(System.getProperty("java.io.tmpdir"));
    Path p = generateTempPkcs12(dir);
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(p.toString(), PASS, TYPE, null, null, null, null);
    com.linkedin.restli.client.RestClient client =
        DefaultRestliClientFactory.getRestLiClient("localhost", 8443, "", true, null, null, cfg);
    assertNotNull(client);
  }

  @Test
  public void getRestLiClientInvalidUriThrows() {
    assertThrows(
        IllegalArgumentException.class,
        () -> DefaultRestliClientFactory.getRestLiClient(URI.create("https://localhost/"), null));
  }
}
