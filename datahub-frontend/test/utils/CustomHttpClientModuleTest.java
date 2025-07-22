package utils;

import static org.junit.jupiter.api.Assertions.*;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.IOException;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.http.impl.client.CloseableHttpClient;
import org.junit.jupiter.api.Test;

class CustomHttpClientModuleTest {

  private static final String TRUSTSTORE_PASSWORD = "testpassword";
  private static final String TRUSTSTORE_TYPE = "PKCS12";

  // Helper: Generate a temp PKCS12 truststore using keytool
  static Path generateTempTruststore() throws IOException, InterruptedException {
    Path tempDir = Files.createTempDirectory("test-truststore");
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
  void testProvideClientsWithValidTruststore() throws Exception {
    Path truststorePath = generateTempTruststore();
    Config config =
        ConfigFactory.parseString(
            "metadata.service.ssl.trust-store-path=\""
                + truststorePath.toString()
                + "\"\n"
                + "metadata.service.ssl.trust-store-password=\""
                + TRUSTSTORE_PASSWORD
                + "\"\n"
                + "metadata.service.ssl.trust-store-type=\""
                + TRUSTSTORE_TYPE
                + "\"");

    Injector injector =
        Guice.createInjector(
            binder -> {
              binder.bind(Config.class).toInstance(config);
              binder.install(new CustomHttpClientModule());
            });

    CloseableHttpClient apacheClient = injector.getInstance(CloseableHttpClient.class);
    HttpClient javaClient = injector.getInstance(HttpClient.class);

    assertNotNull(apacheClient);
    assertNotNull(javaClient);
  }

  @Test
  void testProvideClientsWithInvalidTruststoreFallsBack() {
    Config config =
        ConfigFactory.parseString(
            "metadata.service.ssl.trust-store-path=\"doesnotexist.p12\"\n"
                + "metadata.service.ssl.trust-store-password=\"badpassword\"\n"
                + "metadata.service.ssl.trust-store-type=\"PKCS12\"");

    Injector injector =
        Guice.createInjector(
            binder -> {
              binder.bind(Config.class).toInstance(config);
              binder.install(new CustomHttpClientModule());
            });

    CloseableHttpClient apacheClient = injector.getInstance(CloseableHttpClient.class);
    HttpClient javaClient = injector.getInstance(HttpClient.class);

    assertNotNull(apacheClient);
    assertNotNull(javaClient);
  }
}
