package com.linkedin.metadata.restli;

import static org.testng.Assert.*;

import java.io.File;
import java.nio.file.Path;
import java.util.List;
import javax.net.ssl.SSLContext;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class SslContextUtilTest {

  private static final String PASS = "testpassword";
  private static final String TYPE = "PKCS12";

  /** Same approach as datahub-frontend CustomHttpClientFactoryTest. */
  private Path generateTempPkcs12(Path dir) throws Exception {
    Path p = dir.resolve("ssl-" + System.nanoTime() + ".p12");
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
  public void buildClientSslContextEmptyUsesDefault() throws Exception {
    SSLContext ctx = SslContextUtil.buildClientSslContext(RestliClientSslConfig.empty());
    assertNotNull(ctx);
    assertEquals(ctx, SSLContext.getDefault());
  }

  @Test
  public void buildClientSslContextTruststoreOnly() throws Exception {
    Path dir = Path.of(System.getProperty("java.io.tmpdir"));
    Path p = generateTempPkcs12(dir);
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(p.toString(), PASS, TYPE, null, null, null, null);
    SSLContext ctx = SslContextUtil.buildClientSslContext(cfg);
    assertNotNull(ctx);
    assertNotNull(ctx.getSocketFactory());
  }

  @Test
  public void buildClientSslContextKeystoreOnly() throws Exception {
    Path dir = Path.of(System.getProperty("java.io.tmpdir"));
    Path p = generateTempPkcs12(dir);
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(null, null, null, p.toString(), PASS, TYPE, null);
    SSLContext ctx = SslContextUtil.buildClientSslContext(cfg);
    assertNotNull(ctx);
    assertNotNull(ctx.getSocketFactory());
  }

  @Test
  public void buildClientSslContextTrustAndKeystore() throws Exception {
    Path dir = Path.of(System.getProperty("java.io.tmpdir"));
    Path trust = generateTempPkcs12(dir);
    Path key = generateTempPkcs12(dir);
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(
            trust.toString(), PASS, TYPE, key.toString(), PASS, TYPE, null);
    SSLContext ctx = SslContextUtil.buildClientSslContext(cfg);
    assertNotNull(ctx);
  }

  @Test
  public void buildClientSslContextTrustPathWithoutPasswordThrows() {
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings("/nope.p12", null, TYPE, null, null, null, null);
    assertThrows(IllegalArgumentException.class, () -> SslContextUtil.buildClientSslContext(cfg));
  }

  @Test
  public void buildClientSslContextKeystorePathWithoutPasswordThrows() {
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(null, null, null, "/nope.p12", null, TYPE, null);
    assertThrows(IllegalArgumentException.class, () -> SslContextUtil.buildClientSslContext(cfg));
  }

  @Test
  public void buildClientSslContextMissingFileThrows() {
    RestliClientSslConfig cfg =
        RestliClientSslConfig.fromNullableStrings(
            "/definitely-missing-file-12345.p12", PASS, TYPE, null, null, null, null);
    assertThrows(Exception.class, () -> SslContextUtil.buildClientSslContext(cfg));
  }
}
