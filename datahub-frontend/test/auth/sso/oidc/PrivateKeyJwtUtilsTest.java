package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.security.interfaces.RSAPublicKey;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PrivateKeyJwtUtilsTest {

  @Test
  void loadsUnencryptedPemPrivateKey() throws IOException {
    RSAPrivateKey key = PrivateKeyJwtUtils.loadPrivateKey(TestKeyMaterial.PRIVATE_KEY_PATH, null);
    assertEquals("RSA", key.getAlgorithm());
    assertTrue(key.getModulus().bitLength() >= 2048);
  }

  @Test
  void loadsEncryptedPemPrivateKey() throws IOException {
    RSAPrivateKey key =
        PrivateKeyJwtUtils.loadPrivateKey(
            TestKeyMaterial.ENCRYPTED_PRIVATE_KEY_PATH, TestKeyMaterial.ENCRYPTED_KEY_PASSWORD);
    assertEquals("RSA", key.getAlgorithm());
  }

  @Test
  void encryptedPemRejectsMissingOrWrongPassword() {
    assertThrows(
        IllegalArgumentException.class,
        () -> PrivateKeyJwtUtils.loadPrivateKey(TestKeyMaterial.ENCRYPTED_PRIVATE_KEY_PATH, null));
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PrivateKeyJwtUtils.loadPrivateKey(
                TestKeyMaterial.ENCRYPTED_PRIVATE_KEY_PATH, "wrongpassword"));
  }

  @Test
  void rejectsMissingAndEmptyKeyFiles(@TempDir Path tempDir) throws IOException {
    assertThrows(
        IOException.class, () -> PrivateKeyJwtUtils.loadPrivateKey("/nonexistent-key.pem", null));

    Path emptyFile = tempDir.resolve("empty.pem");
    Files.writeString(emptyFile, "");
    assertThrows(
        IllegalArgumentException.class,
        () -> PrivateKeyJwtUtils.loadPrivateKey(emptyFile.toString(), null));
  }

  @Test
  void loadsCertificateChain() throws Exception {
    List<X509Certificate> chain =
        PrivateKeyJwtUtils.loadCertificateChain(TestKeyMaterial.CERTIFICATE_PATH);
    assertFalse(chain.isEmpty());
    assertEquals("X.509", chain.get(0).getType());

    assertThrows(
        IOException.class, () -> PrivateKeyJwtUtils.loadCertificateChain("/nonexistent-cert.pem"));
  }

  @Test
  void sha256ThumbprintIsBase64UrlAndDeterministic() throws Exception {
    X509Certificate cert =
        PrivateKeyJwtUtils.loadCertificateChain(TestKeyMaterial.CERTIFICATE_PATH).get(0);
    String thumbprint = PrivateKeyJwtUtils.computeSha256Thumbprint(cert);

    assertEquals(43, thumbprint.length()); // 256 bits, base64url, no padding
    assertFalse(thumbprint.matches(".*[+/=].*"));
    assertEquals(thumbprint, PrivateKeyJwtUtils.computeSha256Thumbprint(cert));
  }

  @Test
  void keyAndCertificateMatch() throws Exception {
    RSAPrivateKey key = PrivateKeyJwtUtils.loadPrivateKey(TestKeyMaterial.PRIVATE_KEY_PATH, null);
    RSAPublicKey pub =
        (RSAPublicKey)
            PrivateKeyJwtUtils.loadCertificateChain(TestKeyMaterial.CERTIFICATE_PATH)
                .get(0)
                .getPublicKey();
    assertEquals(key.getModulus(), pub.getModulus());
  }
}
