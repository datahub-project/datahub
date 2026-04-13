package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PrivateKeyJwtUtilsTest {

  @Test
  void testLoadPrivateKey_ValidUnencryptedPem() throws IOException {
    RSAPrivateKey privateKey =
        PrivateKeyJwtUtils.loadPrivateKey(TestKeyMaterial.PRIVATE_KEY_PATH, null);

    assertNotNull(privateKey);
    assertEquals("RSA", privateKey.getAlgorithm());
    assertNotNull(privateKey.getModulus());
    assertTrue(privateKey.getModulus().bitLength() >= 2048);
  }

  @Test
  void testLoadPrivateKey_ValidEncryptedPem() throws IOException {
    RSAPrivateKey privateKey =
        PrivateKeyJwtUtils.loadPrivateKey(
            TestKeyMaterial.ENCRYPTED_PRIVATE_KEY_PATH, TestKeyMaterial.ENCRYPTED_KEY_PASSWORD);

    assertNotNull(privateKey);
    assertEquals("RSA", privateKey.getAlgorithm());
    assertNotNull(privateKey.getModulus());
  }

  @Test
  void testLoadPrivateKey_EncryptedPemWithoutPassword() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                PrivateKeyJwtUtils.loadPrivateKey(
                    TestKeyMaterial.ENCRYPTED_PRIVATE_KEY_PATH, null));

    assertTrue(exception.getMessage().contains("encrypted"));
  }

  @Test
  void testLoadPrivateKey_EncryptedPemWithWrongPassword() {
    assertThrows(
        IllegalArgumentException.class,
        () ->
            PrivateKeyJwtUtils.loadPrivateKey(
                TestKeyMaterial.ENCRYPTED_PRIVATE_KEY_PATH, "wrongpassword"));
  }

  @Test
  void testLoadPrivateKey_InvalidPath() {
    assertThrows(
        IOException.class,
        () -> PrivateKeyJwtUtils.loadPrivateKey("test/resources/nonexistent-key.pem", null));
  }

  @Test
  void testLoadPrivateKey_EmptyFile(@TempDir Path tempDir) throws IOException {
    Path emptyFile = tempDir.resolve("empty.pem");
    Files.writeString(emptyFile, "");

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> PrivateKeyJwtUtils.loadPrivateKey(emptyFile.toString(), null));

    assertTrue(exception.getMessage().contains("No PEM object found"));
  }

  @Test
  void testLoadCertificateChain_ValidPem() throws IOException, CertificateException {
    List<X509Certificate> chain =
        PrivateKeyJwtUtils.loadCertificateChain(TestKeyMaterial.CERTIFICATE_PATH);

    assertFalse(chain.isEmpty());
    X509Certificate leaf = chain.get(0);
    assertEquals("X.509", leaf.getType());
    assertNotNull(leaf.getSubjectX500Principal());
  }

  @Test
  void testLoadCertificateChain_InvalidPath() {
    assertThrows(
        IOException.class,
        () -> PrivateKeyJwtUtils.loadCertificateChain("test/resources/nonexistent-cert.pem"));
  }

  @Test
  void testComputeSha256Thumbprint_FormatAndStability() throws IOException, CertificateException {
    X509Certificate certificate =
        PrivateKeyJwtUtils.loadCertificateChain(TestKeyMaterial.CERTIFICATE_PATH).get(0);
    String thumbprint = PrivateKeyJwtUtils.computeSha256Thumbprint(certificate);

    // SHA-256 base64url without padding: 256 bits / 6 bits per char = 43 characters.
    assertEquals(43, thumbprint.length());
    assertFalse(thumbprint.contains("+"));
    assertFalse(thumbprint.contains("/"));
    assertFalse(thumbprint.contains("="));
    assertEquals(
        thumbprint,
        PrivateKeyJwtUtils.computeSha256Thumbprint(certificate),
        "Thumbprint must be deterministic for the same certificate");
  }

  @Test
  void testKeyAndCertificateMatch() throws Exception {
    RSAPrivateKey privateKey =
        PrivateKeyJwtUtils.loadPrivateKey(TestKeyMaterial.PRIVATE_KEY_PATH, null);
    X509Certificate certificate =
        PrivateKeyJwtUtils.loadCertificateChain(TestKeyMaterial.CERTIFICATE_PATH).get(0);

    // The public key in the certificate should have the same modulus as the private key
    java.security.interfaces.RSAPublicKey publicKey =
        (java.security.interfaces.RSAPublicKey) certificate.getPublicKey();

    assertEquals(
        privateKey.getModulus(),
        publicKey.getModulus(),
        "Private key and certificate should form a matching key pair");
  }
}
