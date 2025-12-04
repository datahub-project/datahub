package auth.sso.oidc;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
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
  void testLoadCertificate_ValidPem() throws IOException, CertificateException {
    X509Certificate certificate =
        PrivateKeyJwtUtils.loadCertificate(TestKeyMaterial.CERTIFICATE_PATH);

    assertNotNull(certificate);
    assertEquals("X.509", certificate.getType());
    assertNotNull(certificate.getSubjectX500Principal());
  }

  @Test
  void testLoadCertificate_InvalidPath() {
    assertThrows(
        IOException.class,
        () -> PrivateKeyJwtUtils.loadCertificate("test/resources/nonexistent-cert.pem"));
  }

  @Test
  void testComputeThumbprint() throws IOException, CertificateException {
    X509Certificate certificate =
        PrivateKeyJwtUtils.loadCertificate(TestKeyMaterial.CERTIFICATE_PATH);
    String thumbprint = PrivateKeyJwtUtils.computeThumbprint(certificate);

    assertNotNull(thumbprint);
    // Base64URL encoded SHA-256 hash should be 43 characters (256 bits / 6 bits per char, no
    // padding)
    assertEquals(43, thumbprint.length());
    // Should be Base64URL safe (no + or /)
    assertFalse(thumbprint.contains("+"));
    assertFalse(thumbprint.contains("/"));
    assertFalse(thumbprint.contains("="));
  }

  @Test
  void testComputeThumbprint_Consistency() throws IOException, CertificateException {
    X509Certificate certificate =
        PrivateKeyJwtUtils.loadCertificate(TestKeyMaterial.CERTIFICATE_PATH);
    String thumbprint1 = PrivateKeyJwtUtils.computeThumbprint(certificate);
    String thumbprint2 = PrivateKeyJwtUtils.computeThumbprint(certificate);

    assertEquals(thumbprint1, thumbprint2, "Thumbprint should be consistent for same certificate");
  }

  @Test
  void testKeyAndCertificateMatch() throws Exception {
    RSAPrivateKey privateKey =
        PrivateKeyJwtUtils.loadPrivateKey(TestKeyMaterial.PRIVATE_KEY_PATH, null);
    X509Certificate certificate =
        PrivateKeyJwtUtils.loadCertificate(TestKeyMaterial.CERTIFICATE_PATH);

    // The public key in the certificate should have the same modulus as the private key
    java.security.interfaces.RSAPublicKey publicKey =
        (java.security.interfaces.RSAPublicKey) certificate.getPublicKey();

    assertEquals(
        privateKey.getModulus(),
        publicKey.getModulus(),
        "Private key and certificate should form a matching key pair");
  }
}
