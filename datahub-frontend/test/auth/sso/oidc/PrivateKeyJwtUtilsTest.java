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

  private static final String TEST_RESOURCES_PATH = "test/resources/";

  @Test
  void testLoadPrivateKey_ValidUnencryptedPem() throws IOException {
    String keyPath = TEST_RESOURCES_PATH + "test-private-key.pem";

    RSAPrivateKey privateKey = PrivateKeyJwtUtils.loadPrivateKey(keyPath, null);

    assertNotNull(privateKey);
    assertEquals("RSA", privateKey.getAlgorithm());
    assertNotNull(privateKey.getModulus());
    assertTrue(privateKey.getModulus().bitLength() >= 2048);
  }

  @Test
  void testLoadPrivateKey_ValidEncryptedPem() throws IOException {
    String keyPath = TEST_RESOURCES_PATH + "test-private-key-encrypted.pem";

    RSAPrivateKey privateKey = PrivateKeyJwtUtils.loadPrivateKey(keyPath, "testpassword");

    assertNotNull(privateKey);
    assertEquals("RSA", privateKey.getAlgorithm());
    assertNotNull(privateKey.getModulus());
  }

  @Test
  void testLoadPrivateKey_EncryptedPemWithoutPassword() {
    String keyPath = TEST_RESOURCES_PATH + "test-private-key-encrypted.pem";

    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> PrivateKeyJwtUtils.loadPrivateKey(keyPath, null));

    assertTrue(exception.getMessage().contains("encrypted"));
  }

  @Test
  void testLoadPrivateKey_EncryptedPemWithWrongPassword() {
    String keyPath = TEST_RESOURCES_PATH + "test-private-key-encrypted.pem";

    assertThrows(
        IllegalArgumentException.class,
        () -> PrivateKeyJwtUtils.loadPrivateKey(keyPath, "wrongpassword"));
  }

  @Test
  void testLoadPrivateKey_InvalidPath() {
    String keyPath = TEST_RESOURCES_PATH + "nonexistent-key.pem";

    assertThrows(IOException.class, () -> PrivateKeyJwtUtils.loadPrivateKey(keyPath, null));
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
    String certPath = TEST_RESOURCES_PATH + "test-certificate.pem";

    X509Certificate certificate = PrivateKeyJwtUtils.loadCertificate(certPath);

    assertNotNull(certificate);
    assertEquals("X.509", certificate.getType());
    assertNotNull(certificate.getSubjectX500Principal());
  }

  @Test
  void testLoadCertificate_InvalidPath() {
    String certPath = TEST_RESOURCES_PATH + "nonexistent-cert.pem";

    assertThrows(IOException.class, () -> PrivateKeyJwtUtils.loadCertificate(certPath));
  }

  @Test
  void testComputeThumbprint() throws IOException, CertificateException {
    String certPath = TEST_RESOURCES_PATH + "test-certificate.pem";

    X509Certificate certificate = PrivateKeyJwtUtils.loadCertificate(certPath);
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
    String certPath = TEST_RESOURCES_PATH + "test-certificate.pem";

    X509Certificate certificate = PrivateKeyJwtUtils.loadCertificate(certPath);
    String thumbprint1 = PrivateKeyJwtUtils.computeThumbprint(certificate);
    String thumbprint2 = PrivateKeyJwtUtils.computeThumbprint(certificate);

    assertEquals(thumbprint1, thumbprint2, "Thumbprint should be consistent for same certificate");
  }

  @Test
  void testKeyAndCertificateMatch() throws Exception {
    String keyPath = TEST_RESOURCES_PATH + "test-private-key.pem";
    String certPath = TEST_RESOURCES_PATH + "test-certificate.pem";

    RSAPrivateKey privateKey = PrivateKeyJwtUtils.loadPrivateKey(keyPath, null);
    X509Certificate certificate = PrivateKeyJwtUtils.loadCertificate(certPath);

    // The public key in the certificate should have the same modulus as the private key
    java.security.interfaces.RSAPublicKey publicKey =
        (java.security.interfaces.RSAPublicKey) certificate.getPublicKey();

    assertEquals(
        privateKey.getModulus(),
        publicKey.getModulus(),
        "Private key and certificate should form a matching key pair");
  }
}
