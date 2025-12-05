package auth.sso.oidc;

import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.util.Base64;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;

/**
 * Utility class for loading private keys and certificates for private_key_jwt client
 * authentication (RFC 7523).
 */
public final class PrivateKeyJwtUtils {

  static {
    // Register BouncyCastle as a JCA security provider for encrypted key support
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
  }

  private PrivateKeyJwtUtils() {}

  /**
   * Loads an RSA private key from a PEM file.
   *
   * <p>Supports both encrypted (password-protected) and unencrypted PEM files. The PEM file can
   * contain:
   *
   * <ul>
   *   <li>PKCS#8 format (BEGIN PRIVATE KEY / BEGIN ENCRYPTED PRIVATE KEY)
   *   <li>Traditional RSA format (BEGIN RSA PRIVATE KEY)
   * </ul>
   *
   * @param filePath Path to the PEM file containing the private key
   * @param password Password for encrypted keys, or null for unencrypted keys
   * @return The RSA private key
   * @throws IOException If the file cannot be read
   * @throws IllegalArgumentException If the key format is invalid or unsupported
   */
  public static RSAPrivateKey loadPrivateKey(@Nonnull String filePath, @Nullable String password)
      throws IOException {
    try (PEMParser pemParser = new PEMParser(new FileReader(filePath))) {
      Object pemObject = pemParser.readObject();

      if (pemObject == null) {
        throw new IllegalArgumentException(
            "No PEM object found in file: " + filePath);
      }

      JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
      PrivateKeyInfo privateKeyInfo;

      if (pemObject instanceof PEMEncryptedKeyPair) {
        // Encrypted traditional format (e.g., BEGIN RSA PRIVATE KEY with encryption)
        if (password == null || password.isEmpty()) {
          throw new IllegalArgumentException(
              "Private key is encrypted but no password was provided");
        }
        PEMDecryptorProvider decryptor =
            new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
        PEMKeyPair keyPair = ((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decryptor);
        privateKeyInfo = keyPair.getPrivateKeyInfo();

      } else if (pemObject instanceof PEMKeyPair) {
        // Unencrypted traditional format (BEGIN RSA PRIVATE KEY)
        privateKeyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();

      } else if (pemObject instanceof PrivateKeyInfo) {
        // PKCS#8 format (BEGIN PRIVATE KEY)
        privateKeyInfo = (PrivateKeyInfo) pemObject;

      } else if (pemObject instanceof org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo) {
        // Encrypted PKCS#8 format (BEGIN ENCRYPTED PRIVATE KEY)
        if (password == null || password.isEmpty()) {
          throw new IllegalArgumentException(
              "Private key is encrypted but no password was provided");
        }
        org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo encryptedInfo =
            (org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo) pemObject;
        org.bouncycastle.operator.InputDecryptorProvider decryptorProvider =
            new org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder()
                .build(password.toCharArray());
        privateKeyInfo = encryptedInfo.decryptPrivateKeyInfo(decryptorProvider);

      } else {
        throw new IllegalArgumentException(
            "Unsupported PEM object type: " + pemObject.getClass().getName());
      }

      return (RSAPrivateKey) converter.getPrivateKey(privateKeyInfo);

    } catch (org.bouncycastle.operator.OperatorCreationException
        | org.bouncycastle.pkcs.PKCSException e) {
      throw new IllegalArgumentException("Failed to decrypt private key: " + e.getMessage(), e);
    }
  }

  /**
   * Loads an X.509 certificate from a PEM file.
   *
   * @param filePath Path to the PEM file containing the certificate
   * @return The X.509 certificate
   * @throws IOException If the file cannot be read
   * @throws CertificateException If the certificate is invalid
   */
  public static X509Certificate loadCertificate(@Nonnull String filePath)
      throws IOException, CertificateException {
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    try (var inputStream = Files.newInputStream(Path.of(filePath))) {
      return (X509Certificate) certFactory.generateCertificate(inputStream);
    }
  }

  /**
   * Computes the SHA-256 thumbprint of an X.509 certificate, Base64URL encoded.
   *
   * <p>This is used as the key ID (kid) in JWT headers for private_key_jwt authentication. Azure AD
   * and other IdPs use this format to identify which certificate was used to sign the JWT.
   *
   * @param certificate The X.509 certificate
   * @return The SHA-256 thumbprint, Base64URL encoded (no padding)
   * @throws CertificateEncodingException If the certificate cannot be encoded
   */
  public static String computeThumbprint(@Nonnull X509Certificate certificate)
      throws CertificateEncodingException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(certificate.getEncoded());
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is always available in Java
      throw new RuntimeException("SHA-256 algorithm not available", e);
    }
  }

}
