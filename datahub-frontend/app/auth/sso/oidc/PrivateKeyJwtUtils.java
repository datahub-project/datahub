package auth.sso.oidc;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.RSAPrivateKey;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8DecryptorProviderBuilder;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.operator.InputDecryptorProvider;
import org.bouncycastle.operator.OperatorCreationException;
import org.bouncycastle.pkcs.PKCS8EncryptedPrivateKeyInfo;
import org.bouncycastle.pkcs.PKCSException;

/**
 * Utility class for loading PEM-encoded private keys and X.509 certificates for private_key_jwt
 * client authentication (RFC 7523).
 *
 * <p>PEM is the canonical format. The following PEM variants are supported:
 *
 * <ul>
 *   <li>Private keys: {@code BEGIN PRIVATE KEY} (PKCS#8), {@code BEGIN ENCRYPTED PRIVATE KEY}
 *       (encrypted PKCS#8), {@code BEGIN RSA PRIVATE KEY} (traditional OpenSSL), and its encrypted
 *       variant.
 *   <li>Certificates: {@code BEGIN CERTIFICATE} (X.509). Files containing multiple consecutive
 *       {@code CERTIFICATE} blocks are treated as a chain (leaf first).
 * </ul>
 *
 * <p>DER-encoded X.509 certificates are also accepted because {@link CertificateFactory}
 * transparently handles them. Non-PEM private key formats (PKCS#12, JKS) are not supported here;
 * convert them with {@code openssl pkcs12 -in keystore.p12 -nodes -out key.pem}.
 */
public final class PrivateKeyJwtUtils {

  static {
    // Register BouncyCastle as a JCA security provider for encrypted key support.
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
  }

  private PrivateKeyJwtUtils() {}

  /**
   * Loads an RSA private key from a PEM file.
   *
   * @param filePath Path to the PEM file containing the private key.
   * @param password Password for encrypted keys, or {@code null} for unencrypted keys.
   * @return The RSA private key.
   * @throws IOException If the file cannot be read.
   * @throws IllegalArgumentException If the PEM contents are missing, malformed, or encrypted
   *     without a password.
   */
  public static RSAPrivateKey loadPrivateKey(@Nonnull String filePath, @Nullable String password)
      throws IOException {
    try (PEMParser pemParser =
        new PEMParser(Files.newBufferedReader(Path.of(filePath), StandardCharsets.US_ASCII))) {
      Object pemObject = pemParser.readObject();

      if (pemObject == null) {
        throw new IllegalArgumentException("No PEM object found in file: " + filePath);
      }

      JcaPEMKeyConverter converter = new JcaPEMKeyConverter();
      PrivateKeyInfo privateKeyInfo;

      if (pemObject instanceof PEMEncryptedKeyPair) {
        // Encrypted traditional OpenSSL format (BEGIN RSA PRIVATE KEY with DEK-Info header).
        if (password == null || password.isEmpty()) {
          throw new IllegalArgumentException(
              "Private key is encrypted but no password was provided");
        }
        PEMDecryptorProvider decryptor =
            new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
        PEMKeyPair keyPair = ((PEMEncryptedKeyPair) pemObject).decryptKeyPair(decryptor);
        privateKeyInfo = keyPair.getPrivateKeyInfo();

      } else if (pemObject instanceof PEMKeyPair) {
        // Unencrypted traditional OpenSSL format (BEGIN RSA PRIVATE KEY).
        privateKeyInfo = ((PEMKeyPair) pemObject).getPrivateKeyInfo();

      } else if (pemObject instanceof PrivateKeyInfo) {
        // Unencrypted PKCS#8 (BEGIN PRIVATE KEY).
        privateKeyInfo = (PrivateKeyInfo) pemObject;

      } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo) {
        // Encrypted PKCS#8 (BEGIN ENCRYPTED PRIVATE KEY).
        if (password == null || password.isEmpty()) {
          throw new IllegalArgumentException(
              "Private key is encrypted but no password was provided");
        }
        InputDecryptorProvider decryptorProvider =
            new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password.toCharArray());
        privateKeyInfo =
            ((PKCS8EncryptedPrivateKeyInfo) pemObject).decryptPrivateKeyInfo(decryptorProvider);

      } else {
        throw new IllegalArgumentException(
            "Unsupported PEM object type: " + pemObject.getClass().getName());
      }

      return (RSAPrivateKey) converter.getPrivateKey(privateKeyInfo);

    } catch (OperatorCreationException | PKCSException e) {
      throw new IllegalArgumentException("Failed to decrypt private key: " + e.getMessage(), e);
    }
  }

  /**
   * Loads an X.509 certificate chain from a file. If the file contains multiple concatenated PEM
   * {@code CERTIFICATE} blocks, they are returned in file order (leaf first, then intermediates).
   * DER-encoded single certificates are also accepted.
   *
   * @param filePath Path to the certificate file.
   * @return The certificate chain — never empty.
   * @throws IOException If the file cannot be read.
   * @throws CertificateException If no certificates are present or any entry cannot be parsed.
   */
  public static List<X509Certificate> loadCertificateChain(@Nonnull String filePath)
      throws IOException, CertificateException {
    CertificateFactory certFactory = CertificateFactory.getInstance("X.509");
    try (InputStream in = Files.newInputStream(Path.of(filePath))) {
      Collection<? extends Certificate> certs = certFactory.generateCertificates(in);
      if (certs.isEmpty()) {
        throw new CertificateException("No certificates found in file: " + filePath);
      }
      List<X509Certificate> chain = new ArrayList<>(certs.size());
      for (Certificate c : certs) {
        chain.add((X509Certificate) c);
      }
      return chain;
    }
  }

  /**
   * Computes the SHA-256 thumbprint of an X.509 certificate, Base64URL encoded without padding.
   * This is the canonical form of the JWT {@code x5t#S256} header parameter (RFC 7515 §4.1.8) and
   * is also a reasonable default for {@code kid} when none is configured.
   *
   * @param certificate The X.509 certificate.
   * @return Base64URL-encoded SHA-256 thumbprint (43 characters, no padding).
   * @throws CertificateEncodingException If the certificate cannot be encoded.
   */
  public static String computeSha256Thumbprint(@Nonnull X509Certificate certificate)
      throws CertificateEncodingException {
    try {
      MessageDigest digest = MessageDigest.getInstance("SHA-256");
      byte[] hash = digest.digest(certificate.getEncoded());
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      // SHA-256 is a JRE-required algorithm.
      throw new AssertionError("SHA-256 algorithm not available", e);
    }
  }
}
