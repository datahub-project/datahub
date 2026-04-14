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
 * PEM key/certificate loaders for {@code private_key_jwt} client authentication (RFC 7523).
 * Supports PKCS#8 ({@code BEGIN [ENCRYPTED] PRIVATE KEY}) and traditional OpenSSL ({@code BEGIN RSA
 * PRIVATE KEY}, encrypted or not) for keys, and PEM or DER X.509 for certificates (multi-cert PEMs
 * are treated as a chain, leaf first). For PKCS#12 / JKS sources, convert first with {@code openssl
 * pkcs12 -in keystore.p12 -nodes -out key.pem}.
 */
public final class PrivateKeyJwtUtils {

  static {
    if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
      Security.addProvider(new BouncyCastleProvider());
    }
  }

  private PrivateKeyJwtUtils() {}

  /**
   * @param password password for encrypted keys, or {@code null} for unencrypted keys
   * @throws IllegalArgumentException if the PEM is missing, malformed, or encrypted without a
   *     password
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

      if (pemObject instanceof PEMEncryptedKeyPair encrypted) {
        // BEGIN RSA PRIVATE KEY with DEK-Info header
        requirePassword(password);
        PEMDecryptorProvider decryptor =
            new JcePEMDecryptorProviderBuilder().build(password.toCharArray());
        privateKeyInfo = encrypted.decryptKeyPair(decryptor).getPrivateKeyInfo();
      } else if (pemObject instanceof PEMKeyPair keyPair) {
        // BEGIN RSA PRIVATE KEY
        privateKeyInfo = keyPair.getPrivateKeyInfo();
      } else if (pemObject instanceof PrivateKeyInfo info) {
        // BEGIN PRIVATE KEY (PKCS#8)
        privateKeyInfo = info;
      } else if (pemObject instanceof PKCS8EncryptedPrivateKeyInfo encrypted) {
        // BEGIN ENCRYPTED PRIVATE KEY (PKCS#8)
        requirePassword(password);
        InputDecryptorProvider decryptor =
            new JceOpenSSLPKCS8DecryptorProviderBuilder().build(password.toCharArray());
        privateKeyInfo = encrypted.decryptPrivateKeyInfo(decryptor);
      } else {
        throw new IllegalArgumentException(
            "Unsupported PEM object type: " + pemObject.getClass().getName());
      }

      return (RSAPrivateKey) converter.getPrivateKey(privateKeyInfo);

    } catch (OperatorCreationException | PKCSException e) {
      throw new IllegalArgumentException("Failed to decrypt private key: " + e.getMessage(), e);
    }
  }

  private static void requirePassword(@Nullable String password) {
    if (password == null || password.isEmpty()) {
      throw new IllegalArgumentException("Private key is encrypted but no password was provided");
    }
  }

  /** Returns the cert chain in file order (leaf first). Never empty. */
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

  /** Base64URL-encoded SHA-256 thumbprint (RFC 7515 §4.1.8 {@code x5t#S256}). */
  public static String computeSha256Thumbprint(@Nonnull X509Certificate certificate)
      throws CertificateEncodingException {
    try {
      byte[] hash = MessageDigest.getInstance("SHA-256").digest(certificate.getEncoded());
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (NoSuchAlgorithmException e) {
      throw new AssertionError("SHA-256 algorithm not available", e); // JRE-required algorithm
    }
  }
}
