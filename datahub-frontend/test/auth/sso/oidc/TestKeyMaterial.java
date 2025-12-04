package auth.sso.oidc;

import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Security;
import java.security.cert.X509Certificate;
import java.util.Date;
import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PKCS8Generator;
import org.bouncycastle.openssl.jcajce.JcaPEMWriter;
import org.bouncycastle.openssl.jcajce.JceOpenSSLPKCS8EncryptorBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.OutputEncryptor;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.openssl.jcajce.JcaPKCS8Generator;
import org.bouncycastle.util.io.pem.PemObject;

/**
 * Generates an ephemeral RSA keypair and self-signed X.509 certificate at JVM startup, writing them
 * as PEM files into a temp directory. Used by the OIDC private_key_jwt tests instead of committing
 * fixture PEMs (which secret scanners flag even when the keys are throwaway).
 */
public final class TestKeyMaterial {

  public static final String ENCRYPTED_KEY_PASSWORD = "testpassword";

  public static final String PRIVATE_KEY_PATH;
  public static final String ENCRYPTED_PRIVATE_KEY_PATH;
  public static final String CERTIFICATE_PATH;

  static {
    try {
      if (Security.getProvider(BouncyCastleProvider.PROVIDER_NAME) == null) {
        Security.addProvider(new BouncyCastleProvider());
      }

      Path dir = Files.createTempDirectory("datahub-oidc-test-keys");
      dir.toFile().deleteOnExit();

      KeyPairGenerator kpg = KeyPairGenerator.getInstance("RSA");
      kpg.initialize(2048);
      KeyPair keyPair = kpg.generateKeyPair();

      X509Certificate certificate = generateSelfSignedCertificate(keyPair);

      Path certPath = dir.resolve("cert.pem");
      writePem(certPath, certificate);

      Path keyPath = dir.resolve("key.pem");
      writePem(keyPath, keyPair.getPrivate());

      Path encKeyPath = dir.resolve("key-encrypted.pem");
      writeEncryptedPkcs8(encKeyPath, keyPair, ENCRYPTED_KEY_PASSWORD);

      PRIVATE_KEY_PATH = keyPath.toString();
      ENCRYPTED_PRIVATE_KEY_PATH = encKeyPath.toString();
      CERTIFICATE_PATH = certPath.toString();
    } catch (Exception e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private TestKeyMaterial() {}

  private static X509Certificate generateSelfSignedCertificate(KeyPair keyPair) throws Exception {
    X500Name subject = new X500Name("CN=datahub-oidc-test");
    BigInteger serial = BigInteger.valueOf(System.currentTimeMillis());
    Date notBefore = new Date(System.currentTimeMillis() - 60_000L);
    Date notAfter = new Date(System.currentTimeMillis() + 365L * 24 * 60 * 60 * 1000L);

    JcaX509v3CertificateBuilder builder =
        new JcaX509v3CertificateBuilder(
            subject, serial, notBefore, notAfter, subject, keyPair.getPublic());

    ContentSigner signer =
        new JcaContentSignerBuilder("SHA256withRSA")
            .setProvider(BouncyCastleProvider.PROVIDER_NAME)
            .build(keyPair.getPrivate());

    return new JcaX509CertificateConverter()
        .setProvider(BouncyCastleProvider.PROVIDER_NAME)
        .getCertificate(builder.build(signer));
  }

  private static void writePem(Path path, Object object) throws IOException {
    try (JcaPEMWriter writer = new JcaPEMWriter(Files.newBufferedWriter(path))) {
      writer.writeObject(object);
    }
  }

  private static void writeEncryptedPkcs8(Path path, KeyPair keyPair, String password)
      throws Exception {
    OutputEncryptor encryptor =
        new JceOpenSSLPKCS8EncryptorBuilder(PKCS8Generator.AES_256_CBC)
            .setProvider(BouncyCastleProvider.PROVIDER_NAME)
            .setPassword(password.toCharArray())
            .build();
    PemObject pem = new JcaPKCS8Generator(keyPair.getPrivate(), encryptor).generate();
    try (JcaPEMWriter writer = new JcaPEMWriter(Files.newBufferedWriter(path))) {
      writer.writeObject(pem);
    }
  }
}
