package auth.pac4j;

import java.security.SecureRandom;
import org.apache.shiro.crypto.cipher.AesCipherService;
import org.pac4j.core.util.CommonHelper;
import org.pac4j.play.store.DataEncrypter;

/**
 * AES session cookie encryption compatible with {@code PlayCookieSessionStore}, using Apache Shiro
 * 2.x cryptography (same behavior as legacy pac4j {@code ShiroAesDataEncrypter} against Shiro 1.x).
 */
public final class Shiro2AesDataEncrypter implements DataEncrypter {

  private static final SecureRandom random = new SecureRandom();

  private final AesCipherService aesCipherService = new AesCipherService();
  private final byte[] key;

  public Shiro2AesDataEncrypter(final byte[] key) {
    CommonHelper.assertNotNull("key", key);
    this.key = key.clone();
  }

  public Shiro2AesDataEncrypter() {
    byte[] bytes = new byte[16];
    random.nextBytes(bytes);
    this.key = bytes;
  }

  @Override
  public byte[] decrypt(final byte[] encryptedBytes) {
    if (encryptedBytes == null) {
      return null;
    }
    return aesCipherService.decrypt(encryptedBytes, key).getClonedBytes();
  }

  @Override
  public byte[] encrypt(final byte[] rawBytes) {
    if (rawBytes == null) {
      return null;
    }
    return aesCipherService.encrypt(rawBytes, key).getBytes();
  }
}
