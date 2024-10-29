package security;

import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.codec.digest.HmacAlgorithms;

/** Auth Utils Adheres to HSEC requirement for creating application tokens */
public final class AuthUtil {

  private static final String HMAC_SHA256_ALGORITHM = HmacAlgorithms.HMAC_SHA_256.toString();
  private static final String DELIIMITER = ":";
  private static final String HEX_CHARS = "0123456789ABCDEF";

  private AuthUtil() {}

  /**
   * Generate hash string using the secret HMAC Key
   *
   * @param value value to be hashed
   * @param hmacKey secret HMAC key
   * @return Hashed string using the secret key
   * @throws NoSuchAlgorithmException
   * @throws InvalidKeyException
   */
  public static String generateHash(String value, byte[] hmacKey)
      throws NoSuchAlgorithmException, InvalidKeyException {
    // Time-stamp at Encryption time
    long tStamp = System.currentTimeMillis();
    String uTValue = new String();
    String cValue;
    String finalEncValue;

    // Concatenated Values
    uTValue = uTValue.concat(value).concat(":").concat(Long.toString(tStamp));
    cValue = uTValue;

    // Digest - HMAC-SHA256
    SecretKeySpec signingKey = new SecretKeySpec(hmacKey, HMAC_SHA256_ALGORITHM);
    Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
    mac.init(signingKey);

    byte[] rawHmac = mac.doFinal(uTValue.getBytes());
    String hmacString = getHex(rawHmac);
    finalEncValue =
        Base64.getEncoder()
            .encodeToString((cValue.concat(DELIIMITER).concat(hmacString).getBytes()));

    return finalEncValue;
  }

  /**
   * Validate the one-way hash string
   *
   * @param hashedValue Hashed value to be validated
   * @param hmacKey HMAC Key used to create the hash
   * @param sessionWindow previously defined session window to validate if the hash is expired
   * @return First element of the hash
   * @throws Exception
   */
  public static String verifyHash(String hashedValue, byte[] hmacKey, long sessionWindow)
      throws GeneralSecurityException {
    // Username:Timestamp:SignedHMAC(Username:Timestamp)
    String[] decryptedHash = decryptBase64Hash(hashedValue);
    String username = decryptedHash[0];
    String timestamp = decryptedHash[1];
    String signedHMAC = decryptedHash[2];
    long newTStamp = System.currentTimeMillis();
    String newUTValue = username.concat(DELIIMITER).concat(timestamp);

    // Digest - HMAC-SHA1 Verify
    SecretKeySpec signingKey = new SecretKeySpec(hmacKey, HMAC_SHA256_ALGORITHM);
    Mac mac = Mac.getInstance(HMAC_SHA256_ALGORITHM);
    mac.init(signingKey);
    String rawHmac2 = getHex(mac.doFinal(newUTValue.getBytes()));
    byte[] rawHmac2Bytes = rawHmac2.getBytes();

    if (!isEqual(rawHmac2Bytes, signedHMAC.getBytes())) {
      throw new GeneralSecurityException("Hash mismatch, tampered session data.");
    }

    if (newTStamp - Long.parseLong(decryptedHash[1]) >= sessionWindow) {
      throw new GeneralSecurityException("Session expired");
    }

    return decryptedHash[0];
  }

  /**
   * Decrypt base64 hash
   *
   * @param value base 64 hash string
   * @return Decrypted base 64 string
   */
  private static String[] decryptBase64Hash(String value) {
    String decodedBase64 = new String(Base64.getDecoder().decode(value));
    return decodedBase64.split(DELIIMITER);
  }

  /**
   * Get Hex string from byte array
   *
   * @param raw byte array
   * @return Hex representation of the byte array
   */
  private static String getHex(byte[] raw) {
    if (raw == null) {
      return null;
    }

    final StringBuilder hex = new StringBuilder(2 * raw.length);

    for (final byte b : raw) {
      hex.append(HEX_CHARS.charAt((b & 0xF0) >> 4)).append(HEX_CHARS.charAt((b & 0x0F)));
    }

    return hex.toString();
  }

  /**
   * Compares two HMAC byte arrays
   *
   * @param a HMAC byte array 1
   * @param b HMAC byte array 2
   * @return true if the two HMAC are identical
   */
  private static boolean isEqual(byte[] a, byte[] b) {
    if (a == null || b == null || a.length != b.length) {
      return false;
    }

    int result = 0;

    for (int i = 0; i < a.length; i++) {
      result |= a[i] ^ b[i];
    }

    return result == 0;
  }
}
