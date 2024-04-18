package com.linkedin.datahub.graphql.resolvers.ingest.secret;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

/** Utility methods to encrypt and decrypt DataHub secrets. */
public class SecretUtils {

  static String encrypt(String value, String secret) {
    try {
      SecretKeySpec secretKey = null;
      byte[] key;
      MessageDigest sha = null;
      try {
        key = secret.getBytes(StandardCharsets.UTF_8);
        sha = MessageDigest.getInstance("SHA-1");
        key = sha.digest(key);
        key = Arrays.copyOf(key, 16);
        secretKey = new SecretKeySpec(key, "AES");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      return Base64.getEncoder()
          .encodeToString(cipher.doFinal(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt value using provided secret!");
    }
  }

  static String decrypt(String encryptedValue, String secret) {
    try {
      SecretKeySpec secretKey = null;
      byte[] key;
      MessageDigest sha = null;
      try {
        key = secret.getBytes(StandardCharsets.UTF_8);
        sha = MessageDigest.getInstance("SHA-1");
        key = sha.digest(key);
        key = Arrays.copyOf(key, 16);
        secretKey = new SecretKeySpec(key, "AES");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedValue)));
    } catch (Exception e) {
      System.out.println("Error while decrypting: " + e.toString());
    }
    return null;
  }

  private SecretUtils() {}
}
