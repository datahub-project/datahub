package com.linkedin.metadata.secret;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;


public class SecretService {

  private final String _secret;

  public SecretService(final String secret) {
    _secret = secret;
  }

  public String encrypt(String value) {
    try {
      SecretKeySpec secretKey = null;
      byte[] key;
      MessageDigest sha = null;
      try {
        key = _secret.getBytes(StandardCharsets.UTF_8);
        sha = MessageDigest.getInstance("SHA-1");
        key = sha.digest(key);
        key = Arrays.copyOf(key, 16);
        secretKey = new SecretKeySpec(key, "AES");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      return Base64.getEncoder().encodeToString(cipher.doFinal(value.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt value using provided secret!", e);
    }
  }

  public String decrypt(String encryptedValue) {
    try {
      SecretKeySpec secretKey = null;
      byte[] key;
      MessageDigest sha = null;
      try {
        key = _secret.getBytes(StandardCharsets.UTF_8);
        sha = MessageDigest.getInstance("SHA-1");
        key = sha.digest(key);
        key = Arrays.copyOf(key, 16);
        secretKey = new SecretKeySpec(key, "AES");
      } catch (NoSuchAlgorithmException e) {
        e.printStackTrace();
      }
      Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING");
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      return new String(cipher.doFinal(Base64.getDecoder().decode(encryptedValue)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt value using provided secret!", e);
    }
  }
}
