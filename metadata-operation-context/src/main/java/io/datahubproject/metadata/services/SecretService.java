package io.datahubproject.metadata.services;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class SecretService {
  private static final int LOWERCASE_ASCII_START = 97;
  private static final int LOWERCASE_ASCII_END = 122;
  public static final String HASHING_ALGORITHM = "SHA-256";

  private final String _secret;
  private final SecureRandom _secureRandom;
  private final Base64.Encoder _encoder;
  private final Base64.Decoder _decoder;
  private final MessageDigest _messageDigest;

  public SecretService(final String secret) {
    _secret = secret;
    _secureRandom = new SecureRandom();
    _encoder = Base64.getEncoder();
    _decoder = Base64.getDecoder();
    try {
      _messageDigest = MessageDigest.getInstance(HASHING_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unable to create MessageDigest", e);
    }
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
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.ENCRYPT_MODE, secretKey);
      return _encoder.encodeToString(cipher.doFinal(value.getBytes(StandardCharsets.UTF_8)));
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
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      return new String(cipher.doFinal(_decoder.decode(encryptedValue)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt value using provided secret!", e);
    }
  }

  public String generateUrlSafeToken(int length) {
    return _secureRandom
        .ints(length, LOWERCASE_ASCII_START, LOWERCASE_ASCII_END + 1)
        .mapToObj(i -> String.valueOf((char) i))
        .collect(Collectors.joining());
  }

  public String hashString(@Nonnull final String str) {
    byte[] hashedBytes = _messageDigest.digest(str.getBytes());
    return _encoder.encodeToString(hashedBytes);
  }

  public byte[] generateSalt(int length) {
    byte[] randomBytes = new byte[length];
    _secureRandom.nextBytes(randomBytes);
    return randomBytes;
  }

  public String getHashedPassword(@Nonnull byte[] salt, @Nonnull String password)
      throws IOException {
    byte[] saltedPassword = saltPassword(salt, password);
    byte[] hashedPassword = _messageDigest.digest(saltedPassword);
    return _encoder.encodeToString(hashedPassword);
  }

  byte[] saltPassword(@Nonnull byte[] salt, @Nonnull String password) throws IOException {
    byte[] passwordBytes = password.getBytes();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.write(salt);
    byteArrayOutputStream.write(passwordBytes);
    return byteArrayOutputStream.toByteArray();
  }
}
