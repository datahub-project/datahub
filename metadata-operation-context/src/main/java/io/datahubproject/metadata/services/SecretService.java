package io.datahubproject.metadata.services;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.crypto.Cipher;
import javax.crypto.Mac;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

public class SecretService {
  private static final int LOWERCASE_ASCII_START = 97;
  private static final int LOWERCASE_ASCII_END = 122;
  public static final String HASHING_ALGORITHM = "SHA-256";

  // GCM constants - matching AWS encryption standards
  private static final String AES_GCM_ALGORITHM = "AES/GCM/NoPadding";
  private static final String AES_ALGORITHM = "AES";
  private static final int GCM_IV_LENGTH = 12; // 96 bits (AWS standard)
  private static final int GCM_TAG_LENGTH = 128; // 128 bits (AWS standard)
  private static final int AES_KEY_SIZE = 256; // 256 bits (AWS-256 standard)

  // Key derivation constants
  private static final String HMAC_ALGORITHM = "HmacSHA256";
  private static final String KEY_DERIVATION_INFO = "DataHub-AES-GCM-Key";

  // Version prefix to identify new encryption format
  private static final String VERSION_PREFIX = "v2:";

  private final String _secret;
  private final SecureRandom _secureRandom;
  private final Base64.Encoder _encoder;
  private final Base64.Decoder _decoder;
  private final MessageDigest _messageDigest;
  private final SecretKeySpec _derivedKey;
  private final boolean v1AlgorithmEnabled;

  public SecretService(final String secret, final boolean v1AlgorithmEnabled) {
    _secret = secret;
    _secureRandom = new SecureRandom();
    _encoder = Base64.getEncoder();
    _decoder = Base64.getDecoder();
    this.v1AlgorithmEnabled = v1AlgorithmEnabled;

    try {
      _messageDigest = MessageDigest.getInstance(HASHING_ALGORITHM);
    } catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("Unable to create MessageDigest", e);
    }

    // Derive key using HMAC-based key derivation (deterministic, no salt needed)
    _derivedKey = deriveKey(_secret);
  }

  /**
   * Encrypts a value using AES-GCM encryption. The output format is: "v2:base64(iv || ciphertext ||
   * tag)"
   */
  public String encrypt(String value) {
    if (value == null) {
      throw new IllegalArgumentException("Value to encrypt cannot be null");
    }

    try {
      // Generate random IV
      byte[] iv = new byte[GCM_IV_LENGTH];
      _secureRandom.nextBytes(iv);

      // Initialize cipher
      Cipher cipher = Cipher.getInstance(AES_GCM_ALGORITHM);
      GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
      cipher.init(Cipher.ENCRYPT_MODE, _derivedKey, gcmSpec);

      // Encrypt
      byte[] ciphertext = cipher.doFinal(value.getBytes(StandardCharsets.UTF_8));

      // Combine IV and ciphertext
      ByteBuffer buffer = ByteBuffer.allocate(iv.length + ciphertext.length);
      buffer.put(iv);
      buffer.put(ciphertext);

      // Return versioned and encoded result
      return VERSION_PREFIX + _encoder.encodeToString(buffer.array());
    } catch (Exception e) {
      throw new RuntimeException("Failed to encrypt value using AES-GCM!", e);
    }
  }

  /** Decrypts a value, supporting both new AES-GCM and legacy AES encryption. */
  public String decrypt(String encryptedValue) {
    if (encryptedValue == null) {
      throw new IllegalArgumentException("Encrypted value cannot be null");
    }

    // Check if this is new format
    if (!isLegacyFormat(encryptedValue)) {
      return decryptV2(encryptedValue.substring(VERSION_PREFIX.length()));
    } else {
      // Legacy format - use legacy decryption
      return decryptLegacy(encryptedValue);
    }
  }

  /** Decrypts using the new AES-GCM method. */
  private String decryptV2(String encryptedValue) {
    try {
      byte[] combined;
      try {
        combined = _decoder.decode(encryptedValue);
      } catch (IllegalArgumentException e) {
        throw new RuntimeException("Invalid Base64 encoding in encrypted value", e);
      }

      // Validate minimum length
      if (combined.length < GCM_IV_LENGTH + 16) { // IV + min ciphertext + tag
        throw new RuntimeException("Encrypted value too short for AES-GCM format");
      }

      // Extract IV and ciphertext
      ByteBuffer buffer = ByteBuffer.wrap(combined);

      byte[] iv = new byte[GCM_IV_LENGTH];
      buffer.get(iv);

      byte[] ciphertext = new byte[buffer.remaining()];
      buffer.get(ciphertext);

      // Initialize cipher
      Cipher cipher = Cipher.getInstance(AES_GCM_ALGORITHM);
      GCMParameterSpec gcmSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
      cipher.init(Cipher.DECRYPT_MODE, _derivedKey, gcmSpec);

      // Decrypt
      byte[] plaintext = cipher.doFinal(ciphertext);
      return new String(plaintext, StandardCharsets.UTF_8);
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt value using AES-GCM!", e);
    }
  }

  /** Legacy decryption method for backward compatibility. */
  private String decryptLegacy(String encryptedValue) {
    if (!v1AlgorithmEnabled) {
      throw new IllegalArgumentException(
          "Legacy v1 decryption is disabled. Set SECRET_SERVICE_V1_ALGORITHM_ENABLED=true to enable.");
    }

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
        throw new RuntimeException("Failed to create legacy key", e);
      }
      Cipher cipher = Cipher.getInstance("AES");
      cipher.init(Cipher.DECRYPT_MODE, secretKey);
      return new String(cipher.doFinal(_decoder.decode(encryptedValue)));
    } catch (Exception e) {
      throw new RuntimeException("Failed to decrypt value using legacy method!", e);
    }
  }

  /**
   * Derives a 256-bit key using HMAC-based key derivation. This is deterministic and doesn't
   * require storing a salt.
   */
  private SecretKeySpec deriveKey(String secret) {
    try {
      // Use HMAC-SHA256 for key derivation
      Mac mac = Mac.getInstance(HMAC_ALGORITHM);

      // First, create an initial key from the secret
      byte[] initialKey = _messageDigest.digest(secret.getBytes(StandardCharsets.UTF_8));
      SecretKeySpec macKey = new SecretKeySpec(initialKey, HMAC_ALGORITHM);
      mac.init(macKey);

      // Derive the final key using HMAC with a fixed info string
      byte[] derivedKey = mac.doFinal(KEY_DERIVATION_INFO.getBytes(StandardCharsets.UTF_8));

      // Use first 32 bytes (256 bits) for AES-256
      return new SecretKeySpec(derivedKey, 0, AES_KEY_SIZE / 8, AES_ALGORITHM);
    } catch (Exception e) {
      throw new RuntimeException("Failed to derive key!", e);
    }
  }

  /** Generates a URL-safe token. */
  public String generateUrlSafeToken(int length) {
    return _secureRandom
        .ints(length, LOWERCASE_ASCII_START, LOWERCASE_ASCII_END + 1)
        .mapToObj(i -> String.valueOf((char) i))
        .collect(Collectors.joining());
  }

  /** Hashes a string using SHA-256. */
  public String hashString(@Nonnull final String str) {
    byte[] hashedBytes = _messageDigest.digest(str.getBytes(StandardCharsets.UTF_8));
    return _encoder.encodeToString(hashedBytes);
  }

  /** Generates a cryptographically secure salt. */
  public byte[] generateSalt(int length) {
    byte[] randomBytes = new byte[length];
    _secureRandom.nextBytes(randomBytes);
    return randomBytes;
  }

  /**
   * Creates a hashed password with salt. Note: This still uses salt for password hashing, which is
   * different from encryption.
   */
  public String getHashedPassword(@Nonnull byte[] salt, @Nonnull String password)
      throws IOException {
    byte[] saltedPassword = saltPassword(salt, password);
    byte[] hashedPassword = _messageDigest.digest(saltedPassword);
    return _encoder.encodeToString(hashedPassword);
  }

  /** Combines salt and password bytes. */
  byte[] saltPassword(@Nonnull byte[] salt, @Nonnull String password) throws IOException {
    byte[] passwordBytes = password.getBytes(StandardCharsets.UTF_8);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    byteArrayOutputStream.write(salt);
    byteArrayOutputStream.write(passwordBytes);
    return byteArrayOutputStream.toByteArray();
  }

  /** Checks if an encrypted value uses the legacy format. */
  public boolean isLegacyFormat(String encryptedValue) {
    return !encryptedValue.startsWith(VERSION_PREFIX);
  }

  /**
   * Encrypts using legacy method - for testing/comparison only.
   *
   * @deprecated Use encrypt() instead
   */
  @Deprecated
  String encryptLegacy(String value) {
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
}
