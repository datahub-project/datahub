package io.datahubproject.metadata.services;

import static org.testng.Assert.*;

import java.io.IOException;
import java.util.Base64;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

public class SecretServiceTest {

  private static final String TEST_SECRET = "test-secret-key-12345";
  private static final String ALTERNATIVE_SECRET = "different-secret-key-67890";
  private SecretService secretService;

  @BeforeMethod
  public void setUp() {
    secretService = new SecretService(TEST_SECRET, true);
  }

  @Test
  public void testEncryptDecrypt() {
    // Given
    String plaintext = "This is a test credential";

    // When
    String encrypted = secretService.encrypt(plaintext);
    String decrypted = secretService.decrypt(encrypted);

    // Then
    assertNotNull(encrypted);
    assertTrue(encrypted.startsWith("v2:"));
    assertEquals(decrypted, plaintext);
  }

  @Test
  public void testEncryptionProducesDifferentCiphertexts() {
    // Given
    String plaintext = "Same plaintext";

    // When
    String encrypted1 = secretService.encrypt(plaintext);
    String encrypted2 = secretService.encrypt(plaintext);

    // Then
    assertNotEquals(
        encrypted1, encrypted2, "Encryption should produce different ciphertexts due to random IV");
    assertEquals(secretService.decrypt(encrypted1), plaintext);
    assertEquals(secretService.decrypt(encrypted2), plaintext);
  }

  @Test
  public void testLegacyDecryption() {
    // Given - a value encrypted with the legacy method
    String plaintext = "Legacy credential";
    String legacyEncrypted = secretService.encryptLegacy(plaintext);

    // When
    String decrypted = secretService.decrypt(legacyEncrypted);

    // Then
    assertFalse(legacyEncrypted.startsWith("v2:"));
    assertEquals(decrypted, plaintext);
  }

  @Test
  public void testIsLegacyFormat() {
    // Given
    String v2Encrypted = secretService.encrypt("test");
    String legacyEncrypted = secretService.encryptLegacy("test");

    // Then
    assertFalse(secretService.isLegacyFormat(v2Encrypted));
    assertTrue(secretService.isLegacyFormat(legacyEncrypted));
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to decrypt.*")
  public void testDecryptWithWrongSecret() {
    // Given
    String encrypted = secretService.encrypt("test credential");
    SecretService wrongSecretService = new SecretService(ALTERNATIVE_SECRET, true);

    // When/Then - should throw exception
    wrongSecretService.decrypt(encrypted);
  }

  @Test(expectedExceptions = RuntimeException.class)
  public void testDecryptInvalidData() {
    // Given
    String invalidEncrypted = "v2:invalid-base64-data!!!";

    // When/Then - should throw exception
    secretService.decrypt(invalidEncrypted);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testEncryptNull() {
    secretService.encrypt(null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testDecryptNull() {
    secretService.decrypt(null);
  }

  @DataProvider(name = "credentialTestData")
  public Object[][] credentialTestData() {
    return new Object[][] {
      {"simple-password"},
      {"password-with-special-chars!@#$%^&*()"},
      {"very long credential " + "x".repeat(1000)},
      {"unicode-密码-パスワード"},
      {""}, // empty string
      {"credential\nwith\nnewlines"},
      {"credential\twith\ttabs"},
      {"{\"user\":\"admin\",\"password\":\"secret123\"}"} // JSON credential
    };
  }

  @Test(dataProvider = "credentialTestData")
  public void testEncryptDecryptVariousCredentials(String credential) {
    // When
    String encrypted = secretService.encrypt(credential);
    String decrypted = secretService.decrypt(encrypted);

    // Then
    assertEquals(decrypted, credential);
  }

  @Test
  public void testGenerateUrlSafeToken() {
    // Given
    int length = 32;

    // When
    String token1 = secretService.generateUrlSafeToken(length);
    String token2 = secretService.generateUrlSafeToken(length);

    // Then
    assertEquals(token1.length(), length);
    assertEquals(token2.length(), length);
    assertNotEquals(token1, token2);
    assertTrue(token1.matches("[a-z]+"));
    assertTrue(token2.matches("[a-z]+"));
  }

  @Test
  public void testHashString() {
    // Given
    String input = "test string to hash";

    // When
    String hash1 = secretService.hashString(input);
    String hash2 = secretService.hashString(input);

    // Then
    assertNotNull(hash1);
    assertEquals(hash1, hash2, "Hashing should be deterministic");

    // Verify it's base64 encoded
    try {
      Base64.getDecoder().decode(hash1);
    } catch (IllegalArgumentException e) {
      fail("Hash should be valid base64: " + e.getMessage());
    }

    // Verify different inputs produce different hashes
    String differentHash = secretService.hashString("different input");
    assertNotEquals(hash1, differentHash);
  }

  @Test
  public void testGenerateSalt() {
    // Given
    int length = 16;

    // When
    byte[] salt1 = secretService.generateSalt(length);
    byte[] salt2 = secretService.generateSalt(length);

    // Then
    assertEquals(salt1.length, length);
    assertEquals(salt2.length, length);
    assertNotEquals(salt1, salt2);
  }

  @Test
  public void testGetHashedPassword() throws IOException {
    // Given
    byte[] salt = secretService.generateSalt(16);
    String password = "myPassword123";

    // When
    String hashedPassword1 = secretService.getHashedPassword(salt, password);
    String hashedPassword2 = secretService.getHashedPassword(salt, password);

    // Then
    assertEquals(
        hashedPassword1, hashedPassword2, "Same salt and password should produce same hash");

    // Different salt should produce different hash
    byte[] differentSalt = secretService.generateSalt(16);
    String hashedPassword3 = secretService.getHashedPassword(differentSalt, password);
    assertNotEquals(hashedPassword1, hashedPassword3);
  }

  @Test
  public void testBackwardCompatibilityRoundTrip() {
    // Test that we can decrypt legacy encrypted values and re-encrypt them
    String[] testCredentials = {"database-password-123", "api-key-xyz789", "oauth-secret-abc456"};

    for (String credential : testCredentials) {
      // Encrypt with legacy
      String legacyEncrypted = secretService.encryptLegacy(credential);

      // Decrypt
      String decrypted = secretService.decrypt(legacyEncrypted);
      assertEquals(decrypted, credential);

      // Re-encrypt with new method
      String newEncrypted = secretService.encrypt(credential);
      assertTrue(newEncrypted.startsWith("v2:"));

      // Decrypt new format
      String decryptedNew = secretService.decrypt(newEncrypted);
      assertEquals(decryptedNew, credential);
    }
  }

  @Test
  public void testEncryptedDataStructure() {
    // Given
    String plaintext = "test";

    // When
    String encrypted = secretService.encrypt(plaintext);

    // Then
    assertTrue(encrypted.startsWith("v2:"));
    String base64Part = encrypted.substring(3);

    // Verify it's valid base64
    byte[] decoded = Base64.getDecoder().decode(base64Part);

    // Verify minimum length (IV + at least some ciphertext + tag)
    assertTrue(
        decoded.length >= 12 + 1 + 16, "Encrypted data should contain IV, ciphertext, and tag");
  }

  @Test
  public void testDeterministicKeyDerivation() {
    // Given
    SecretService service1 = new SecretService(TEST_SECRET, true);
    SecretService service2 = new SecretService(TEST_SECRET, true);
    String plaintext = "test credential";

    // When
    String encrypted = service1.encrypt(plaintext);
    String decrypted = service2.decrypt(encrypted);

    // Then
    assertEquals(decrypted, plaintext, "Different instances with same secret should be compatible");
  }

  @Test
  public void testLargeCredential() {
    // Given - a large credential (e.g., a certificate or long token)
    StringBuilder largeCredential = new StringBuilder();
    for (int i = 0; i < 10000; i++) {
      largeCredential.append("This is line ").append(i).append(" of a large credential.\n");
    }
    String plaintext = largeCredential.toString();

    // When
    String encrypted = secretService.encrypt(plaintext);
    String decrypted = secretService.decrypt(encrypted);

    // Then
    assertEquals(decrypted, plaintext);
  }

  @Test
  public void testEmptyStringEncryption() {
    // Given
    String empty = "";

    // When
    String encrypted = secretService.encrypt(empty);
    String decrypted = secretService.decrypt(encrypted);

    // Then
    assertTrue(encrypted.startsWith("v2:"));
    assertEquals(decrypted, empty);
  }

  @Test(
      expectedExceptions = RuntimeException.class,
      expectedExceptionsMessageRegExp = ".*Failed to decrypt.*")
  public void testCorruptedEncryptedData() {
    // Given
    String encrypted = secretService.encrypt("test");
    // Corrupt the encrypted data by changing some characters
    String corrupted = encrypted.substring(0, 10) + "XXX" + encrypted.substring(13);

    // When/Then - should throw exception
    secretService.decrypt(corrupted);
  }

  @Test(
      expectedExceptions = IllegalArgumentException.class,
      expectedExceptionsMessageRegExp = ".*Legacy v1 decryption is disabled.*")
  public void testDecryptLegacyWhenV1Disabled() {
    // Given - a SecretService with v1 algorithm disabled
    SecretService v1DisabledService = new SecretService(TEST_SECRET, false);

    // Create a legacy encrypted value using a service with v1 enabled
    String plaintext = "test credential";
    String legacyEncrypted = secretService.encryptLegacy(plaintext);

    // When/Then - attempting to decrypt legacy format should throw exception
    v1DisabledService.decrypt(legacyEncrypted);
  }

  @Test
  public void testV2EncryptionWorksWhenV1Disabled() {
    // Given - a SecretService with v1 algorithm disabled
    SecretService v1DisabledService = new SecretService(TEST_SECRET, false);
    String plaintext = "test credential";

    // When - using v2 encryption/decryption
    String encrypted = v1DisabledService.encrypt(plaintext);
    String decrypted = v1DisabledService.decrypt(encrypted);

    // Then - v2 should work fine
    assertTrue(encrypted.startsWith("v2:"));
    assertEquals(decrypted, plaintext);
  }
}
