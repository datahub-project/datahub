package io.datahubproject.metadata.services;

import static org.testng.Assert.*;

import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.Test;

/**
 * Integration test demonstrating the full masking workflow as used in IngestionScheduler.
 *
 * <p>This simulates what happens when IngestionScheduler processes a recipe with environment
 * variables.
 */
public class MaskingIntegrationTest {

  private static final Logger log = LogManager.getLogger(MaskingIntegrationTest.class);

  @Test
  public void testFullMaskingWorkflow() {
    // Simulate a recipe with environment variables
    String recipe =
        """
            source:
              type: mysql
              config:
                host: localhost
                username: datahub
                password: ${DB_PASSWORD}
                api_key: ${API_KEY}
            """;

    // Step 1: Extract environment variable references (as IngestionScheduler does)
    Set<String> envVars = SecretMasker.extractEnvVarReferences(recipe);
    log.info("Extracted {} environment variables from recipe", envVars.size());

    assertEquals(envVars.size(), 2);
    assertTrue(envVars.contains("DB_PASSWORD"));
    assertTrue(envVars.contains("API_KEY"));

    // Step 2: Set test environment variables
    // In real scenario, these would already be set in the container
    System.setProperty("DB_PASSWORD", "my_secret_password_123");
    System.setProperty("API_KEY", "api_key_secret_456");

    try {
      // Step 3: Create masker (as IngestionScheduler does)
      SecretMasker masker = new SecretMasker(envVars);
      assertTrue(masker.isEnabled(), "Masker should be enabled with secrets");

      // Step 4: Install masking appenders (as IngestionScheduler does)
      MaskingManager.installForCurrentThread(masker);
      log.info("Masking appenders installed");

      // Step 5: Simulate logging that would happen during ingestion
      // These logs should have secrets masked
      log.info("Connecting to database with password: my_secret_password_123");
      log.info("Using API key: api_key_secret_456");
      log.info("Combined: password=my_secret_password_123, key=api_key_secret_456");

      // Step 6: Test masking directly
      String testMessage =
          "Connecting with credentials: password=my_secret_password_123, api_key=api_key_secret_456";
      String masked = masker.mask(testMessage);

      log.info("Original message: {}", testMessage);
      log.info("Masked message: {}", masked);

      // Verify masking worked
      assertFalse(masked.contains("my_secret_password_123"), "Secret password should be masked");
      assertFalse(masked.contains("api_key_secret_456"), "Secret API key should be masked");
      assertTrue(masked.contains("${DB_PASSWORD}"), "Should contain masked placeholder");
      assertTrue(masked.contains("${API_KEY}"), "Should contain masked placeholder");

      log.info("✓ Masking verification successful");

    } finally {
      // Step 7: Cleanup (as IngestionScheduler does in finally block)
      MaskingManager.cleanupForCurrentThread();
      log.info("Masking appenders cleaned up");

      // Cleanup test properties
      System.clearProperty("DB_PASSWORD");
      System.clearProperty("API_KEY");
    }
  }

  @Test
  public void testMaskingDisabledWhenNoSecrets() {
    // Recipe without any environment variables
    String recipe =
        """
            source:
              type: mysql
              config:
                host: localhost
                username: datahub
                password: hardcoded_password
            """;

    Set<String> envVars = SecretMasker.extractEnvVarReferences(recipe);
    assertEquals(envVars.size(), 0, "Should find no environment variables");

    SecretMasker masker = new SecretMasker(envVars);
    assertFalse(masker.isEnabled(), "Masker should be disabled with no secrets");

    // Installing masking manager should be safe even with disabled masker
    MaskingManager.installForCurrentThread(masker);

    log.info("Normal log message without secrets");

    MaskingManager.cleanupForCurrentThread();
  }

  @Test
  public void testMaskingWithMultipleSecrets() {
    System.setProperty("SECRET_1", "value12345");
    System.setProperty("SECRET_2", "value23456");
    System.setProperty("SECRET_3", "value34567");

    try {
      Set<String> envVars = Set.of("SECRET_1", "SECRET_2", "SECRET_3");
      SecretMasker masker = new SecretMasker(envVars);

      String message = "Secrets are: value12345, value23456, and value34567 here";
      String masked = masker.mask(message);

      assertFalse(masked.contains("value12345"));
      assertFalse(masked.contains("value23456"));
      assertFalse(masked.contains("value34567"));

      assertTrue(masked.contains("${SECRET_1}"));
      assertTrue(masked.contains("${SECRET_2}"));
      assertTrue(masked.contains("${SECRET_3}"));

    } finally {
      System.clearProperty("SECRET_1");
      System.clearProperty("SECRET_2");
      System.clearProperty("SECRET_3");
    }
  }

  @Test
  public void testCleanupPreventsMemoryLeak() {
    // Run multiple masking operations with cleanup
    for (int i = 0; i < 10; i++) {
      System.setProperty("TEST_SECRET_" + i, "secret_value_" + i);

      try {
        Set<String> envVars = Set.of("TEST_SECRET_" + i);
        SecretMasker masker = new SecretMasker(envVars);
        MaskingManager.installForCurrentThread(masker);

        log.info("Test iteration {} with secret", i);

      } finally {
        MaskingManager.cleanupForCurrentThread();
        System.clearProperty("TEST_SECRET_" + i);
      }
    }

    // If cleanup works properly, no memory leaks or appender buildup
    log.info("Completed 10 masking operations with cleanup");
  }
}
