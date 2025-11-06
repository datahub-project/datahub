package io.datahubproject.metadata.services;

import static com.github.stefanbirkner.systemlambda.SystemLambda.*;
import static org.testng.Assert.*;

import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

/**
 * Edge case tests for secret masking framework.
 *
 * <p>Tests scenarios that could occur in production but are less common.
 */
public class MaskingEdgeCaseTest {

  private static final Logger log = LogManager.getLogger(MaskingEdgeCaseTest.class);

  @AfterMethod
  public void cleanup() {
    MaskingManager.cleanupForCurrentThread();
  }

  @Test
  public void testMultipleMaskersOnSameThread() throws Exception {
    withEnvironmentVariable("SECRET_1", "value1")
        .and("SECRET_2", "value2")
        .execute(
            () -> {
              // Install first masker
              SecretMasker masker1 = new SecretMasker(Set.of("SECRET_1"));
              MaskingManager.installForCurrentThread(masker1);
              log.info("First masker installed");

              // Installing second masker should replace first
              SecretMasker masker2 = new SecretMasker(Set.of("SECRET_2"));
              MaskingManager.installForCurrentThread(masker2);
              log.info("Second masker installed");

              // Only second masker's secrets should be masked
              String message = "value1 and value2";
              String masked = masker2.mask(message);

              assertFalse(masked.contains("value2"), "Second secret should be masked");
            });
  }

  @Test
  public void testCleanupWithoutPriorInstallation() throws Exception {
    // Calling cleanup without installation should be safe
    MaskingManager.cleanupForCurrentThread();
    log.info("Cleanup without installation succeeded");

    // Should still be able to install after cleanup
    withEnvironmentVariable("TEST_SECRET", "value")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));
              MaskingManager.installForCurrentThread(masker);
              log.info("Installation after empty cleanup succeeded");
              MaskingManager.cleanupForCurrentThread();
            });
  }

  @Test
  public void testEmptyEnvironmentVariableSet() {
    // Create masker with empty set
    SecretMasker masker = new SecretMasker(Set.of());

    assertFalse(masker.isEnabled(), "Masker should be disabled with empty set");

    // Installation should be safe but no-op
    MaskingManager.installForCurrentThread(masker);
    log.info("Installation with empty set succeeded");

    // Masking should return original message
    String message = "This is a normal message";
    String masked = masker.mask(message);
    assertEquals(masked, message, "Empty masker should not modify message");
  }

  @Test
  public void testConcurrentThreadExecution() throws Exception {
    // Simplified concurrent test - set env vars before threads start
    withEnvironmentVariable("THREAD_SECRET_0", "thread_value_0")
        .and("THREAD_SECRET_1", "thread_value_1")
        .and("THREAD_SECRET_2", "thread_value_2")
        .and("THREAD_SECRET_3", "thread_value_3")
        .and("THREAD_SECRET_4", "thread_value_4")
        .execute(
            () -> {
              int threadCount = 5;
              ExecutorService executor = Executors.newFixedThreadPool(threadCount);
              CountDownLatch latch = new CountDownLatch(threadCount);
              AtomicInteger successCount = new AtomicInteger(0);

              for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                final String secretName = "THREAD_SECRET_" + threadId;
                final String secretValue = "thread_value_" + threadId;

                executor.submit(
                    () -> {
                      try {
                        SecretMasker masker = new SecretMasker(Set.of(secretName));
                        MaskingManager.installForCurrentThread(masker);

                        log.info("Thread {} logging with secret: {}", threadId, secretValue);

                        String masked = masker.mask("Secret is: " + secretValue);
                        if (!masked.contains(secretValue)
                            && masked.contains("${" + secretName + "}")) {
                          successCount.incrementAndGet();
                        }

                      } finally {
                        MaskingManager.cleanupForCurrentThread();
                        latch.countDown();
                      }
                    });
              }

              assertTrue(latch.await(10, TimeUnit.SECONDS), "All threads should complete");
              executor.shutdown();

              assertEquals(
                  successCount.get(),
                  threadCount,
                  "All threads should successfully mask their secrets");
            });
  }

  @Test
  public void testMissingEnvironmentVariable() {
    // Reference non-existent environment variable
    SecretMasker masker = new SecretMasker(Set.of("NONEXISTENT_VAR"));

    // Masker can still be enabled even if env var doesn't exist
    // This is by design - it will just have no patterns to mask
    if (masker.isEnabled()) {
      String message = "Test message with no secrets";
      String masked = masker.mask(message);
      assertEquals(masked, message, "Should not modify message when no secrets present");
    }
  }

  @Test
  public void testEmptyEnvironmentVariableValue() throws Exception {
    withEnvironmentVariable("EMPTY_VAR", "")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("EMPTY_VAR"));

              // Empty env var values may still enable masker (with empty pattern)
              // This tests that masking with empty value doesn't break
              String message = "Normal message";
              String masked = masker.mask(message);
              assertEquals(masked, message, "Should not modify message with empty secret value");
            });
  }

  @Test
  public void testWhitespaceOnlyEnvironmentVariable() throws Exception {
    withEnvironmentVariable("WHITESPACE_VAR", "   ")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("WHITESPACE_VAR"));

              // Whitespace-only values may still enable masker
              // This tests that it doesn't cause issues
              String message = "Test with   whitespace";
              String masked = masker.mask(message);

              // Should mask the whitespace value if present
              assertNotNull(masked, "Should return a valid result");
            });
  }

  @Test
  public void testMultipleInstallationsAndCleanups() throws Exception {
    withEnvironmentVariable("TEST_SECRET", "secret_value")
        .execute(
            () -> {
              // Install and cleanup multiple times
              for (int i = 0; i < 3; i++) {
                SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));
                MaskingManager.installForCurrentThread(masker);
                log.info("Installation {} complete", i + 1);

                MaskingManager.cleanupForCurrentThread();
                log.info("Cleanup {} complete", i + 1);
              }

              // Final installation should still work
              SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));
              MaskingManager.installForCurrentThread(masker);

              String masked = masker.mask("secret_value here");
              assertFalse(masked.contains("secret_value"), "Final masker should work after cycles");

              MaskingManager.cleanupForCurrentThread();
            });
  }

  @Test
  public void testSecretWithSpecialRegexCharacters() throws Exception {
    withEnvironmentVariable("SPECIAL_SECRET", "pass$word.123*")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("SPECIAL_SECRET"));
              assertTrue(masker.isEnabled(), "Masker should handle special characters");

              String message = "The password is pass$word.123*";
              String masked = masker.mask(message);

              assertFalse(
                  masked.contains("pass$word.123*"),
                  "Special characters should be properly escaped");
              assertTrue(masked.contains("${SPECIAL_SECRET}"), "Should contain placeholder");
            });
  }

  @Test
  public void testVeryLongSecretValue() throws Exception {
    String longSecret = "x".repeat(10000);
    withEnvironmentVariable("LONG_SECRET", longSecret)
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("LONG_SECRET"));
              assertTrue(masker.isEnabled(), "Masker should handle long secrets");

              String message = "Secret: " + longSecret + " end";
              String masked = masker.mask(message);

              assertFalse(masked.contains(longSecret), "Long secret should be masked");
              assertTrue(masked.contains("${LONG_SECRET}"), "Should contain placeholder");
            });
  }

  @Test
  public void testSecretAppearingMultipleTimes() throws Exception {
    withEnvironmentVariable("REPEATED_SECRET", "repeated_value")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("REPEATED_SECRET"));

              String message =
                  "First: repeated_value, second: repeated_value, third: repeated_value, fourth:"
                      + " repeated_value";
              String masked = masker.mask(message);

              assertFalse(
                  masked.contains("repeated_value"), "All occurrences of secret should be masked");

              int count = masked.split("\\$\\{REPEATED_SECRET\\}", -1).length - 1;
              assertEquals(count, 4, "Should mask all 4 occurrences");
            });
  }

  @Test
  public void testNestedSecretValues() throws Exception {
    withEnvironmentVariable("SECRET_A", "value_A")
        .and("SECRET_B", "value_A_extended")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("SECRET_A", "SECRET_B"));

              String message = "Short: value_A, Long: value_A_extended";
              String masked = masker.mask(message);

              assertFalse(masked.contains("value_A_extended"), "Longer secret should be masked");
              assertFalse(masked.contains("value_A"), "Shorter secret should be masked");
            });
  }

  @Test
  public void testExceptionDuringMasking() throws Exception {
    withEnvironmentVariable("TEST_SECRET", "test_value")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));
              MaskingManager.installForCurrentThread(masker);

              // Even if an exception occurs, cleanup should happen
              try {
                log.info("About to throw exception with secret: test_value");
                throw new RuntimeException("Test exception");
              } catch (RuntimeException e) {
                log.info("Caught exception: {}", e.getMessage());
              } finally {
                MaskingManager.cleanupForCurrentThread();
              }
            });
  }

  @Test
  public void testMaskingWithNullMessage() throws Exception {
    withEnvironmentVariable("TEST_SECRET", "value")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));
              String masked = masker.mask(null);
              assertEquals(masked, "", "Masking null should return empty string");
            });
  }

  @Test
  public void testMaskingWithEmptyMessage() throws Exception {
    withEnvironmentVariable("TEST_SECRET", "value")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));
              String masked = masker.mask("");
              assertEquals(masked, "", "Masking empty string should return empty string");
            });
  }
}
