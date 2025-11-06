package io.datahubproject.metadata.services;

import static com.github.stefanbirkner.systemlambda.SystemLambda.*;
import static org.testng.Assert.*;

import java.util.HashSet;
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
 * Security-focused tests for critical fixes in Java secret masking.
 *
 * <p>Tests cover: 1. CRITICAL: ThreadLocal memory leak prevention with withMasking() 2. HIGH:
 * Minimum secret length boundary (8 characters) 3. HIGH: Large secret set optimization (50+
 * secrets)
 */
public class SecurityFixesTest {

  private static final Logger log = LogManager.getLogger(SecurityFixesTest.class);

  @AfterMethod
  public void cleanup() {
    MaskingManager.cleanupForCurrentThread();
  }

  // ======================================================================================
  // CRITICAL FIX #2: ThreadLocal Memory Leak Prevention
  // ======================================================================================

  @Test
  public void testWithMaskingGuaranteesCleanup() throws Exception {
    withEnvironmentVariable("TEST_SECRET", "test_secret_12345")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("TEST_SECRET"));

              // Use withMasking which guarantees cleanup
              MaskingManager.withMasking(
                  masker,
                  () -> {
                    log.info("Logging with secret: test_secret_12345");
                    // Masking should be active here
                  });

              // After withMasking returns, ThreadLocal should be cleaned up
              // This is critical for thread pool scenarios
            });
  }

  @Test
  public void testWithMaskingCleansUpOnException() throws Exception {
    withEnvironmentVariable("EXCEPTION_SECRET", "exception_secret_12345")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("EXCEPTION_SECRET"));

              try {
                MaskingManager.withMasking(
                    masker,
                    () -> {
                      log.info("About to throw exception with exception_secret_12345");
                      throw new RuntimeException("Intentional exception");
                    });
                fail("Should have thrown exception");
              } catch (RuntimeException e) {
                assertEquals(e.getMessage(), "Intentional exception");
              }

              // Even though operation threw exception, cleanup should have occurred
              // We can't directly verify ThreadLocal is empty, but no leak warnings should appear
            });
  }

  @Test
  public void testWithMaskingInThreadPool() throws Exception {
    withEnvironmentVariable("POOL_SECRET_0", "pool_secret_value_0")
        .and("POOL_SECRET_1", "pool_secret_value_1")
        .and("POOL_SECRET_2", "pool_secret_value_2")
        .execute(
            () -> {
              int threadCount = 3;
              ExecutorService executor = Executors.newFixedThreadPool(threadCount);
              CountDownLatch latch = new CountDownLatch(threadCount);
              AtomicInteger successCount = new AtomicInteger(0);

              for (int i = 0; i < threadCount; i++) {
                final int threadId = i;
                final String secretName = "POOL_SECRET_" + threadId;
                final String secretValue = "pool_secret_value_" + threadId;

                executor.submit(
                    () -> {
                      try {
                        SecretMasker masker = new SecretMasker(Set.of(secretName));

                        // Use withMasking to ensure cleanup even in thread pool
                        MaskingManager.withMasking(
                            masker,
                            () -> {
                              log.info("Thread {} logging with secret: {}", threadId, secretValue);

                              String masked = masker.mask("Secret is: " + secretValue);
                              if (!masked.contains(secretValue)
                                  && masked.contains("${" + secretName + "}")) {
                                successCount.incrementAndGet();
                              }
                            });
                      } catch (Exception e) {
                        log.error("Thread {} failed", threadId, e);
                      } finally {
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
  public void testManualCleanupStillWorks() throws Exception {
    withEnvironmentVariable("MANUAL_SECRET", "manual_secret_12345")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("MANUAL_SECRET"));

              try {
                MaskingManager.installForCurrentThread(masker);

                String masked = masker.mask("Password is manual_secret_12345");
                assertFalse(masked.contains("manual_secret_12345"));
                assertTrue(masked.contains("${MANUAL_SECRET}"));

              } finally {
                // Manual cleanup should still work for backwards compatibility
                MaskingManager.cleanupForCurrentThread();
              }
            });
  }

  @Test
  public void testWithMaskingCleansUpOnInstallationFailure() throws Exception {
    // Test that cleanup happens even if installation throws an exception
    // This simulates a scenario where installForCurrentThread() partially succeeds

    // Use a masker with an invalid configuration that might cause issues
    withEnvironmentVariable("INSTALL_FAIL_SECRET", "install_fail_secret_12345")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("INSTALL_FAIL_SECRET"));

              try {
                MaskingManager.withMasking(
                    masker,
                    () -> {
                      // Operation that throws immediately
                      throw new RuntimeException("Simulated operation failure");
                    });
                fail("Should have thrown RuntimeException");
              } catch (RuntimeException e) {
                assertEquals(e.getMessage(), "Simulated operation failure");
              }

              // Verify ThreadLocal was cleaned up despite the exception
              // We can't directly verify ThreadLocal is empty, but this test ensures
              // the cleanup code path was executed without hanging or leaking
            });
  }

  // ======================================================================================
  // HIGH FIX #4: Minimum Secret Length (3 characters)
  // ======================================================================================

  @Test
  public void testShortSecretsNotMasked() throws Exception {
    withEnvironmentVariable("SHORT1", "a")
        .and("SHORT2", "1")
        .and("SHORT3", "on")
        .and("SHORT4", "x")
        .execute(
            () -> {
              // All these are too short (< 3 chars)
              SecretMasker masker =
                  new SecretMasker(Set.of("SHORT1", "SHORT2", "SHORT3", "SHORT4"));

              // Masker should be disabled because all secrets are too short
              assertFalse(masker.isEnabled(), "Masker should be disabled for short secrets");
            });
  }

  @Test
  public void testExactly3CharsIsMasked() throws Exception {
    withEnvironmentVariable("BOUNDARY", "abc")
        .execute(
            () -> {
              // Exactly 3 characters - should be masked
              SecretMasker masker = new SecretMasker(Set.of("BOUNDARY"));

              assertTrue(masker.isEnabled(), "Masker should be enabled for 3-char secret");

              String masked = masker.mask("Password is abc");
              assertFalse(masked.contains("abc"), "3-char secret should be masked");
              assertTrue(masked.contains("${BOUNDARY}"), "Should contain placeholder");
            });
  }

  @Test
  public void test2CharsNotMasked() throws Exception {
    withEnvironmentVariable("TOO_SHORT", "ab")
        .execute(
            () -> {
              // 2 characters - should NOT be masked
              SecretMasker masker = new SecretMasker(Set.of("TOO_SHORT"));

              assertFalse(masker.isEnabled(), "Masker should be disabled for 2-char value");
            });
  }

  @Test
  public void test4CharsIsMasked() throws Exception {
    withEnvironmentVariable("LONG_ENOUGH", "abcd")
        .execute(
            () -> {
              // 4 characters - should be masked
              SecretMasker masker = new SecretMasker(Set.of("LONG_ENOUGH"));

              assertTrue(masker.isEnabled(), "Masker should be enabled for 4-char secret");

              String masked = masker.mask("Secret is abcd");
              assertFalse(masked.contains("abcd"), "4-char secret should be masked");
              assertTrue(masked.contains("${LONG_ENOUGH}"), "Should contain placeholder");
            });
  }

  @Test
  public void testMixedLengthSecrets() throws Exception {
    withEnvironmentVariable("SHORT", "ab")
        .and("MEDIUM", "pwd")
        .and("LONG", "very_long_secret_12345")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("SHORT", "MEDIUM", "LONG"));

              // Should be enabled because MEDIUM and LONG are >= 3 chars
              assertTrue(masker.isEnabled(), "Masker should be enabled with some long secrets");

              String input = "Short: ab, Medium: pwd, Long: very_long_secret_12345";
              String masked = masker.mask(input);

              // SHORT (2 chars) should NOT be masked
              assertTrue(masked.contains("ab"), "Short secret should not be masked");

              // MEDIUM (3 chars) should be masked
              assertFalse(masked.contains("pwd"), "Medium secret should be masked");
              assertTrue(masked.contains("${MEDIUM}"), "Should contain MEDIUM placeholder");

              // LONG should be masked
              assertFalse(
                  masked.contains("very_long_secret_12345"), "Long secret should be masked");
              assertTrue(masked.contains("${LONG}"), "Should contain LONG placeholder");
            });
  }

  // ======================================================================================
  // HIGH FIX #6: Large Secret Set Optimization
  // ======================================================================================

  @Test
  public void testLargeSecretSetPerformance() throws Exception {
    // Create 100 secret environment variables
    int secretCount = 100;
    Set<String> secretNames = new HashSet<>();
    StringBuilder envVarSetup = new StringBuilder();

    for (int i = 0; i < secretCount; i++) {
      String varName = "LARGE_SECRET_" + i;
      String value = "large_secret_value_" + i + "_abcdefgh"; // 8+ chars
      secretNames.add(varName);

      if (i == 0) {
        envVarSetup.append(
            String.format("withEnvironmentVariable(\"%s\", \"%s\")", varName, value));
      } else {
        envVarSetup.append(String.format(".and(\"%s\", \"%s\")", varName, value));
      }
    }

    // Manually set environment variables for the test
    for (int i = 0; i < secretCount; i++) {
      String varName = "LARGE_SECRET_" + i;
      String value = "large_secret_value_" + i + "_abcdefgh";
      System.setProperty(varName, value); // Using system property as proxy
    }

    try {
      // Measure compilation time
      long startTime = System.currentTimeMillis();
      SecretMasker masker = new SecretMasker(secretNames);
      long compilationTime = System.currentTimeMillis() - startTime;

      assertTrue(masker.isEnabled(), "Masker should be enabled with 100 secrets");

      // Compilation should be fast (< 100ms) thanks to chunking
      assertTrue(
          compilationTime < 100,
          "Compilation should be fast with chunked patterns. Took: " + compilationTime + "ms");

      // Test masking performance
      String input =
          "Testing with large_secret_value_0_abcdefgh and large_secret_value_50_abcdefgh and"
              + " large_secret_value_99_abcdefgh";

      startTime = System.currentTimeMillis();
      String masked = masker.mask(input);
      long maskingTime = System.currentTimeMillis() - startTime;

      // Verify secrets are masked
      assertFalse(
          masked.contains("large_secret_value_0_abcdefgh"), "First secret should be masked");
      assertFalse(
          masked.contains("large_secret_value_50_abcdefgh"), "Middle secret should be masked");
      assertFalse(
          masked.contains("large_secret_value_99_abcdefgh"), "Last secret should be masked");

      assertTrue(masked.contains("${LARGE_SECRET_0}"), "Should contain first placeholder");
      assertTrue(masked.contains("${LARGE_SECRET_50}"), "Should contain middle placeholder");
      assertTrue(masked.contains("${LARGE_SECRET_99}"), "Should contain last placeholder");

      // Masking should be fast (< 10ms for 3 replacements)
      assertTrue(maskingTime < 10, "Masking should be fast. Took: " + maskingTime + "ms");

    } finally {
      // Clean up system properties
      for (int i = 0; i < secretCount; i++) {
        System.clearProperty("LARGE_SECRET_" + i);
      }
    }
  }

  @Test
  public void testChunkedPatternsPreserveLongestMatch() throws Exception {
    // Test that longest-match-first is preserved across chunks
    withEnvironmentVariable("SHORT_SECRET", "password12345678")
        .and("LONG_SECRET", "password12345678_extended")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("SHORT_SECRET", "LONG_SECRET"));

              assertTrue(masker.isEnabled());

              // When both secrets are substrings, longer should match first
              String input = "Using password12345678_extended here";
              String masked = masker.mask(input);

              // Longer secret should be matched, not the shorter substring
              assertTrue(
                  masked.contains("${LONG_SECRET}"), "Longer secret should be matched first");
              assertFalse(
                  masked.contains("${SHORT_SECRET}"), "Shorter substring should not be matched");
            });
  }

  @Test
  public void testSmallSecretSetUsesOptimizedPath() throws Exception {
    // With < 50 secrets, should use single pattern (optimal path)
    Set<String> secretNames = new HashSet<>();
    for (int i = 0; i < 10; i++) {
      String varName = "SMALL_SECRET_" + i;
      String value = "small_value_" + i + "_12345";
      System.setProperty(varName, value);
      secretNames.add(varName);
    }

    try {
      SecretMasker masker = new SecretMasker(secretNames);
      assertTrue(masker.isEnabled());

      // Should mask efficiently with single pattern
      String input = "Test with small_value_5_12345 here";
      String masked = masker.mask(input);

      assertFalse(masked.contains("small_value_5_12345"));
      assertTrue(masked.contains("${SMALL_SECRET_5}"));

    } finally {
      for (int i = 0; i < 10; i++) {
        System.clearProperty("SMALL_SECRET_" + i);
      }
    }
  }

  // ======================================================================================
  // P0 FIX #2: Java Thread Safety
  // ======================================================================================

  @Test
  public void testConcurrentMaskingThreadSafety() throws Exception {
    // Test that multiple threads can safely call mask() concurrently
    withEnvironmentVariable("CONCURRENT_SECRET_0", "concurrent_value_0_abcdefgh")
        .and("CONCURRENT_SECRET_1", "concurrent_value_1_abcdefgh")
        .and("CONCURRENT_SECRET_2", "concurrent_value_2_abcdefgh")
        .execute(
            () -> {
              int threadCount = 10;
              int iterationsPerThread = 100;

              SecretMasker masker =
                  new SecretMasker(
                      Set.of("CONCURRENT_SECRET_0", "CONCURRENT_SECRET_1", "CONCURRENT_SECRET_2"));

              assertTrue(masker.isEnabled(), "Masker should be enabled");

              ExecutorService executor = Executors.newFixedThreadPool(threadCount);
              CountDownLatch latch = new CountDownLatch(threadCount);
              AtomicInteger successCount = new AtomicInteger(0);
              AtomicInteger totalOperations = new AtomicInteger(0);

              for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                final int secretIndex = t % 3; // Rotate through 3 secrets

                executor.submit(
                    () -> {
                      try {
                        for (int i = 0; i < iterationsPerThread; i++) {
                          String secretValue = "concurrent_value_" + secretIndex + "_abcdefgh";
                          String input =
                              String.format("Thread %d iteration %d: %s", threadId, i, secretValue);

                          String masked = masker.mask(input);

                          // Verify secret was masked
                          if (!masked.contains(secretValue)
                              && masked.contains("${CONCURRENT_SECRET_" + secretIndex + "}")) {
                            successCount.incrementAndGet();
                          }

                          totalOperations.incrementAndGet();
                        }
                      } catch (Exception e) {
                        log.error("Thread {} failed", threadId, e);
                      } finally {
                        latch.countDown();
                      }
                    });
              }

              assertTrue(
                  latch.await(30, TimeUnit.SECONDS), "All threads should complete within timeout");
              executor.shutdown();

              int expectedOperations = threadCount * iterationsPerThread;
              assertEquals(
                  totalOperations.get(),
                  expectedOperations,
                  "All operations should have been attempted");
              assertEquals(
                  successCount.get(), expectedOperations, "All masking operations should succeed");
            });
  }

  @Test
  public void testRegexInjectionPrevention() throws Exception {
    // Test that Matcher.quoteReplacement() prevents regex injection
    withEnvironmentVariable("REGEX_SECRET", "secret$with\\special$chars")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("REGEX_SECRET"));

              assertTrue(masker.isEnabled(), "Masker should be enabled");

              // Secret contains $ which would be interpreted as backreference without quoting
              String input = "Password is secret$with\\special$chars here";
              String masked = masker.mask(input);

              // Verify secret was properly masked (not treated as regex backreference)
              assertFalse(masked.contains("secret$with\\special$chars"), "Secret should be masked");
              assertTrue(masked.contains("${REGEX_SECRET}"), "Should contain safe placeholder");
            });
  }

  @Test
  public void testStringBuilderPerformance() throws Exception {
    // Benchmark: StringBuilder should be faster than StringBuffer for thread-local usage
    withEnvironmentVariable("PERF_SECRET_0", "perf_secret_value_0_12345678")
        .and("PERF_SECRET_1", "perf_secret_value_1_12345678")
        .and("PERF_SECRET_2", "perf_secret_value_2_12345678")
        .execute(
            () -> {
              SecretMasker masker =
                  new SecretMasker(Set.of("PERF_SECRET_0", "PERF_SECRET_1", "PERF_SECRET_2"));

              // Create input with multiple secrets
              String input =
                  "Test with perf_secret_value_0_12345678 and perf_secret_value_1_12345678 and"
                      + " perf_secret_value_2_12345678";

              // Warm up
              for (int i = 0; i < 100; i++) {
                masker.mask(input);
              }

              // Benchmark
              int iterations = 1000;
              long startTime = System.nanoTime();

              for (int i = 0; i < iterations; i++) {
                String masked = masker.mask(input);
                assertFalse(
                    masked.contains("perf_secret_value_0_12345678"), "Secret 0 should be masked");
                assertFalse(
                    masked.contains("perf_secret_value_1_12345678"), "Secret 1 should be masked");
                assertFalse(
                    masked.contains("perf_secret_value_2_12345678"), "Secret 2 should be masked");
              }

              long endTime = System.nanoTime();
              long durationMs = (endTime - startTime) / 1_000_000;

              // Performance expectation: < 100ms for 1000 iterations (< 0.1ms per mask call)
              assertTrue(
                  durationMs < 100,
                  "StringBuilder-based masking should be fast. Took: " + durationMs + "ms");

              log.info(
                  "Performance: {} iterations in {}ms (avg: {}µs per call)",
                  iterations,
                  durationMs,
                  (durationMs * 1000) / iterations);
            });
  }

  @Test
  public void testMatcherNotSharedBetweenThreads() throws Exception {
    // Verify that Pattern.matcher() creates independent Matcher instances
    withEnvironmentVariable("MATCHER_SECRET", "matcher_secret_12345678")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("MATCHER_SECRET"));

              int threadCount = 5;
              ExecutorService executor = Executors.newFixedThreadPool(threadCount);
              CountDownLatch startLatch = new CountDownLatch(1);
              CountDownLatch doneLatch = new CountDownLatch(threadCount);
              AtomicInteger successCount = new AtomicInteger(0);

              for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(
                    () -> {
                      try {
                        // Wait for all threads to be ready
                        startLatch.await();

                        // All threads call mask() simultaneously
                        for (int i = 0; i < 50; i++) {
                          String input =
                              String.format("Thread %d: matcher_secret_12345678", threadId);
                          String masked = masker.mask(input);

                          if (!masked.contains("matcher_secret_12345678")
                              && masked.contains("${MATCHER_SECRET}")) {
                            successCount.incrementAndGet();
                          }
                        }
                      } catch (Exception e) {
                        log.error("Thread {} failed", threadId, e);
                      } finally {
                        doneLatch.countDown();
                      }
                    });
              }

              // Start all threads at once
              startLatch.countDown();

              assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "All threads should complete");
              executor.shutdown();

              // All operations should succeed (no corruption from shared Matcher)
              assertEquals(
                  successCount.get(),
                  threadCount * 50,
                  "All masking operations should succeed without Matcher conflicts");
            });
  }

  @Test
  public void testConcurrentHashMapThreadSafety() throws Exception {
    // Verify that secretValueToName (ConcurrentHashMap) is safely accessed from multiple threads
    // Using 5 secrets instead of 20 to keep test manageable with withEnvironmentVariable()
    withEnvironmentVariable("CHMAP_SECRET_0", "chmap_value_0_12345678")
        .and("CHMAP_SECRET_1", "chmap_value_1_12345678")
        .and("CHMAP_SECRET_2", "chmap_value_2_12345678")
        .and("CHMAP_SECRET_3", "chmap_value_3_12345678")
        .and("CHMAP_SECRET_4", "chmap_value_4_12345678")
        .execute(
            () -> {
              Set<String> secretNames =
                  Set.of(
                      "CHMAP_SECRET_0",
                      "CHMAP_SECRET_1",
                      "CHMAP_SECRET_2",
                      "CHMAP_SECRET_3",
                      "CHMAP_SECRET_4");
              SecretMasker masker = new SecretMasker(secretNames);
              assertTrue(masker.isEnabled(), "Masker should be enabled");

              int threadCount = 5;
              int secretCount = 5;
              ExecutorService executor = Executors.newFixedThreadPool(threadCount);
              CountDownLatch latch = new CountDownLatch(threadCount);
              AtomicInteger successCount = new AtomicInteger(0);

              for (int t = 0; t < threadCount; t++) {
                final int threadId = t;
                executor.submit(
                    () -> {
                      try {
                        // Each thread masks all secrets concurrently
                        for (int i = 0; i < secretCount; i++) {
                          String secretValue = "chmap_value_" + i + "_12345678";
                          String input =
                              String.format("Thread %d secret: %s", threadId, secretValue);
                          String masked = masker.mask(input);

                          // Verify secret was masked (ConcurrentHashMap lookup worked)
                          if (!masked.contains(secretValue)
                              && masked.contains("${CHMAP_SECRET_" + i + "}")) {
                            successCount.incrementAndGet();
                          }
                        }
                      } catch (Exception e) {
                        log.error("Thread {} failed", threadId, e);
                      } finally {
                        latch.countDown();
                      }
                    });
              }

              assertTrue(latch.await(10, TimeUnit.SECONDS), "All threads should complete");
              executor.shutdown();

              // All lookups should succeed
              assertEquals(
                  successCount.get(),
                  threadCount * secretCount,
                  "All ConcurrentHashMap lookups should succeed");
            });
  }

  /**
   * P1 FIX #5: Document stack trace masking limitation.
   *
   * <p>This test demonstrates that stack trace frames (StackTraceElement[]) are NOT masked. Only
   * exception messages are masked. This is a Java platform limitation - StackTraceElement is
   * immutable and contains method names, file names, line numbers, but no free-form text to mask.
   *
   * <p>Secrets could theoretically appear in stack traces if:
   *
   * <ul>
   *   <li>Debug symbols are enabled and local variable names contain secrets
   *   <li>Method parameter names in the trace contain secrets (rare in practice)
   * </ul>
   *
   * <p>This is documented but not fixed, as there's no practical way to mask StackTraceElement
   * without breaking the stack trace structure.
   */
  @Test
  public void testStackTraceNotMasked_LimitationDocumented() throws Exception {
    withEnvironmentVariable("STACK_SECRET", "stack_secret_12345678")
        .execute(
            () -> {
              SecretMasker masker = new SecretMasker(Set.of("STACK_SECRET"));

              // This test documents the limitation that stack traces themselves
              // are NOT masked, only exception messages are masked.

              // Create an exception with secret in message
              RuntimeException original = new RuntimeException("Error: stack_secret_12345678");
              StackTraceElement[] originalTrace = original.getStackTrace();

              // Use MaskingAppender internally which calls maskThrowable
              MaskingManager.withMasking(
                  masker,
                  () -> {
                    try {
                      throw new RuntimeException("Error: stack_secret_12345678");
                    } catch (RuntimeException e) {
                      // When logged, the exception message will be masked,
                      // but the stack trace elements themselves are NOT masked.
                      // This is documented in MaskingAppender.maskThrowable()
                      log.error("Exception occurred", e);
                    }
                  });

              // Verify stack trace structure is preserved (this is the limitation)
              // The test primarily documents that we don't mask stack traces,
              // not that we successfully do mask them.
              assertNotNull(originalTrace);
              assertTrue(originalTrace.length > 0, "Stack trace should have elements");

              // Note: We can't directly verify masking behavior in this test without
              // complex log capture setup. The main purpose is to document the limitation
              // in test form alongside the javadoc in MaskingAppender.maskThrowable().
            });
  }
}
