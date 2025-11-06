package io.datahubproject.metadata.services;

import static org.testng.Assert.*;

import java.io.IOException;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Set;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.WriterAppender;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Unit tests for MaskingAppender exception masking functionality.
 *
 * <p>Tests verify that:
 *
 * <ul>
 *   <li>Exception messages are masked
 *   <li>Cause chains are recursively masked
 *   <li>Stack traces are preserved
 *   <li>Multiple secrets in same exception are masked
 *   <li>Edge cases are handled gracefully
 * </ul>
 */
public class MaskingAppenderTest {

  private SecretMasker masker;
  private StringWriter logOutput;
  private WriterAppender writerAppender;
  private MaskingAppender maskingAppender;
  private Logger logger;

  @BeforeMethod
  public void setup() {
    // Set up test secrets (3+ characters required for masking)
    System.setProperty("TEST_PASSWORD", "secret12345");
    System.setProperty("TEST_API_KEY", "key45678");

    // Create masker with test secrets
    masker = new SecretMasker(Set.of("TEST_PASSWORD", "TEST_API_KEY"));

    // Create string writer to capture output
    logOutput = new StringWriter();

    // Create writer appender with pattern that includes full exceptions
    writerAppender =
        WriterAppender.newBuilder()
            .setName("TestWriter")
            .setTarget(logOutput)
            .setLayout(PatternLayout.newBuilder().withPattern("%m%n%ex{full}").build())
            .build();
    writerAppender.start();

    // Wrap with masking appender
    maskingAppender = new MaskingAppender(writerAppender, masker);

    // Get test logger and attach masking appender
    logger = (Logger) LogManager.getLogger("test.masking.appender");
    logger.addAppender(maskingAppender);
    logger.setLevel(Level.INFO);
  }

  @AfterMethod
  public void cleanup() {
    if (logger != null && maskingAppender != null) {
      logger.removeAppender(maskingAppender);
    }
    if (maskingAppender != null) {
      maskingAppender.stop();
    }
    if (writerAppender != null) {
      writerAppender.stop();
    }
    System.clearProperty("TEST_PASSWORD");
    System.clearProperty("TEST_API_KEY");
  }

  @Test
  public void testMasksBasicExceptionMessage() {
    // Create exception with secret in message
    Exception exception = new SQLException("Failed to connect with password: secret12345");

    // Log the exception
    logger.error("Database error", exception);

    // Get the logged output
    String output = logOutput.toString();

    // Verify secret is masked
    assertFalse(
        output.contains("secret12345"),
        "Secret 'secret12345' should be masked in exception message");

    // Verify placeholder is present
    assertTrue(output.contains("${TEST_PASSWORD}"), "Should contain ${TEST_PASSWORD} placeholder");

    // Verify the exception type is still mentioned
    assertTrue(output.contains("SQLException"), "Exception type should be preserved");
  }

  @Test
  public void testMasksExceptionCauseChain() {
    // Create a 3-level exception chain
    Exception rootCause = new IOException("Connection failed: key45678");
    Exception middleCause = new SQLException("DB error: secret12345", rootCause);
    Exception topException = new RuntimeException("Top level", middleCause);

    // Log the top exception
    logger.error("Error occurred", topException);

    // Get output
    String output = logOutput.toString();

    // Verify all secrets are masked
    assertFalse(output.contains("secret12345"), "Middle cause secret should be masked");
    assertFalse(output.contains("key45678"), "Root cause secret should be masked");

    // Verify all placeholders are present
    assertTrue(output.contains("${TEST_PASSWORD}"), "Should have password placeholder");
    assertTrue(output.contains("${TEST_API_KEY}"), "Should have API key placeholder");

    // Verify exception chain structure is preserved
    assertTrue(output.contains("IOException"), "Root cause type preserved");
    assertTrue(output.contains("SQLException"), "Middle cause type preserved");
    assertTrue(output.contains("RuntimeException"), "Top exception type preserved");
  }

  @Test
  public void testPreservesStackTrace() {
    // Create exception with secret in message
    Exception exception = new RuntimeException("Error with secret12345");

    // Capture original stack trace
    StackTraceElement[] originalTrace = exception.getStackTrace();

    // Log the exception
    logger.error("Test error", exception);

    // Get output
    String output = logOutput.toString();

    // Verify stack trace elements are present
    int foundElements = 0;
    for (StackTraceElement element : originalTrace) {
      if (element.getFileName() != null && output.contains(element.getFileName())) {
        foundElements++;
      }
    }

    assertTrue(
        foundElements >= 3,
        "Should preserve at least 3 stack trace elements, found: " + foundElements);

    // Verify the secret is still masked
    assertFalse(output.contains("secret12345"), "Secret should be masked even with stack trace");
    assertTrue(
        output.contains("${TEST_PASSWORD}"), "Placeholder should be present in masked message");
  }

  @Test
  public void testExceptionWithoutSecrets() {
    // Create exception without secrets
    Exception exception = new RuntimeException("Normal error message");

    // Log it
    logger.error("Test error", exception);

    // Get output
    String output = logOutput.toString();

    // Verify original message is preserved
    assertTrue(output.contains("Normal error message"), "Original message should be preserved");

    // Verify no unnecessary placeholders
    assertFalse(
        output.contains("${TEST_PASSWORD}"), "Should not add placeholders when no secrets present");
    assertFalse(
        output.contains("${TEST_API_KEY}"), "Should not add placeholders when no secrets present");
  }

  @Test
  public void testMasksMultipleSecretsInOneException() {
    // Create exception with multiple secrets
    Exception exception =
        new RuntimeException("Failed with password: secret12345 and api_key: key45678");

    // Log it
    logger.error("Multiple secrets", exception);

    // Get output
    String output = logOutput.toString();

    // Verify both secrets are masked
    assertFalse(output.contains("secret12345"), "First secret should be masked");
    assertFalse(output.contains("key45678"), "Second secret should be masked");

    // Verify both placeholders are present
    assertTrue(output.contains("${TEST_PASSWORD}"), "Password placeholder should be present");
    assertTrue(output.contains("${TEST_API_KEY}"), "API key placeholder should be present");
  }

  @Test
  public void testCombinedMessageAndExceptionMasking() {
    // Create exception with secret
    Exception exception = new RuntimeException("Exception has secret12345");

    // Log with message that also has a secret
    logger.error("Log message has key45678", exception);

    // Get output
    String output = logOutput.toString();

    // Verify both secrets are masked
    assertFalse(output.contains("secret12345"), "Exception secret should be masked");
    assertFalse(output.contains("key45678"), "Log message secret should be masked");

    // Verify both placeholders are present
    assertTrue(output.contains("${TEST_PASSWORD}"), "Password placeholder should be present");
    assertTrue(output.contains("${TEST_API_KEY}"), "API key placeholder should be present");
  }

  @Test
  public void testExceptionWithNullMessage() {
    // Create exception with null message
    Exception exception = new RuntimeException((String) null);

    // Log it
    logger.error("Error occurred", exception);

    // Get output - should not throw NPE
    String output = logOutput.toString();

    // Verify test completes without exceptions
    assertNotNull(output, "Output should not be null");

    // Verify log message is present
    assertTrue(output.contains("Error occurred"), "Log message should be present");

    // Verify exception type is present
    assertTrue(output.contains("RuntimeException"), "Exception type should be present");
  }

  @Test
  public void testVeryDeepCauseChain() {
    // Create a 10-level deep cause chain
    Exception exception = new RuntimeException("Level 0: secret12345");
    for (int i = 1; i < 10; i++) {
      exception = new RuntimeException("Level " + i + ": key45678", exception);
    }

    // Log it - should not cause stack overflow
    logger.error("Deep chain", exception);

    // Get output
    String output = logOutput.toString();

    // Verify no crash occurred
    assertNotNull(output, "Should handle deep chains without crashing");

    // Verify secrets are masked
    assertFalse(output.contains("secret12345"), "Secrets in deep chain should be masked");
    assertFalse(output.contains("key45678"), "Secrets in deep chain should be masked");

    // Verify placeholders are present
    assertTrue(output.contains("${TEST_PASSWORD}"), "Password placeholder should be present");
    assertTrue(output.contains("${TEST_API_KEY}"), "API key placeholder should be present");

    // Verify multiple levels are present
    assertTrue(output.contains("Level 0"), "Level 0 should be present");
    assertTrue(output.contains("Level 5"), "Level 5 should be present");
    assertTrue(output.contains("Level 9"), "Level 9 should be present");
  }

  @Test
  public void testMaskingDisabledScenario() {
    // Create a separate masker with no secrets
    SecretMasker emptyMasker = new SecretMasker(Set.of());

    // Create separate output and appenders
    StringWriter disabledOutput = new StringWriter();
    WriterAppender disabledWriterAppender =
        WriterAppender.newBuilder()
            .setName("DisabledTestWriter")
            .setTarget(disabledOutput)
            .setLayout(PatternLayout.newBuilder().withPattern("%m%n%ex{full}").build())
            .build();
    disabledWriterAppender.start();

    MaskingAppender disabledMaskingAppender =
        new MaskingAppender(disabledWriterAppender, emptyMasker);

    Logger disabledLogger = (Logger) LogManager.getLogger("test.masking.disabled");
    disabledLogger.addAppender(disabledMaskingAppender);
    disabledLogger.setLevel(Level.INFO);

    try {
      // Create exception with secret
      Exception exception = new RuntimeException("password: secret12345");

      // Log it
      disabledLogger.error("Test", exception);

      // Get output
      String output = disabledOutput.toString();

      // Verify secret is NOT masked (appears as-is)
      assertTrue(
          output.contains("secret12345"), "Secret should NOT be masked when masker is disabled");

      // Verify no placeholders are added
      assertFalse(
          output.contains("${TEST_PASSWORD}"),
          "Should not add placeholders when masker is disabled");

    } finally {
      // Cleanup
      disabledLogger.removeAppender(disabledMaskingAppender);
      disabledMaskingAppender.stop();
      disabledWriterAppender.stop();
    }
  }

  @Test
  public void testDifferentExceptionTypes() {
    // Test with SQLException
    Exception sqlException = new SQLException("SQL error with password: secret12345");
    logger.error("SQL error", sqlException);

    // Test with IOException
    Exception ioException = new IOException("IO error with key: key45678");
    logger.error("IO error", ioException);

    // Test with RuntimeException
    Exception runtimeException = new RuntimeException("Runtime error with secret12345");
    logger.error("Runtime error", runtimeException);

    // Get output
    String output = logOutput.toString();

    // Verify all secrets are masked regardless of exception type
    assertFalse(
        output.contains("secret12345"),
        "Secrets should be masked in all exception types (found in: " + output + ")");
    assertFalse(
        output.contains("key45678"),
        "Secrets should be masked in all exception types (found in: " + output + ")");

    // Verify placeholders are present
    assertTrue(
        output.contains("${TEST_PASSWORD}"),
        "Password placeholder should be present for all types");
    assertTrue(
        output.contains("${TEST_API_KEY}"), "API key placeholder should be present for all types");

    // Verify all exception types are preserved
    assertTrue(output.contains("SQLException"), "SQLException type should be preserved");
    assertTrue(output.contains("IOException"), "IOException type should be preserved");
    assertTrue(output.contains("RuntimeException"), "RuntimeException type should be preserved");
  }

  @Test
  public void testExceptionWithCauseConstructorInitialized() {
    // Create exceptions using (String, Throwable) constructor
    Exception rootCause = new IOException("Root cause with key45678");
    Exception middleException = new SQLException("Middle with secret12345", rootCause);

    // Log it
    logger.error("Constructor initialized", middleException);

    // Get output
    String output = logOutput.toString();

    // Verify both secrets are masked
    assertFalse(
        output.contains("secret12345"),
        "Secret in exception with constructor-initialized cause should be masked");
    assertFalse(
        output.contains("key45678"), "Secret in constructor-initialized cause should be masked");

    // Verify placeholders are present
    assertTrue(
        output.contains("${TEST_PASSWORD}"),
        "Password placeholder should be present in constructor-initialized chain");
    assertTrue(
        output.contains("${TEST_API_KEY}"),
        "API key placeholder should be present in constructor-initialized chain");

    // Verify no exceptions were thrown during masking
    assertTrue(
        output.contains("SQLException"),
        "Should handle constructor-initialized cause without errors");
  }

  @Test
  public void testPartiallyMaskedCauseChain() {
    // Create 3-level chain where only level 0 and level 2 have secrets
    Exception rootCause = new IOException("Root with key45678"); // Has secret
    Exception middleCause = new SQLException("Middle has no secrets", rootCause); // No secret
    Exception topException =
        new RuntimeException("Top with secret12345", middleCause); // Has secret

    // Log it
    logger.error("Partial masking", topException);

    // Get output
    String output = logOutput.toString();

    // Verify secrets are masked
    assertFalse(output.contains("secret12345"), "Top level secret should be masked");
    assertFalse(output.contains("key45678"), "Root level secret should be masked");

    // Verify placeholders are present
    assertTrue(
        output.contains("${TEST_PASSWORD}"),
        "Password placeholder should be present for top level");
    assertTrue(
        output.contains("${TEST_API_KEY}"), "API key placeholder should be present for root level");

    // Verify middle level message is unchanged (no secrets)
    assertTrue(
        output.contains("Middle has no secrets"),
        "Middle level without secrets should be unchanged");

    // Verify all exception types are present
    assertTrue(output.contains("RuntimeException"), "Top exception type should be preserved");
    assertTrue(output.contains("SQLException"), "Middle exception type should be preserved");
    assertTrue(output.contains("IOException"), "Root exception type should be preserved");
  }

  @Test
  public void testSecretAtWordBoundaries() {
    // Test that secrets are masked even at word boundaries
    Exception exception1 = new RuntimeException("secret12345 at start");
    Exception exception2 = new RuntimeException("at end secret12345");
    Exception exception3 = new RuntimeException("in-between: secret12345!");

    logger.error("Boundary test 1", exception1);
    logger.error("Boundary test 2", exception2);
    logger.error("Boundary test 3", exception3);

    String output = logOutput.toString();

    // Verify secret is masked in all positions
    assertFalse(
        output.contains("secret12345"),
        "Secret should be masked at all word boundaries (found in: " + output + ")");

    // Count occurrences of placeholder (should be 3)
    int count = 0;
    int index = 0;
    while ((index = output.indexOf("${TEST_PASSWORD}", index)) != -1) {
      count++;
      index += "${TEST_PASSWORD}".length();
    }
    assertTrue(
        count >= 3, "Should have at least 3 occurrences of placeholder (found " + count + ")");
  }

  @Test
  public void testExceptionMessageWithSpecialCharacters() {
    // Test that masking works with special regex characters in context
    Exception exception =
        new RuntimeException("Error (secret12345) in [context] with $special chars");

    logger.error("Special chars test", exception);

    String output = logOutput.toString();

    // Verify secret is masked even with special characters around it
    assertFalse(
        output.contains("secret12345"),
        "Secret should be masked even with special characters nearby");

    // Verify placeholder is present
    assertTrue(
        output.contains("${TEST_PASSWORD}"),
        "Placeholder should be present even with special characters in message");

    // Verify special characters are preserved
    assertTrue(output.contains("("), "Special characters should be preserved");
    assertTrue(output.contains("[context]"), "Special characters should be preserved");
  }

  @Test
  public void testRepeatedSecretsInSameException() {
    // Test that the same secret appearing multiple times is all masked
    Exception exception =
        new RuntimeException("First secret12345 and again secret12345 and once more secret12345");

    logger.error("Repeated secrets", exception);

    String output = logOutput.toString();

    // Verify secret is completely absent
    assertFalse(output.contains("secret12345"), "All occurrences of secret should be masked");

    // Count occurrences of placeholder (should be 3)
    int count = 0;
    int index = 0;
    while ((index = output.indexOf("${TEST_PASSWORD}", index)) != -1) {
      count++;
      index += "${TEST_PASSWORD}".length();
    }
    assertTrue(
        count >= 3,
        "Should have at least 3 occurrences of placeholder for repeated secret (found "
            + count
            + ")");
  }
}
