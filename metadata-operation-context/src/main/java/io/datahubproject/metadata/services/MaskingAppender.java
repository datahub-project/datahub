package io.datahubproject.metadata.services;

import java.lang.reflect.Constructor;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.impl.Log4jLogEvent;
import org.apache.logging.log4j.message.SimpleMessage;

/**
 * Appender wrapper that masks secrets before delegating to wrapped appender.
 *
 * <p>Wraps any existing appender and applies secret masking to all log messages before passing them
 * through.
 *
 * <p>Usage: MaskingAppender maskingAppender = new MaskingAppender(consoleAppender, secretMasker);
 */
@Slf4j
public class MaskingAppender extends AbstractAppender {

  private final Appender wrappedAppender;
  private final SecretMasker masker;

  /**
   * Create a masking appender that wraps another appender.
   *
   * @param wrappedAppender The appender to wrap (e.g., ConsoleAppender, FileAppender)
   * @param masker The secret masker to use for masking
   */
  public MaskingAppender(@Nonnull Appender wrappedAppender, @Nonnull SecretMasker masker) {
    super(
        "Masking-" + wrappedAppender.getName(),
        null,
        wrappedAppender.getLayout(),
        true,
        Property.EMPTY_ARRAY);
    this.wrappedAppender = wrappedAppender;
    this.masker = masker;
    start();
  }

  @Override
  public void append(LogEvent event) {
    if (masker == null || !masker.isEnabled()) {
      // No masking needed, pass through
      wrappedAppender.append(event);
      return;
    }

    // Get original message
    String originalMessage = event.getMessage().getFormattedMessage();
    if (originalMessage == null || originalMessage.isEmpty()) {
      wrappedAppender.append(event);
      return;
    }

    // Mask the message
    String maskedMessage = masker.mask(originalMessage);

    // Mask the exception if present
    Throwable maskedThrowable = event.getThrown();
    if (maskedThrowable != null) {
      maskedThrowable = maskThrowable(maskedThrowable, masker);
    }

    if (maskedMessage.equals(originalMessage) && maskedThrowable == event.getThrown()) {
      // No masking occurred, pass through original event
      wrappedAppender.append(event);
      return;
    }

    // Create new event with masked message and exception
    LogEvent maskedEvent =
        Log4jLogEvent.newBuilder()
            .setLoggerName(event.getLoggerName())
            .setMarker(event.getMarker())
            .setLoggerFqcn(event.getLoggerFqcn())
            .setLevel(event.getLevel())
            .setMessage(new SimpleMessage(maskedMessage))
            .setThrown(maskedThrowable)
            .setContextStack(event.getContextStack())
            .setThreadName(event.getThreadName())
            .setThreadId(event.getThreadId())
            .setThreadPriority(event.getThreadPriority())
            .setSource(event.getSource())
            .setTimeMillis(event.getTimeMillis())
            .build();

    wrappedAppender.append(maskedEvent);
  }

  @Override
  public void stop() {
    super.stop();
    // Don't stop the wrapped appender - it may be used by other loggers
  }

  /**
   * Recursively masks exception messages and their cause chains.
   *
   * <p>Creates new exception instances with masked messages while preserving stack traces and cause
   * chains. Uses reflection to construct new exception instances.
   *
   * <p><b>LIMITATION:</b> Stack trace frames themselves are NOT masked. The stack trace preserves
   * the original StackTraceElement[] which may contain secrets in method parameter values or local
   * variable names (if debug symbols are enabled). Masking only applies to exception messages. This
   * is a Java platform limitation - StackTraceElement is immutable and contains no text content to
   * mask.
   *
   * <p>To minimize risk:
   *
   * <ul>
   *   <li>Don't pass secrets as method parameters in production code
   *   <li>Use proper secret management patterns (inject from config, not inline)
   *   <li>Consider disabling debug symbols in production JARs
   * </ul>
   *
   * @param thrown The original exception
   * @param masker The secret masker to use
   * @return A new exception with masked message and cause chain, or the original if masking fails
   */
  @Nullable
  private Throwable maskThrowable(@Nullable Throwable thrown, @Nonnull SecretMasker masker) {
    if (thrown == null) {
      return null;
    }

    try {
      // Mask the message
      String originalMessage = thrown.getMessage();
      String maskedMessage = originalMessage != null ? masker.mask(originalMessage) : null;

      // Recursively mask the cause
      Throwable originalCause = thrown.getCause();
      Throwable maskedCause = originalCause != null ? maskThrowable(originalCause, masker) : null;

      // Check if masking occurred
      boolean messageChanged = !equals(originalMessage, maskedMessage);
      boolean causeChanged = maskedCause != originalCause;

      if (!messageChanged && !causeChanged) {
        // No masking needed, return original
        return thrown;
      }

      // Create new exception instance with masked message
      Throwable maskedThrowable = createMaskedException(thrown, maskedMessage);

      if (maskedThrowable == null) {
        // Failed to create new instance, return original
        log.debug(
            "Failed to create masked exception for type {}, returning original",
            thrown.getClass().getName());
        return thrown;
      }

      // Preserve stack trace
      maskedThrowable.setStackTrace(thrown.getStackTrace());

      // Set masked cause
      if (maskedCause != null) {
        try {
          maskedThrowable.initCause(maskedCause);
        } catch (IllegalStateException e) {
          // Cause already initialized by constructor
          log.debug("Could not set cause on exception, already initialized", e);
        }
      }

      return maskedThrowable;

    } catch (Exception e) {
      // If anything goes wrong, return original exception
      log.debug("Error masking exception, returning original", e);
      return thrown;
    }
  }

  /**
   * Creates a new exception instance with a masked message using reflection.
   *
   * @param original The original exception
   * @param maskedMessage The masked message
   * @return A new exception instance, or null if creation fails
   */
  @Nullable
  private Throwable createMaskedException(
      @Nonnull Throwable original, @Nullable String maskedMessage) {
    Class<? extends Throwable> exceptionClass = original.getClass();

    try {
      // Try constructor with String message
      Constructor<? extends Throwable> constructor = exceptionClass.getConstructor(String.class);
      return constructor.newInstance(maskedMessage);
    } catch (NoSuchMethodException e) {
      // No String constructor, try no-arg constructor
      try {
        Constructor<? extends Throwable> constructor = exceptionClass.getConstructor();
        return constructor.newInstance();
      } catch (Exception ex) {
        log.debug(
            "Could not create masked exception for type {}: {}",
            exceptionClass.getName(),
            ex.getMessage());
        return null;
      }
    } catch (Exception e) {
      log.debug(
          "Could not create masked exception for type {}: {}",
          exceptionClass.getName(),
          e.getMessage());
      return null;
    }
  }

  /**
   * Null-safe equals check.
   *
   * @param a First object
   * @param b Second object
   * @return true if both are null or equal
   */
  private boolean equals(@Nullable Object a, @Nullable Object b) {
    if (a == null && b == null) {
      return true;
    }
    if (a == null || b == null) {
      return false;
    }
    return a.equals(b);
  }
}
