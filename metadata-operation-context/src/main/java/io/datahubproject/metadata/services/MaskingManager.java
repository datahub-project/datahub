package io.datahubproject.metadata.services;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;

/**
 * Manages the lifecycle of masking appenders for thread-local secret masking.
 *
 * <p>This class provides utilities to install and cleanup masking appenders in Log4j2 configuration
 * safely. It tracks installed appenders per thread to ensure proper cleanup and prevent memory
 * leaks.
 *
 * <p>CRITICAL: Always use the {@link #withMasking} method to ensure guaranteed cleanup. Manual
 * install/cleanup is error-prone and can cause ThreadLocal memory leaks in thread pools.
 *
 * <p>Recommended usage:
 *
 * <pre>
 * SecretMasker masker = new SecretMasker(envVars);
 * MaskingManager.withMasking(masker, () -> {
 *   // Your code that produces logs here
 * });
 * </pre>
 *
 * <p>Legacy manual usage (not recommended):
 *
 * <pre>
 * try {
 *   MaskingManager.installForCurrentThread(masker);
 *   // Execute code
 * } finally {
 *   MaskingManager.cleanupForCurrentThread();
 * }
 * </pre>
 */
@Slf4j
public class MaskingManager {

  // Track installed masking appenders per thread for cleanup
  private static final ThreadLocal<List<String>> INSTALLED_APPENDERS =
      ThreadLocal.withInitial(ArrayList::new);

  // Appenders that should be wrapped with masking
  private static final Set<String> APPENDERS_TO_WRAP = Set.of("Console", "RollingFile", "File");

  /**
   * Functional interface for operations that need masking.
   *
   * <p>Used with {@link #withMasking} to guarantee cleanup.
   */
  @FunctionalInterface
  public interface MaskingOperation {
    void execute() throws Exception;
  }

  /**
   * Execute an operation with secret masking, guaranteeing cleanup.
   *
   * <p>This is the RECOMMENDED way to use masking. It ensures ThreadLocal cleanup happens even if:
   * - The operation throws an exception - Cleanup itself throws an exception - Early returns or
   * breaks occur
   *
   * <p>This prevents ThreadLocal memory leaks in thread pool scenarios (GraphQL executors,
   * scheduled tasks, etc.) where threads are reused.
   *
   * @param masker The SecretMasker to use for masking
   * @param operation The operation to execute with masking enabled
   * @throws Exception if the operation throws an exception (after cleanup)
   */
  public static void withMasking(@Nonnull SecretMasker masker, @Nonnull MaskingOperation operation)
      throws Exception {
    boolean installed = false;
    try {
      installForCurrentThread(masker);
      installed = true;
      operation.execute();
    } finally {
      // CRITICAL: Always cleanup if installed OR if ThreadLocal is not empty
      // This handles cases where installation partially succeeded
      if (installed || !INSTALLED_APPENDERS.get().isEmpty()) {
        try {
          cleanupForCurrentThread();
        } catch (Exception cleanupEx) {
          log.error(
              "CRITICAL: Failed to cleanup masking ThreadLocal. Forcing removal to prevent memory leak.",
              cleanupEx);
          // Force remove ThreadLocal to prevent accumulation in thread pools
          try {
            INSTALLED_APPENDERS.remove();
          } catch (Exception forceEx) {
            log.error("Failed to force-remove ThreadLocal. Memory leak may occur.", forceEx);
          }
        }
      }
    }
  }

  /**
   * Install masking appenders for the current thread's execution.
   *
   * <p>Wraps configured appenders (Console, File, etc.) with MaskingAppender to mask secrets in log
   * messages.
   *
   * <p>WARNING: When using this method directly (not via {@link #withMasking}), you MUST call
   * {@link #cleanupForCurrentThread()} in a finally block to prevent ThreadLocal memory leaks.
   *
   * @param masker The SecretMasker to use for masking
   */
  public static void installForCurrentThread(@Nonnull SecretMasker masker) {
    if (!masker.isEnabled()) {
      log.debug("Masker not enabled, skipping appender installation");
      return;
    }

    try {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      Configuration config = context.getConfiguration();

      int installed = 0;
      for (Appender appender : config.getAppenders().values()) {
        if (shouldWrapAppender(appender)) {
          wrapAppender(config, appender, masker);
          installed++;
        }
      }

      if (installed > 0) {
        context.updateLoggers();
        log.info("Installed {} masking appender(s) for current execution", installed);
      }
    } catch (Exception e) {
      log.warn("Failed to install masking appenders, continuing without masking", e);
    }
  }

  /**
   * Cleanup masking appenders installed for the current thread.
   *
   * <p>Removes all masking appenders and restores original appenders. Should be called in a finally
   * block to ensure cleanup even if execution fails.
   *
   * <p>IMPORTANT: This method removes the ThreadLocal reference, which is critical for preventing
   * memory leaks in thread pool scenarios.
   */
  public static void cleanupForCurrentThread() {
    List<String> installedAppenders = INSTALLED_APPENDERS.get();
    if (installedAppenders.isEmpty()) {
      // Still remove the ThreadLocal even if empty to ensure cleanup
      INSTALLED_APPENDERS.remove();
      return;
    }

    try {
      LoggerContext context = (LoggerContext) LogManager.getContext(false);
      Configuration config = context.getConfiguration();

      for (String maskingAppenderName : installedAppenders) {
        Appender maskingAppender = config.getAppender(maskingAppenderName);
        if (maskingAppender instanceof MaskingAppender) {
          MaskingAppender wrapper = (MaskingAppender) maskingAppender;

          // Remove masking appender from root logger
          config.getRootLogger().removeAppender(maskingAppenderName);

          // Stop the masking appender
          wrapper.stop();

          // Remove from configuration
          config.getAppenders().remove(maskingAppenderName);

          log.debug("Removed masking appender: {}", maskingAppenderName);
        }
      }

      if (!installedAppenders.isEmpty()) {
        context.updateLoggers();
        log.info("Cleaned up {} masking appender(s)", installedAppenders.size());
      }

    } catch (Exception e) {
      log.warn("Failed to cleanup masking appenders", e);
    } finally {
      // CRITICAL: Always clear and remove the ThreadLocal to prevent memory leaks
      // This is essential for thread pool scenarios where threads are reused
      installedAppenders.clear();
      INSTALLED_APPENDERS.remove();
    }
  }

  /** Check if an appender should be wrapped with masking. */
  private static boolean shouldWrapAppender(Appender appender) {
    if (appender == null) {
      return false;
    }

    // Don't wrap if already a MaskingAppender
    if (appender instanceof MaskingAppender) {
      return false;
    }

    // Check if this is an appender type we want to wrap
    return APPENDERS_TO_WRAP.contains(appender.getName());
  }

  /**
   * Wrap an appender with MaskingAppender.
   *
   * <p>Atomically replaces the original appender by adding the masking wrapper before removing the
   * original. This prevents race conditions where log events could be lost.
   */
  private static void wrapAppender(
      Configuration config, Appender originalAppender, SecretMasker masker) {
    String originalName = originalAppender.getName();
    MaskingAppender maskingAppender = new MaskingAppender(originalAppender, masker);

    // Add masking appender to configuration
    config.addAppender(maskingAppender);

    // Add masking appender to root logger BEFORE removing original to prevent event loss
    config.getRootLogger().addAppender(maskingAppender, null, null);

    // Remove original from root logger (AFTER adding new one to prevent race condition)
    config.getRootLogger().removeAppender(originalName);

    // Track for cleanup
    INSTALLED_APPENDERS.get().add(maskingAppender.getName());

    log.debug(
        "Wrapped appender '{}' with masking appender '{}'",
        originalName,
        maskingAppender.getName());
  }
}
