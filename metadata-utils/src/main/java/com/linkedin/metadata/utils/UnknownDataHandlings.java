package com.linkedin.metadata.utils;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The {@link UnknownDataHandling} this process uses, chosen once at startup from {@code
 * featureFlags.unknownEntityAspectToleranceEnabled} and {@code
 * featureFlags.unknownEnumToleranceEnabled}.
 *
 * <p>Process-wide on purpose: the flags are a per-deployment setting that never varies by request,
 * and much of the code that needs it (validation, schema walks, batch items, registry helpers,
 * GraphQL mappers) has no {@code OperationContext}. Carrying it on the context would need a
 * separate carrier, each with its own default, at every such place.
 *
 * <p>Every process that runs the unknown-data paths must apply the flags at startup through {@code
 * SystemOperationContextFactory.configureUnknownDataHandling}: GMS, the MAE and MCE consumers and
 * system-update do it while building the system operation context, which installs the handling with
 * {@link #install}; installing different flags again fails startup. A process that writes through
 * {@code EntityService} without that context must call it itself. Until a process applies them it
 * is strict, the same as the flags' default (off), so a process that never applies them behaves as
 * before this tolerance existed. Tests of the tolerant paths choose it with {@link #use}.
 *
 * <p>Tests that switch it with {@link #use} must not run in parallel with other classes in the same
 * JVM, and must {@link #reset} afterwards.
 */
public final class UnknownDataHandlings {

  // Strict until configured: the same as the flags' default (off).
  private static volatile UnknownDataHandling current = StrictHandling.INSTANCE;
  // What the process applied from its flags, so a later install that disagrees fails startup.
  @Nullable private static UnknownDataHandling installed;

  private UnknownDataHandlings() {}

  @Nonnull
  public static UnknownDataHandling handling() {
    return current;
  }

  /**
   * Applies the process's flags, while the system operation context is built. A process can build
   * it more than once with the same flags; one that installs different ones fails, since every
   * caller shares this handling.
   *
   * @throws IllegalStateException when a handling tolerating different kinds is already installed
   */
  public static synchronized void install(@Nonnull final UnknownDataHandling handling) {
    if (installed != null
        && (installed.toleratesUnknownEntityAspect() != handling.toleratesUnknownEntityAspect()
            || installed.toleratesUnknownEnum() != handling.toleratesUnknownEnum())) {
      throw new IllegalStateException(
          String.format(
              "Unknown data handling is already installed with entity and aspect tolerance %s and"
                  + " enum tolerance %s; can't install entity and aspect tolerance %s and enum"
                  + " tolerance %s",
              installed.toleratesUnknownEntityAspect(),
              installed.toleratesUnknownEnum(),
              handling.toleratesUnknownEntityAspect(),
              handling.toleratesUnknownEnum()));
    }
    installed = handling;
    current = handling;
  }

  /** Tests of the tolerant paths: switches the handling without installing it. */
  @VisibleForTesting
  public static void use(@Nonnull final UnknownDataHandling handling) {
    current = handling;
  }

  @VisibleForTesting
  public static synchronized void reset() {
    installed = null;
    current = StrictHandling.INSTANCE;
  }
}
