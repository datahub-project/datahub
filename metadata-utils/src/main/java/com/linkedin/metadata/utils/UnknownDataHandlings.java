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
 * before this tolerance existed.
 *
 * <p>Tests of the tolerant paths don't change the installed handling: they set a test override that
 * {@link #handling} returns instead, through {@code UnknownDataHandlingsForTests} in this module's
 * test fixtures, which aren't on any production classpath. The override is one value per JVM, so
 * those fixtures refuse it in a TestNG test that runs classes alongside, and their {@code
 * UnknownDataHandlingIsolationListener} removes it after every test class, so a class that forgets
 * to can't leave it for the next one.
 */
public final class UnknownDataHandlings {

  // What the process applied from its flags; strict until then, the same as the flags' default.
  @Nullable private static volatile UnknownDataHandling installed;
  // Set only by tests, in place of the installed handling.
  @Nullable private static volatile UnknownDataHandling testOverride;

  private UnknownDataHandlings() {}

  @Nonnull
  public static UnknownDataHandling handling() {
    final UnknownDataHandling override = testOverride;
    if (override != null) {
      return override;
    }
    final UnknownDataHandling applied = installed;
    return applied == null ? StrictHandling.INSTANCE : applied;
  }

  /**
   * Applies the process's flags, while the system operation context is built. The first install
   * stays for the life of the process: building the context again with the same flags keeps it, and
   * installing different flags fails, since every caller shares this handling.
   *
   * @throws IllegalStateException when a handling tolerating different kinds is already installed
   */
  public static synchronized void install(@Nonnull final UnknownDataHandling handling) {
    if (installed == null) {
      installed = handling;
      return;
    }
    if (installed.toleratesUnknownEntityAspect() != handling.toleratesUnknownEntityAspect()
        || installed.toleratesUnknownEnum() != handling.toleratesUnknownEnum()) {
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
  }

  /** Tests of the tolerant paths: overrides the installed handling until {@link #reset}. */
  @VisibleForTesting
  static synchronized void use(@Nonnull final UnknownDataHandling handling) {
    testOverride = handling;
  }

  /** Tests: removes the override, going back to the installed handling (strict if none). */
  @VisibleForTesting
  static synchronized void reset() {
    testOverride = null;
  }

  /**
   * Tests of {@link #install} itself, which model several process startups in one JVM: forgets the
   * installed handling so the next install starts afresh.
   */
  @VisibleForTesting
  static synchronized void uninstall() {
    installed = null;
  }
}
