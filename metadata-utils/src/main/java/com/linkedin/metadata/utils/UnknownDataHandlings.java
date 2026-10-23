package com.linkedin.metadata.utils;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nonnull;

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
 * system-update do it while building the system operation context; a process that writes through
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

  private UnknownDataHandlings() {}

  @Nonnull
  public static UnknownDataHandling handling() {
    return current;
  }

  /** Called once while the system operation context is built. */
  public static void use(@Nonnull final UnknownDataHandling handling) {
    current = handling;
  }

  @VisibleForTesting
  public static void reset() {
    current = StrictHandling.INSTANCE;
  }
}
