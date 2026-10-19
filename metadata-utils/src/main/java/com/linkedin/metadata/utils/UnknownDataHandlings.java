package com.linkedin.metadata.utils;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nonnull;

/**
 * The {@link UnknownDataHandling} this process uses, chosen once at startup from {@code
 * featureFlags.unknownDataToleranceEnabled}.
 *
 * <p>Process-wide on purpose: the flag is a per-deployment setting that never varies by request,
 * and much of the code that needs it (validation, schema walks, batch items, registry helpers,
 * GraphQL mappers) has no {@code OperationContext}. Carrying it on the context would need a
 * separate carrier, each with its own default, at every such place.
 *
 * <p>Every process that runs the unknown-data paths must apply the flag at startup through {@code
 * SystemOperationContextFactory.configureUnknownDataHandling}: GMS, the MAE and MCE consumers and
 * system-update do it while building the system operation context; a process that writes through
 * {@code EntityService} without that context must call it itself. A process that never applies it
 * (tests, tools) keeps the tolerant default, the same as the flag's default.
 *
 * <p>Tests that switch it with {@link #use} must not run in parallel with other classes in the same
 * JVM, and must {@link #reset} afterwards.
 */
public final class UnknownDataHandlings {

  // Tolerant until configured: the same as the flag's default (on).
  private static volatile UnknownDataHandling current = TolerantHandling.INSTANCE;

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
    current = TolerantHandling.INSTANCE;
  }
}
