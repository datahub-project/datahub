package com.linkedin.metadata.utils;

import com.google.common.annotations.VisibleForTesting;
import javax.annotation.Nonnull;

/**
 * The {@link UnknownDataHandling} this process uses, chosen once at startup from configuration.
 * Static because much of the code that needs it (validation, schema walks, registry helpers) has no
 * request context.
 */
public final class UnknownDataHandlings {

  // Tolerant until configured, so tests and tools that never build the system context keep the
  // default behavior.
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
