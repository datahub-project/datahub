package com.linkedin.metadata.billing;

import javax.annotation.Nonnull;

/**
 * Enumeration of billable DataHub products.
 *
 * <p>Each product maps to a configuration key under {@code metronome.products} in application.yaml.
 * The actual billing provider product ID is loaded from configuration.
 *
 * <p>When adding new products, add the corresponding configuration key here and the matching
 * property in application.yaml under {@code metronome.products}.
 */
public enum BillingProduct {
  /** Ask DataHub - maps to {@code metronome.products.askDataHubProductId} */
  ASK_DATAHUB("askDataHubProductId");

  private final String configKey;

  BillingProduct(@Nonnull String configKey) {
    this.configKey = configKey;
  }

  /** Returns the configuration key used to look up the billing provider product ID. */
  @Nonnull
  public String getConfigKey() {
    return configKey;
  }
}
