package com.linkedin.metadata.config;

import lombok.Data;

/** Configuration for billing and usage tracking. Supports multiple billing providers */
@Data
public class BillingConfiguration {
  /** Whether billing is enabled for this instance */
  private boolean enabled;

  /** Billing provider to use (e.g., "metronome", "stripe") */
  private String provider;

  /** Metronome-specific configuration */
  private MetronomeConfiguration metronome;

  /** Configuration specific to Metronome billing provider. */
  @Data
  public static class MetronomeConfiguration {
    /** Metronome API key for authentication */
    private String apiKey;

    /** Base URL for Metronome API (e.g., https://api.metronome.com) */
    private String baseUrl;

    /**
     * Product ID mappings keyed by product config key.
     *
     * <p>Example YAML:
     *
     * <pre>
     * billing:
     *   metronome:
     *     products:
     *       askDataHubProductId: "fccd322e-7119-4a2b-a7ee-95f22ecb800e"
     * </pre>
     */
    private java.util.Map<String, String> products;

    /**
     * Metronome package alias used when creating contracts for customers.
     *
     * <p>Packages in Metronome bundle a rate card, recurring credits, and other contract details
     * into a reusable template. Using a package alias simplifies contract creation to a single
     * identifier instead of specifying rate cards and credits individually.
     */
    private String packageAlias;
  }
}
