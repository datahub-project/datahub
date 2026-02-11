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
     * Named contract templates for different customer types.
     *
     * <p>Allows defining multiple contract types in configuration without code changes. Map keys
     * are contract template names (e.g., "freeTrial", "standard", "enterprise"), and values are
     * contract configurations.
     *
     * <p>Example YAML:
     *
     * <pre>
     * billing:
     *   metronome:
     *     contracts:
     *       freeTrial:
     *         rateCardId: "rc-free-trial"
     *         recurringCredits: [...]
     *       standard:
     *         rateCardId: "rc-standard"
     *         recurringCredits: [...]
     *       enterprise:
     *         rateCardId: "rc-enterprise"
     *         recurringCredits: [...]
     * </pre>
     */
    private java.util.Map<String, ContractConfiguration> contracts;

    /**
     * Configuration for a contract with recurring credits.
     *
     * <p>A contract defines the pricing structure (rate card) and the recurring credits that
     * customers receive. This can be used for free trials, paid plans, or custom agreements.
     */
    @Data
    public static class ContractConfiguration {
      /** Metronome rate card ID that defines pricing structure */
      private String rateCardId;

      /** List of recurring credit allocations for different products/credit types */
      private java.util.List<RecurringCredit> recurringCredits;

      /**
       * Recurring credit allocation for a specific product and credit type.
       *
       * <p>Example: 1000 monthly credits for "Ask DataHub" answers, or 5000 monthly credits for
       * search queries.
       */
      @Data
      public static class RecurringCredit {
        /** Product name identifier for lookup (e.g., "askDataHub") */
        private String productName;

        /** Metronome product ID (e.g., AI Chat product, Search product) */
        private String productId;

        /** Metronome credit type ID (e.g., answers, searches, API calls) */
        private String creditTypeId;

        /** Number of credits to grant per month */
        private int monthlyCredits;

        /** Optional display name for logging and debugging (e.g., "Ask DataHub Answers") */
        private String displayName;
      }
    }
  }
}
