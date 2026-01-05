package com.linkedin.metadata.billing;

import com.linkedin.metadata.billing.contract.ContractSpec;
import com.linkedin.metadata.config.BillingConfiguration;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Main entry point for billing operations in DataHub.
 *
 * <p>This service orchestrates customer provisioning by routing requests to the configured billing
 * provider.
 *
 * @see BillingProvider for provider implementations
 * @see BillingConfiguration for configuration options
 */
@Slf4j
public class BillingHandler {

  // Event type constants
  public static final String EVENT_TYPE_AI_MESSAGE = "ai_message";

  private final BillingConfiguration config;
  private final BillingProvider provider;
  private final String customerName;

  // Cached provider customer ID (fetched from provider API or set after provisioning)
  private volatile String cachedProviderCustomerId;

  /**
   * Construct a BillingHandler.
   *
   * @param config Billing configuration (provider, API keys, limits, etc.)
   * @param provider Billing provider implementation
   * @param customerName Customer name/hostname for billing provider
   */
  public BillingHandler(
      @Nonnull BillingConfiguration config,
      @Nonnull BillingProvider provider,
      @Nonnull String customerName) {
    this.config = Objects.requireNonNull(config, "config must not be null");
    this.provider = Objects.requireNonNull(provider, "provider must not be null");
    this.customerName = Objects.requireNonNull(customerName, "customerName must not be null");
  }

  /**
   * Check if billing is enabled.
   *
   * @return true if billing is enabled in configuration
   */
  public boolean isEnabled() {
    return config.isEnabled();
  }

  /**
   * Get the product ID for a specific contract type and product name.
   *
   * <p>Retrieves the product ID from the billing configuration based on the contract type (e.g.,
   * "freeTrial") and the product name (e.g., "askDataHub").
   *
   * @param contractType The contract type (e.g., "freeTrial", "standard")
   * @param productName The product name to search for
   * @return The product ID, or null if not found
   */
  @javax.annotation.Nullable
  public String getProductId(@Nonnull String contractType, @Nonnull String productName) {
    Objects.requireNonNull(contractType, "contractType must not be null");
    Objects.requireNonNull(productName, "productName must not be null");

    if (config.getMetronome() == null || config.getMetronome().getContracts() == null) {
      return null;
    }

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contract =
        config.getMetronome().getContracts().get(contractType);

    if (contract == null
        || contract.getRecurringCredits() == null
        || contract.getRecurringCredits().isEmpty()) {
      return null;
    }

    // Search for the product by name
    for (BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit credit :
        contract.getRecurringCredits()) {
      if (productName.equals(credit.getProductName())) {
        return credit.getProductId();
      }
    }

    // Product name not found
    log.warn(
        "Product name '{}' not found in contract '{}' recurring credits",
        productName,
        contractType);
    return null;
  }

  /**
   * Provision a customer with one or more contracts.
   *
   * <p>This method is idempotent. If the customer already exists in the billing provider, it will
   * add the specified contracts to the existing customer. If the customer does not exist, it will
   * create the customer first and then add the contracts.
   *
   * <p>Multiple contracts can be provided to support complex billing scenarios:
   *
   * <ul>
   *   <li>Free trial + promotional credits
   *   <li>Multiple product lines with separate rate cards
   * </ul>
   *
   * @param contracts List of contract specifications to create
   * @throws BillingException if provisioning fails
   * @throws IllegalArgumentException if contracts is null or empty
   */
  public void provisionCustomer(@Nonnull List<ContractSpec> contracts) throws BillingException {
    Objects.requireNonNull(contracts, "contracts must not be null");

    if (!isEnabled()) {
      return;
    }

    if (contracts.isEmpty()) {
      throw new IllegalArgumentException("At least one contract must be provided");
    }

    log.info("Provisioning customer '{}' with {} contract(s)", customerName, contracts.size());

    try {
      String providerCustomerId = provider.provisionCustomer(customerName, contracts);
      this.cachedProviderCustomerId = providerCustomerId;
      log.info(
          "Successfully provisioned customer '{}' with ID: {}", customerName, providerCustomerId);
    } catch (Exception e) {
      log.error("Failed to provision customer '{}': {}", customerName, e.getMessage(), e);
      throw new BillingException("Failed to provision customer", e);
    }
  }

  /**
   * Get the provider's internal customer ID.
   *
   * <p>This queries the billing provider API to retrieve the customer ID. For Metronome, it uses
   * the ingest alias (hostname) to look up the customer. The result is cached in memory to avoid
   * repeated API calls.
   *
   * @return The provider's internal customer ID
   * @throws BillingException if customer ID cannot be retrieved
   */
  @Nonnull
  public String getProviderCustomerId() throws BillingException {
    if (cachedProviderCustomerId != null) {
      return cachedProviderCustomerId;
    }

    synchronized (this) {
      if (cachedProviderCustomerId != null) {
        return cachedProviderCustomerId;
      }

      // For Metronome, query by ingest alias (hostname)
      if (provider instanceof com.linkedin.metadata.billing.metronome.MetronomeClient) {
        com.linkedin.metadata.billing.metronome.MetronomeClient metronomeClient =
            (com.linkedin.metadata.billing.metronome.MetronomeClient) provider;
        String customerId = metronomeClient.getCustomerByIngestAlias(customerName);

        if (customerId != null) {
          this.cachedProviderCustomerId = customerId;
          log.info("Retrieved customer ID from provider API: {}", customerId);
          return customerId;
        }

        // Customer not found, might need to be provisioned
        log.warn(
            "Customer not found with ingest alias: {}. May need to be provisioned.", customerName);
        throw new BillingException("Customer not found. Please provision the customer first.");
      }

      // For other providers, we'd need to implement similar logic
      throw new BillingException(
          "Unable to get customer ID: Provider does not support customer lookup");
    }
  }

  /**
   * Check if instance has remaining credits for a specific product.
   *
   * <p>Returns true if the customer has credits remaining for the specified product. This should be
   * called before processing requests to ensure the customer has not exceeded their limit.
   *
   * @param productId The product ID to check credits for (e.g., "ask_datahub_product")
   * @return true if credits remain (or if billing is disabled/fails), false if limit is exhausted
   */
  public boolean hasRemainingCredits(@Nonnull String productId) {
    Objects.requireNonNull(productId, "productId must not be null");

    if (!isEnabled()) {
      return true;
    }

    try {
      String providerCustomerId = getProviderCustomerId();
      boolean hasCredits = provider.hasRemainingCredits(providerCustomerId, productId);
      return hasCredits;
    } catch (Exception e) {
      log.error("Failed to check credits for product: {}, failing open", productId, e);
      return true;
    }
  }

  /**
   * Report usage to billing provider.
   *
   * <p>Generic method to report usage events to the billing system. This decrements the customer's
   * credit balance for all the associated billable metrics tied to any given contracts.
   *
   * @param eventType The type of event being reported (e.g., "ai_message", "data_export", etc.)
   * @param transactionId Unique identifier for this usage event (for idempotency)
   * @param quantity Number of credits to deduct
   * @param properties Additional properties to include with the usage event
   */
  public void reportUsage(
      @Nonnull String eventType,
      @Nonnull String transactionId,
      int quantity,
      @Nonnull java.util.Map<String, Object> properties) {
    Objects.requireNonNull(eventType, "eventType must not be null");
    Objects.requireNonNull(transactionId, "transactionId must not be null");
    Objects.requireNonNull(properties, "properties must not be null");

    if (!isEnabled()) {
      return;
    }

    try {
      provider.reportUsage(customerName, eventType, transactionId, quantity, properties);
    } catch (Exception e) {
      log.error(
          "Failed to report usage for transaction: {} (event_type: {})",
          transactionId,
          eventType,
          e);
    }
  }
}
