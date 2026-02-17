package com.linkedin.metadata.billing;

import java.util.Objects;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Main entry point for billing operations in DataHub.
 *
 * <p>This service orchestrates customer provisioning by routing requests to the configured billing
 * provider. It is provider-agnostic and delegates all provider-specific logic to the {@link
 * BillingProvider} implementation.
 *
 * @see BillingProvider for provider implementations
 * @see BillingProduct for product definitions
 */
@Slf4j
public class BillingHandler {

  // Event type constants
  public static final String EVENT_TYPE_AI_MESSAGE = "ai_message";

  private final boolean enabled;
  private final BillingProvider provider;
  private final String customerName;

  // Cached provider customer ID (fetched from provider API or set after provisioning)
  private volatile String cachedProviderCustomerId;

  /**
   * Construct a BillingHandler.
   *
   * @param enabled Whether billing is enabled
   * @param provider Billing provider implementation
   * @param customerName Customer name/hostname for billing provider
   */
  public BillingHandler(
      boolean enabled, @Nonnull BillingProvider provider, @Nonnull String customerName) {
    this.enabled = enabled;
    this.provider = Objects.requireNonNull(provider, "provider must not be null");
    this.customerName = Objects.requireNonNull(customerName, "customerName must not be null");
  }

  /**
   * Check if billing is enabled.
   *
   * @return true if billing is enabled
   */
  public boolean isEnabled() {
    return enabled;
  }

  /**
   * Provision a customer in the billing system.
   *
   * <p>This method is idempotent. If the customer already exists in the billing provider, it will
   * add the appropriate contract to the existing customer. If the customer does not exist, it will
   * create the customer first and then assign the contract.
   *
   * <p>Provider-specific details (e.g., package alias, rate cards) are handled internally by the
   * billing provider.
   *
   * @throws BillingException if provisioning fails
   */
  public void provisionCustomer() throws BillingException {
    if (!isEnabled()) {
      return;
    }

    log.info("Provisioning customer '{}'", customerName);

    try {
      String providerCustomerId = provider.provisionCustomer(customerName);
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
   * <p>Queries the billing provider to retrieve the customer ID by customer name. The result is
   * cached in memory to avoid repeated API calls.
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

      String customerId = provider.getCustomerId(customerName);

      if (customerId != null) {
        this.cachedProviderCustomerId = customerId;
        log.info("Retrieved customer ID from provider: {}", customerId);
        return customerId;
      }

      log.warn("Customer not found: {}. May need to be provisioned.", customerName);
      throw new BillingException("Customer not found. Please provision the customer first.");
    }
  }

  /**
   * Check if instance has remaining credits for a specific billing product.
   *
   * <p>This should be called before processing requests to ensure the customer has not exceeded
   * their limit.
   *
   * @param product The billing product to check credits for
   * @return true if credits remain (or if billing is disabled/fails), false if limit is exhausted
   */
  public boolean hasRemainingCredits(@Nonnull BillingProduct product) {
    Objects.requireNonNull(product, "product must not be null");

    if (!isEnabled()) {
      return true;
    }

    try {
      String providerCustomerId = getProviderCustomerId();
      return provider.hasRemainingCredits(providerCustomerId, product);
    } catch (Exception e) {
      log.error("Failed to check credits for product: {}, failing open", product.name(), e);
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
