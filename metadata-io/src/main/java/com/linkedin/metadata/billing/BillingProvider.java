package com.linkedin.metadata.billing;

import javax.annotation.Nonnull;

/**
 * Interface for billing providers to implement rate limiting and usage tracking.
 *
 * <p>This interface abstracts billing operations to allow for pluggable billing systems (Metronome,
 * Stripe, custom implementations, etc.).
 *
 * @see com.linkedin.metadata.billing.metronome.MetronomeClient
 */
public interface BillingProvider {

  /**
   * Look up the provider's internal customer ID by customer name.
   *
   * <p>Queries the billing provider to find a customer by their name/alias. This allows the billing
   * handler to resolve a customer without knowing provider-specific details.
   *
   * @param customerName The customer name to look up (e.g., hostname)
   * @return The provider's internal customer ID, or null if not found
   * @throws BillingException if the lookup fails
   */
  @javax.annotation.Nullable
  String getCustomerId(@Nonnull String customerName) throws BillingException;

  /**
   * Provision a customer in the billing system.
   *
   * <p>This method is idempotent. If the customer already exists in the billing system, it will add
   * the appropriate contract to the existing customer. If the customer does not exist, it will
   * create the customer first and then add the contract.
   *
   * <p>Provider-specific details (e.g., package alias, rate cards) are read from the provider's own
   * configuration.
   *
   * @param customerName The customer name to use in the billing provider
   * @return The billing provider's internal customer ID
   * @throws BillingException if customer provisioning or contract creation fails
   */
  @Nonnull
  String provisionCustomer(@Nonnull String customerName) throws BillingException;

  /**
   * Check if customer has remaining credits/quota for a specific product.
   *
   * <p>Called before processing requests to verify the customer has not exceeded their limit for
   * the specified product.
   *
   * @param customerId The billing provider's internal customer ID
   * @param product The billing product to check credits for
   * @return true if customer has remaining credits, false if limit is exhausted
   * @throws BillingException if balance check fails
   */
  boolean hasRemainingCredits(@Nonnull String customerId, @Nonnull BillingProduct product)
      throws BillingException;

  /**
   * Report usage after successful operation.
   *
   * <p>Called after an operation completes to decrement the customer's credit balance.
   *
   * @param customerId The billing provider's internal customer ID
   * @param eventType The type of event being reported (e.g., "ai_message", "data_export", etc.)
   * @param transactionId Unique identifier for this usage event (for idempotency)
   * @param quantity Number of credits to deduct
   * @param properties Additional properties to include with the usage event
   * @throws BillingException if usage reporting fails
   */
  void reportUsage(
      @Nonnull String customerId,
      @Nonnull String eventType,
      @Nonnull String transactionId,
      int quantity,
      @Nonnull java.util.Map<String, Object> properties)
      throws BillingException;

  /**
   * Get remaining balance for a customer for a specific product.
   *
   * <p>For monitoring/display purposes. Returns the number of credits remaining in the customer's
   * account for the specified product.
   *
   * @param customerId The billing provider's internal customer ID
   * @param productId The product ID to check balance for (e.g., "ask_datahub_product")
   * @return Number of remaining credits (0 if exhausted)
   * @throws BillingException if balance retrieval fails
   */
  int getRemainingBalance(@Nonnull String customerId, @Nonnull String productId)
      throws BillingException;
}
