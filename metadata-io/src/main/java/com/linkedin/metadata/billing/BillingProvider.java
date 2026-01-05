package com.linkedin.metadata.billing;

import com.linkedin.metadata.billing.contract.ContractSpec;
import java.util.List;
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
   * Provision a customer and add contracts.
   *
   * <p>This method is idempotent. If the customer already exists in the billing system, it will
   * simply add the specified contracts to the existing customer. If the customer does not exist, it
   * will create the customer first and then add the contracts.
   *
   * <p>The contracts are created in the order provided. If any contract creation fails, the
   * implementation should decide whether to rollback or continue (provider-specific behavior).
   *
   * @param customerName The customer name to use in the billing provider
   * @param contracts List of contract specifications to create for the customer
   * @return The billing provider's internal customer ID
   * @throws BillingException if customer provisioning or contract creation fails
   * @throws IllegalArgumentException if contracts list is null or empty
   */
  @Nonnull
  String provisionCustomer(@Nonnull String customerName, @Nonnull List<ContractSpec> contracts)
      throws BillingException;

  /**
   * Check if customer has remaining credits/quota for a specific product.
   *
   * <p>Called before processing requests to verify the customer has not exceeded their limit for
   * the specified product.
   *
   * @param customerId The billing provider's internal customer ID
   * @param productId The product ID to check credits for (e.g., "ask_datahub_product")
   * @return true if customer has remaining credits, false if limit is exhausted
   * @throws BillingException if balance check fails
   */
  boolean hasRemainingCredits(@Nonnull String customerId, @Nonnull String productId)
      throws BillingException;

  /**
   * Report usage after successful operation.
   *
   * <p>Called after AI chat response is generated to decrement the customer's credit balance. This
   * is typically called asynchronously to avoid blocking the response.
   *
   * @param customerId The billing provider's internal customer ID
   * @param eventType The type of event being reported (e.g., "ai_message", "data_export", etc.)
   * @param transactionId Unique identifier for this usage event (for idempotency)
   * @param quantity Number of credits to deduct (typically 1 per AI chat answer)
   * @param properties Additional properties to include with the usage event (e.g., conversation_id,
   *     user_id)
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
