package com.linkedin.metadata.billing.contract;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Value;

/**
 * Immutable specification for a recurring credit allocation.
 *
 * <p>Defines how credits are allocated to customers on a recurring basis (e.g., monthly). Each
 * recurring credit is associated with a specific product and credit type in the billing provider.
 *
 * <p>Example: A free trial might include 1000 monthly credits for "Ask DataHub" answers and 5000
 * monthly credits for search queries.
 *
 * @see ContractSpec
 */
@Value
public class RecurringCreditSpec {

  /** Billing provider's product identifier (e.g., Metronome product ID) */
  @Nonnull String productId;

  /** Billing provider's credit type identifier (e.g., answers, searches, API calls) */
  @Nonnull String creditTypeId;

  /** Number of credits to grant per billing period (e.g., per month) */
  int monthlyCredits;

  /**
   * Priority for credit consumption (higher priority credits are consumed first).
   *
   * <p>Defaults to 1 if not specified. Useful when customers have multiple credit pools.
   */
  int priority;

  /** Optional display name for logging and debugging (e.g., "Ask DataHub Answers") */
  @Nullable String displayName;

  /**
   * Create a recurring credit specification.
   *
   * @param productId Billing provider's product identifier
   * @param creditTypeId Billing provider's credit type identifier
   * @param monthlyCredits Number of credits to grant per month
   * @param priority Priority for credit consumption (higher = consumed first)
   * @param displayName Optional display name for logging
   */
  public RecurringCreditSpec(
      @Nonnull String productId,
      @Nonnull String creditTypeId,
      int monthlyCredits,
      int priority,
      @Nullable String displayName) {
    if (productId == null || productId.trim().isEmpty()) {
      throw new IllegalArgumentException("productId must not be null or empty");
    }
    if (creditTypeId == null || creditTypeId.trim().isEmpty()) {
      throw new IllegalArgumentException("creditTypeId must not be null or empty");
    }
    if (monthlyCredits <= 0) {
      throw new IllegalArgumentException("monthlyCredits must be positive");
    }

    this.productId = productId;
    this.creditTypeId = creditTypeId;
    this.monthlyCredits = monthlyCredits;
    this.priority = priority;
    this.displayName = displayName;
  }

  /**
   * Create a recurring credit specification with default priority (1).
   *
   * @param productId Billing provider's product identifier
   * @param creditTypeId Billing provider's credit type identifier
   * @param monthlyCredits Number of credits to grant per month
   * @param displayName Optional display name for logging
   */
  public RecurringCreditSpec(
      @Nonnull String productId,
      @Nonnull String creditTypeId,
      int monthlyCredits,
      @Nullable String displayName) {
    this(productId, creditTypeId, monthlyCredits, 1, displayName);
  }

  /**
   * Create a recurring credit specification with default priority and no display name.
   *
   * @param productId Billing provider's product identifier
   * @param creditTypeId Billing provider's credit type identifier
   * @param monthlyCredits Number of credits to grant per month
   */
  public RecurringCreditSpec(
      @Nonnull String productId, @Nonnull String creditTypeId, int monthlyCredits) {
    this(productId, creditTypeId, monthlyCredits, 1, null);
  }
}
