package com.linkedin.metadata.billing.contract;

import java.time.LocalDate;
import java.util.List;
import javax.annotation.Nonnull;

/**
 * Specification for a billing contract.
 *
 * <p>A contract defines the pricing structure (rate card) and recurring credit allocations for a
 * customer. Contracts can represent free trials, paid subscriptions, enterprise agreements, or
 * custom pricing arrangements.
 *
 * <p>Multiple contracts can be assigned to a single customer to support complex billing scenarios:
 *
 * <ul>
 *   <li>Free trial with base credits + promotional bonus credits
 *   <li>Standard subscription + usage overage contract
 *   <li>Multiple product lines with separate rate cards
 * </ul>
 *
 * <p>Implementations of this interface should be immutable and thread-safe.
 *
 * @see RecurringCreditSpec
 * @see FreeTrialContractSpec
 * @see StandardContractSpec
 */
public interface ContractSpec {

  /**
   * Get the contract name for display and identification.
   *
   * <p>Examples: "Free Trial Credits", "Enterprise Base Subscription", "Usage Overage"
   *
   * @return Human-readable contract name
   */
  @Nonnull
  String getName();

  /**
   * Get the billing provider's rate card identifier.
   *
   * <p>The rate card defines the pricing structure, unit costs, and billing rules for this
   * contract. Rate cards are typically configured in the billing provider's system (e.g.,
   * Metronome, Stripe).
   *
   * @return Rate card ID from the billing provider
   */
  @Nonnull
  String getRateCardId();

  /**
   * Get the contract start date.
   *
   * <p>Determines when the contract becomes active and billing begins. For recurring credits, this
   * is typically the start of a billing period (e.g., first day of month).
   *
   * @return Contract start date
   */
  @Nonnull
  LocalDate getStartDate();

  /**
   * Get the list of recurring credit allocations for this contract.
   *
   * <p>Each recurring credit represents a pool of credits that renews on a schedule (e.g.,
   * monthly). Customers can have multiple credit types for different products or features.
   *
   * @return List of recurring credit specifications (empty list if none)
   */
  @Nonnull
  List<RecurringCreditSpec> getRecurringCredits();

  /**
   * Get optional contract end date.
   *
   * <p>If specified, the contract will automatically expire on this date. Useful for time-limited
   * trials or promotional periods. Returns null for contracts with no expiration.
   *
   * @return Contract end date, or null if contract has no expiration
   */
  @javax.annotation.Nullable
  default LocalDate getEndDate() {
    return null;
  }

  /**
   * Validate that the contract specification is complete and correct.
   *
   * <p>Implementations should override this to perform validation specific to their contract type.
   * The default implementation checks for null/empty required fields.
   *
   * @throws IllegalStateException if the contract specification is invalid
   */
  default void validate() {
    if (getName() == null || getName().trim().isEmpty()) {
      throw new IllegalStateException("Contract name must not be null or empty");
    }
    if (getRateCardId() == null || getRateCardId().trim().isEmpty()) {
      throw new IllegalStateException("Rate card ID must not be null or empty");
    }
    if (getStartDate() == null) {
      throw new IllegalStateException("Start date must not be null");
    }
    if (getRecurringCredits() == null) {
      throw new IllegalStateException("Recurring credits list must not be null");
    }
  }
}
