package com.linkedin.metadata.billing.contract;

import com.linkedin.metadata.config.BillingConfiguration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Contract specification for free trial customers.
 *
 * <p>Free trial contracts provide limited credits on a monthly recurring basis. The contract starts
 * on the first day of the current month and typically has no expiration date (continues until
 * customer upgrades or cancels).
 *
 * <p>Configuration is typically loaded from {@link BillingConfiguration.MetronomeConfiguration
 * .ContractConfiguration}, which defines:
 *
 * <ul>
 *   <li>Rate card ID for free trial pricing
 *   <li>Recurring credit allocations (products, credit types, monthly limits)
 * </ul>
 *
 * @see ContractSpec
 * @see RecurringCreditSpec
 */
public class FreeTrialContractSpec implements ContractSpec {

  private static final String DEFAULT_CONTRACT_NAME = "Free Trial Credits";

  private final String name;
  private final String rateCardId;
  private final LocalDate startDate;
  private final List<RecurringCreditSpec> recurringCredits;

  /**
   * Create a free trial contract specification from configuration.
   *
   * <p>The contract start date is automatically set to the first day of the current month (UTC).
   *
   * @param config Contract configuration from billing configuration
   * @throws IllegalArgumentException if config is invalid or missing required fields
   */
  public FreeTrialContractSpec(
      @Nonnull BillingConfiguration.MetronomeConfiguration.ContractConfiguration config) {
    this(config, DEFAULT_CONTRACT_NAME);
  }

  /**
   * Create a free trial contract specification with custom name.
   *
   * @param config Contract configuration from billing configuration
   * @param contractName Custom contract name
   * @throws IllegalArgumentException if config is invalid or missing required fields
   */
  public FreeTrialContractSpec(
      @Nonnull BillingConfiguration.MetronomeConfiguration.ContractConfiguration config,
      @Nonnull String contractName) {
    Objects.requireNonNull(config, "Contract configuration must not be null");
    Objects.requireNonNull(contractName, "Contract name must not be null");

    if (config.getRateCardId() == null || config.getRateCardId().trim().isEmpty()) {
      throw new IllegalArgumentException("Rate card ID must not be null or empty");
    }

    if (config.getRecurringCredits() == null || config.getRecurringCredits().isEmpty()) {
      throw new IllegalArgumentException(
          "Free trial contract must have at least one recurring credit");
    }

    this.name = contractName;
    this.rateCardId = config.getRateCardId();
    this.startDate = calculateStartDate();
    this.recurringCredits = mapRecurringCredits(config.getRecurringCredits());
  }

  /**
   * Create a free trial contract specification with explicit values.
   *
   * @param name Contract name
   * @param rateCardId Rate card ID
   * @param startDate Contract start date
   * @param recurringCredits List of recurring credit specifications
   */
  public FreeTrialContractSpec(
      @Nonnull String name,
      @Nonnull String rateCardId,
      @Nonnull LocalDate startDate,
      @Nonnull List<RecurringCreditSpec> recurringCredits) {
    this.name = Objects.requireNonNull(name, "Contract name must not be null");
    this.rateCardId = Objects.requireNonNull(rateCardId, "Rate card ID must not be null");
    this.startDate = Objects.requireNonNull(startDate, "Start date must not be null");
    this.recurringCredits =
        Collections.unmodifiableList(
            new ArrayList<>(
                Objects.requireNonNull(recurringCredits, "Recurring credits must not be null")));
  }

  @Nonnull
  @Override
  public String getName() {
    return name;
  }

  @Nonnull
  @Override
  public String getRateCardId() {
    return rateCardId;
  }

  @Nonnull
  @Override
  public LocalDate getStartDate() {
    return startDate;
  }

  @Nonnull
  @Override
  public List<RecurringCreditSpec> getRecurringCredits() {
    return recurringCredits;
  }

  /**
   * Calculate contract start date as first day of current month at 00:00:00 UTC.
   *
   * @return First day of current month
   */
  private LocalDate calculateStartDate() {
    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    ZonedDateTime firstOfMonth =
        now.with(TemporalAdjusters.firstDayOfMonth())
            .withHour(0)
            .withMinute(0)
            .withSecond(0)
            .withNano(0);
    return firstOfMonth.toLocalDate();
  }

  /**
   * Map configuration recurring credits to RecurringCreditSpec objects.
   *
   * @param configCredits Recurring credits from configuration
   * @return List of RecurringCreditSpec objects
   */
  private List<RecurringCreditSpec> mapRecurringCredits(
      List<BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit>
          configCredits) {
    List<RecurringCreditSpec> specs = new ArrayList<>();

    for (BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit credit :
        configCredits) {
      specs.add(
          new RecurringCreditSpec(
              credit.getProductId(),
              credit.getCreditTypeId(),
              credit.getMonthlyCredits(),
              1, // Default priority
              credit.getDisplayName()));
    }

    return Collections.unmodifiableList(specs);
  }

  @Override
  public String toString() {
    return String.format(
        "FreeTrialContractSpec{name='%s', rateCardId='%s', startDate=%s, credits=%d}",
        name, rateCardId, startDate, recurringCredits.size());
  }
}
