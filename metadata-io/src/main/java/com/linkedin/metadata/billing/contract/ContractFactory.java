package com.linkedin.metadata.billing.contract;

import com.linkedin.metadata.config.BillingConfiguration;
import java.time.LocalDate;
import java.util.Objects;
import javax.annotation.Nonnull;

/**
 * Factory for creating common contract types from configuration.
 *
 * <p>This factory simplifies contract construction by providing static factory methods for
 * frequently used contract types. Contracts can be created from configuration or with custom
 * parameters.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Create free trial contract from configuration
 * ContractSpec freeTrial = ContractFactory.createFreeTrialContract(
 *     config.getMetronome().getContracts().get("freeTrial"));
 * }</pre>
 *
 * @see ContractSpec
 * @see FreeTrialContractSpec
 */
public final class ContractFactory {

  // Private constructor to prevent instantiation
  private ContractFactory() {
    throw new UnsupportedOperationException(
        "ContractFactory is a utility class and cannot be instantiated");
  }

  /**
   * Create a free trial contract from configuration.
   *
   * <p>The contract will use default settings:
   *
   * <ul>
   *   <li>Name: "Free Trial Credits"
   *   <li>Start date: First day of current month (UTC)
   *   <li>Rate card and credits from configuration
   * </ul>
   *
   * @param config Free trial contract configuration
   * @return Free trial contract specification
   * @throws IllegalArgumentException if config is null or invalid
   */
  @Nonnull
  public static ContractSpec createFreeTrialContract(
      @Nonnull BillingConfiguration.MetronomeConfiguration.ContractConfiguration config) {
    Objects.requireNonNull(config, "Contract configuration must not be null");
    return new FreeTrialContractSpec(config);
  }

  /**
   * Create a free trial contract with custom name.
   *
   * @param config Free trial contract configuration
   * @param contractName Custom contract name
   * @return Free trial contract specification
   * @throws IllegalArgumentException if config or contractName is null or invalid
   */
  @Nonnull
  public static ContractSpec createFreeTrialContract(
      @Nonnull BillingConfiguration.MetronomeConfiguration.ContractConfiguration config,
      @Nonnull String contractName) {
    Objects.requireNonNull(config, "Contract configuration must not be null");
    Objects.requireNonNull(contractName, "Contract name must not be null");
    return new FreeTrialContractSpec(config, contractName);
  }

  /**
   * Create a contract from named configuration template.
   *
   * <p>Currently only supports free trial contracts. The contract name is used to identify the
   * contract but does not affect behavior.
   *
   * @param config Contract configuration
   * @param contractName Name to use for the contract
   * @param startDate Contract start date (currently ignored, uses first day of month)
   * @return Free trial contract specification
   * @throws IllegalArgumentException if config or contractName is null or invalid
   */
  @Nonnull
  public static ContractSpec createFromConfig(
      @Nonnull BillingConfiguration.MetronomeConfiguration.ContractConfiguration config,
      @Nonnull String contractName,
      @Nonnull LocalDate startDate) {
    Objects.requireNonNull(config, "Contract configuration must not be null");
    Objects.requireNonNull(contractName, "Contract name must not be null");

    // Currently only supports free trial contracts
    // return corresponding spec based on contract name in future
    return new FreeTrialContractSpec(config, contractName);
  }
}
