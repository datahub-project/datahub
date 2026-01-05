package com.linkedin.metadata.billing.contract;

import static org.testng.Assert.*;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.testng.annotations.Test;

/** Unit tests for ContractSpec interface and default methods. */
public class ContractSpecTest {

  @Test
  public void testValidateWithValidContract() {
    // Test that validate() passes for a valid contract
    ContractSpec contract = createValidContract();

    // Should not throw any exception
    contract.validate();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateWithNullName() {
    // Test that validate() throws for null name
    ContractSpec contract =
        new TestContractSpec(null, "rate-card-123", LocalDate.now(), Collections.emptyList());
    contract.validate();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateWithEmptyRateCardId() {
    // Test that validate() throws for empty rate card ID
    ContractSpec contract =
        new TestContractSpec("Test Contract", "", LocalDate.now(), Collections.emptyList());
    contract.validate();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateWithNullStartDate() {
    // Test that validate() throws for null start date
    ContractSpec contract =
        new TestContractSpec("Test Contract", "rate-card-123", null, Collections.emptyList());
    contract.validate();
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testValidateWithNullRecurringCredits() {
    // Test that validate() throws for null recurring credits list
    ContractSpec contract =
        new TestContractSpec("Test Contract", "rate-card-123", LocalDate.now(), null);
    contract.validate();
  }

  @Test
  public void testValidateWithMultipleRecurringCredits() {
    // Test that validate() passes for contract with multiple credits
    List<RecurringCreditSpec> credits =
        Arrays.asList(
            new RecurringCreditSpec("product-1", "credit-1", 1000),
            new RecurringCreditSpec("product-2", "credit-2", 2000));
    ContractSpec contract =
        new TestContractSpec("Test Contract", "rate-card-123", LocalDate.now(), credits);

    // Should not throw any exception
    contract.validate();
  }

  @Test
  public void testContractWithPastStartDate() {
    // Test that validate() passes for contracts with past start dates
    LocalDate pastDate = LocalDate.now().minusMonths(6);
    ContractSpec contract =
        new TestContractSpec("Test Contract", "rate-card-123", pastDate, Collections.emptyList());

    // Should not throw any exception
    contract.validate();
  }

  @Test
  public void testContractWithFutureStartDate() {
    // Test that validate() passes for contracts with future start dates
    LocalDate futureDate = LocalDate.now().plusMonths(6);
    ContractSpec contract =
        new TestContractSpec("Test Contract", "rate-card-123", futureDate, Collections.emptyList());

    // Should not throw any exception
    contract.validate();
  }

  // Helper methods
  private ContractSpec createValidContract() {
    return new TestContractSpec(
        "Test Contract",
        "rate-card-123",
        LocalDate.now(),
        Collections.singletonList(new RecurringCreditSpec("product-1", "credit-1", 1000)));
  }

  // Test implementation of ContractSpec for testing default methods
  private static class TestContractSpec implements ContractSpec {
    private final String name;
    private final String rateCardId;
    private final LocalDate startDate;
    private final List<RecurringCreditSpec> recurringCredits;

    TestContractSpec(
        String name,
        String rateCardId,
        LocalDate startDate,
        List<RecurringCreditSpec> recurringCredits) {
      this.name = name;
      this.rateCardId = rateCardId;
      this.startDate = startDate;
      this.recurringCredits = recurringCredits;
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
  }
}
