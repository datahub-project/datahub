package com.linkedin.metadata.billing.contract;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.BillingConfiguration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.TemporalAdjusters;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for FreeTrialContractSpec. */
public class FreeTrialContractSpecTest {

  private static final String TEST_RATE_CARD_ID = "rate-card-free-trial";
  private static final String TEST_PRODUCT_ID = "product-123";
  private static final String TEST_CREDIT_TYPE_ID = "credit-type-456";
  private static final int TEST_MONTHLY_CREDITS = 1000;
  private static final String TEST_DISPLAY_NAME = "Ask DataHub Answers";

  private BillingConfiguration.MetronomeConfiguration.ContractConfiguration mockContractConfig;
  private BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
      mockCredit;

  @BeforeMethod
  public void setUp() {
    mockContractConfig =
        mock(BillingConfiguration.MetronomeConfiguration.ContractConfiguration.class);
    mockCredit =
        mock(
            BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
                .class);

    when(mockContractConfig.getRateCardId()).thenReturn(TEST_RATE_CARD_ID);
    when(mockContractConfig.getRecurringCredits())
        .thenReturn(Collections.singletonList(mockCredit));

    when(mockCredit.getProductId()).thenReturn(TEST_PRODUCT_ID);
    when(mockCredit.getCreditTypeId()).thenReturn(TEST_CREDIT_TYPE_ID);
    when(mockCredit.getMonthlyCredits()).thenReturn(TEST_MONTHLY_CREDITS);
    when(mockCredit.getDisplayName()).thenReturn(TEST_DISPLAY_NAME);
  }

  @Test
  public void testConstructorWithConfigDefaultName() {
    // Test constructor with config and default name
    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig);

    assertEquals(spec.getName(), "Free Trial Credits");
    assertEquals(spec.getRateCardId(), TEST_RATE_CARD_ID);
    assertNotNull(spec.getStartDate());
    assertEquals(spec.getRecurringCredits().size(), 1);
  }

  @Test
  public void testConstructorWithConfigCustomName() {
    // Test constructor with config and custom name
    String customName = "Custom Free Trial";
    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig, customName);

    assertEquals(spec.getName(), customName);
    assertEquals(spec.getRateCardId(), TEST_RATE_CARD_ID);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullConfig() {
    // Should throw NullPointerException when config is null
    new FreeTrialContractSpec(null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullContractName() {
    // Should throw NullPointerException when contract name is null
    new FreeTrialContractSpec(mockContractConfig, null);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithEmptyRateCardId() {
    // Should throw IllegalArgumentException when rate card ID is empty
    when(mockContractConfig.getRateCardId()).thenReturn("");
    new FreeTrialContractSpec(mockContractConfig);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithEmptyRecurringCredits() {
    // Should throw IllegalArgumentException when recurring credits is empty
    when(mockContractConfig.getRecurringCredits()).thenReturn(Collections.emptyList());
    new FreeTrialContractSpec(mockContractConfig);
  }

  @Test
  public void testStartDateIsFirstDayOfCurrentMonth() {
    // Test that start date is set to first day of current month (UTC)
    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig);

    ZonedDateTime now = ZonedDateTime.now(ZoneId.of("UTC"));
    LocalDate expectedStartDate = now.with(TemporalAdjusters.firstDayOfMonth()).toLocalDate();

    assertEquals(spec.getStartDate(), expectedStartDate);
  }

  @Test
  public void testRecurringCreditsMapping() {
    // Test that recurring credits are properly mapped from config
    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig);

    List<RecurringCreditSpec> credits = spec.getRecurringCredits();
    assertEquals(credits.size(), 1);

    RecurringCreditSpec credit = credits.get(0);
    assertEquals(credit.getProductId(), TEST_PRODUCT_ID);
    assertEquals(credit.getCreditTypeId(), TEST_CREDIT_TYPE_ID);
    assertEquals(credit.getMonthlyCredits(), TEST_MONTHLY_CREDITS);
    assertEquals(credit.getPriority(), 1); // Default priority
    assertEquals(credit.getDisplayName(), TEST_DISPLAY_NAME);
  }

  @Test
  public void testRecurringCreditsWithMultipleCredits() {
    // Test with multiple recurring credits
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit mockCredit2 =
        mock(
            BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
                .class);
    when(mockCredit2.getProductId()).thenReturn("product-789");
    when(mockCredit2.getCreditTypeId()).thenReturn("credit-type-789");
    when(mockCredit2.getMonthlyCredits()).thenReturn(2000);
    when(mockCredit2.getDisplayName()).thenReturn("Search Queries");

    when(mockContractConfig.getRecurringCredits())
        .thenReturn(Arrays.asList(mockCredit, mockCredit2));

    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig);

    List<RecurringCreditSpec> credits = spec.getRecurringCredits();
    assertEquals(credits.size(), 2);
    assertEquals(credits.get(0).getProductId(), TEST_PRODUCT_ID);
    assertEquals(credits.get(1).getProductId(), "product-789");
  }

  @Test
  public void testConstructorWithExplicitValues() {
    // Test constructor with explicit values
    String name = "Test Contract";
    String rateCardId = "test-rate-card";
    LocalDate startDate = LocalDate.of(2024, 1, 1);
    List<RecurringCreditSpec> credits =
        Collections.singletonList(
            new RecurringCreditSpec(TEST_PRODUCT_ID, TEST_CREDIT_TYPE_ID, TEST_MONTHLY_CREDITS));

    FreeTrialContractSpec spec = new FreeTrialContractSpec(name, rateCardId, startDate, credits);

    assertEquals(spec.getName(), name);
    assertEquals(spec.getRateCardId(), rateCardId);
    assertEquals(spec.getStartDate(), startDate);
    assertEquals(spec.getRecurringCredits().size(), 1);
  }

  @Test
  public void testValidateSuccess() {
    // Test that validate() passes for a valid free trial contract
    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig);

    // Should not throw any exception
    spec.validate();
  }

  @Test
  public void testContractImplementsContractSpec() {
    // Test that FreeTrialContractSpec properly implements ContractSpec
    FreeTrialContractSpec spec = new FreeTrialContractSpec(mockContractConfig);

    // Should be assignable to ContractSpec
    ContractSpec contractSpec = spec;
    assertNotNull(contractSpec);
    assertEquals(contractSpec.getName(), spec.getName());
    assertEquals(contractSpec.getRateCardId(), spec.getRateCardId());
    assertEquals(contractSpec.getStartDate(), spec.getStartDate());
    assertEquals(contractSpec.getRecurringCredits(), spec.getRecurringCredits());
  }
}
