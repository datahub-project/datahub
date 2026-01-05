package com.linkedin.metadata.billing.contract;

import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.BillingConfiguration;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.time.LocalDate;
import java.util.Collections;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for ContractFactory. */
public class ContractFactoryTest {

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
  public void testConstructorThrowsException() {
    // Test that ContractFactory cannot be instantiated (utility class)
    try {
      Constructor<ContractFactory> constructor = ContractFactory.class.getDeclaredConstructor();
      constructor.setAccessible(true);
      constructor.newInstance();
      fail("Expected UnsupportedOperationException");
    } catch (InvocationTargetException e) {
      // Expected - should wrap UnsupportedOperationException
      assertTrue(e.getCause() instanceof UnsupportedOperationException);
      assertTrue(e.getCause().getMessage().contains("utility class"));
    } catch (Exception e) {
      fail("Unexpected exception: " + e.getClass().getName());
    }
  }

  @Test
  public void testCreateFreeTrialContractWithConfig() {
    // Test creating free trial contract with config only (default name)
    ContractSpec contract = ContractFactory.createFreeTrialContract(mockContractConfig);

    assertNotNull(contract);
    assertTrue(contract instanceof FreeTrialContractSpec);
    assertEquals(contract.getName(), "Free Trial Credits");
    assertEquals(contract.getRateCardId(), TEST_RATE_CARD_ID);
    assertNotNull(contract.getStartDate());
    assertEquals(contract.getRecurringCredits().size(), 1);
  }

  @Test
  public void testCreateFreeTrialContractWithCustomName() {
    // Test creating free trial contract with custom name
    String customName = "Custom Free Trial";
    ContractSpec contract = ContractFactory.createFreeTrialContract(mockContractConfig, customName);

    assertNotNull(contract);
    assertTrue(contract instanceof FreeTrialContractSpec);
    assertEquals(contract.getName(), customName);
    assertEquals(contract.getRateCardId(), TEST_RATE_CARD_ID);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testCreateFreeTrialContractWithNullConfig() {
    // Should throw NullPointerException when config is null
    ContractFactory.createFreeTrialContract(null);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testCreateFreeTrialContractWithNullConfigAndName() {
    // Should throw NullPointerException when config is null (with custom name)
    ContractFactory.createFreeTrialContract(null, "Custom Name");
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateFreeTrialContractWithInvalidConfig() {
    // Should throw IllegalArgumentException when config is invalid (null rate card)
    when(mockContractConfig.getRateCardId()).thenReturn(null);
    ContractFactory.createFreeTrialContract(mockContractConfig);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testCreateFreeTrialContractWithEmptyRecurringCredits() {
    // Should throw IllegalArgumentException when recurring credits is empty
    when(mockContractConfig.getRecurringCredits()).thenReturn(Collections.emptyList());
    ContractFactory.createFreeTrialContract(mockContractConfig);
  }

  @Test
  public void testCreateFromConfigWithFreeTrialType() {
    // Test creating contract from config with any contract name (currently returns free trial)
    String contractName = "freeTrial";
    LocalDate startDate = LocalDate.now();

    ContractSpec contract =
        ContractFactory.createFromConfig(mockContractConfig, contractName, startDate);

    assertNotNull(contract);
    assertTrue(contract instanceof FreeTrialContractSpec);
    assertEquals(contract.getName(), contractName);
    assertEquals(contract.getRateCardId(), TEST_RATE_CARD_ID);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testCreateFromConfigWithNullContractName() {
    // Should throw NullPointerException when contract name is null
    ContractFactory.createFromConfig(mockContractConfig, null, LocalDate.now());
  }

  @Test
  public void testCreateFreeTrialContractReturnsContractSpec() {
    // Test that factory methods return ContractSpec interface
    ContractSpec contract = ContractFactory.createFreeTrialContract(mockContractConfig);

    // Should be usable as ContractSpec
    assertNotNull(contract.getName());
    assertNotNull(contract.getRateCardId());
    assertNotNull(contract.getStartDate());
    assertNotNull(contract.getRecurringCredits());
    assertNull(contract.getEndDate()); // Free trial has no end date
  }

  @Test
  public void testCreateFromConfigWithNullStartDateStillWorks() {
    // Test that createFromConfig works even with null start date
    // (FreeTrialContractSpec calculates its own start date)
    ContractSpec contract = ContractFactory.createFromConfig(mockContractConfig, "freeTrial", null);

    assertNotNull(contract);
    assertNotNull(contract.getStartDate()); // Should have a start date despite null input
  }
}
