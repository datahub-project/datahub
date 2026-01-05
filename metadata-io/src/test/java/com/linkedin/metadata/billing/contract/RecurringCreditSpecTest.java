package com.linkedin.metadata.billing.contract;

import static org.testng.Assert.*;

import org.testng.annotations.Test;

/** Unit tests for RecurringCreditSpec. */
public class RecurringCreditSpecTest {

  private static final String TEST_PRODUCT_ID = "product-123";
  private static final String TEST_CREDIT_TYPE_ID = "credit-type-456";
  private static final int TEST_MONTHLY_CREDITS = 1000;
  private static final String TEST_DISPLAY_NAME = "Test Credits";

  @Test
  public void testConstructorWithAllParameters() {
    // Test full constructor with all parameters
    RecurringCreditSpec spec =
        new RecurringCreditSpec(
            TEST_PRODUCT_ID, TEST_CREDIT_TYPE_ID, TEST_MONTHLY_CREDITS, 5, TEST_DISPLAY_NAME);

    assertEquals(spec.getProductId(), TEST_PRODUCT_ID);
    assertEquals(spec.getCreditTypeId(), TEST_CREDIT_TYPE_ID);
    assertEquals(spec.getMonthlyCredits(), TEST_MONTHLY_CREDITS);
    assertEquals(spec.getPriority(), 5);
    assertEquals(spec.getDisplayName(), TEST_DISPLAY_NAME);
  }

  @Test
  public void testConstructorWithDefaultPriority() {
    // Test constructor without priority (should default to 1)
    RecurringCreditSpec spec =
        new RecurringCreditSpec(
            TEST_PRODUCT_ID, TEST_CREDIT_TYPE_ID, TEST_MONTHLY_CREDITS, TEST_DISPLAY_NAME);

    assertEquals(spec.getProductId(), TEST_PRODUCT_ID);
    assertEquals(spec.getCreditTypeId(), TEST_CREDIT_TYPE_ID);
    assertEquals(spec.getMonthlyCredits(), TEST_MONTHLY_CREDITS);
    assertEquals(spec.getPriority(), 1); // Default priority
    assertEquals(spec.getDisplayName(), TEST_DISPLAY_NAME);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithNullProductId() {
    // Should throw IllegalArgumentException when productId is null
    new RecurringCreditSpec(null, TEST_CREDIT_TYPE_ID, TEST_MONTHLY_CREDITS, 1, TEST_DISPLAY_NAME);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithEmptyCreditTypeId() {
    // Should throw IllegalArgumentException when creditTypeId is empty
    new RecurringCreditSpec(TEST_PRODUCT_ID, "", TEST_MONTHLY_CREDITS, 1, TEST_DISPLAY_NAME);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testConstructorWithZeroMonthlyCredits() {
    // Should throw IllegalArgumentException when monthlyCredits is zero
    new RecurringCreditSpec(TEST_PRODUCT_ID, TEST_CREDIT_TYPE_ID, 0, 1, TEST_DISPLAY_NAME);
  }

  @Test
  public void testImmutability() {
    // Test that RecurringCreditSpec is immutable (Lombok @Value)
    RecurringCreditSpec spec =
        new RecurringCreditSpec(
            TEST_PRODUCT_ID, TEST_CREDIT_TYPE_ID, TEST_MONTHLY_CREDITS, 1, TEST_DISPLAY_NAME);

    // All getters should return the same values
    assertEquals(spec.getProductId(), TEST_PRODUCT_ID);
    assertEquals(spec.getCreditTypeId(), TEST_CREDIT_TYPE_ID);
    assertEquals(spec.getMonthlyCredits(), TEST_MONTHLY_CREDITS);
  }
}
