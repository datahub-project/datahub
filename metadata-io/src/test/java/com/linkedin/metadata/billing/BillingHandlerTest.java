package com.linkedin.metadata.billing;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for BillingHandler. */
public class BillingHandlerTest {

  private BillingProvider mockProvider;
  private BillingHandler billingHandler;

  private static final String TEST_CUSTOMER_NAME = "test.datahub.com";
  private static final String TEST_PROVIDER_CUSTOMER_ID = "test-customer-123";

  @BeforeMethod
  public void setUp() throws Exception {
    mockProvider = mock(BillingProvider.class);
  }

  @Test
  public void testConstructorWithValidParameters() {
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    assertNotNull(billingHandler);
    assertTrue(billingHandler.isEnabled());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullProvider() {
    new BillingHandler(true, null, TEST_CUSTOMER_NAME);
  }

  @Test
  public void testIsEnabled() {
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);
    assertTrue(billingHandler.isEnabled());
  }

  @Test
  public void testIsDisabled() {
    billingHandler = new BillingHandler(false, mockProvider, TEST_CUSTOMER_NAME);
    assertFalse(billingHandler.isEnabled());
  }

  @Test
  public void testProvisionCustomerWhenDisabled() throws Exception {
    billingHandler = new BillingHandler(false, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer();

    verify(mockProvider, never()).provisionCustomer(anyString());
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerProviderFailure() throws Exception {
    when(mockProvider.provisionCustomer(anyString()))
        .thenThrow(new BillingException("Provider API error"));
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer();
  }

  @Test
  public void testGetProviderCustomerIdFromCache() throws Exception {
    // Provision customer first to populate cache
    when(mockProvider.provisionCustomer(anyString())).thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer();

    String customerId = billingHandler.getProviderCustomerId();

    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockProvider, never()).getCustomerId(anyString());
  }

  @Test
  public void testGetProviderCustomerIdFromProviderApi() throws Exception {
    when(mockProvider.getCustomerId(TEST_CUSTOMER_NAME)).thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    String customerId = billingHandler.getProviderCustomerId();

    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockProvider, times(1)).getCustomerId(TEST_CUSTOMER_NAME);
  }

  @Test
  public void testProvisionCustomerIdempotency() throws Exception {
    when(mockProvider.provisionCustomer(anyString())).thenReturn(TEST_PROVIDER_CUSTOMER_ID);

    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer();
    billingHandler.provisionCustomer();

    verify(mockProvider, times(2)).provisionCustomer(anyString());
  }

  @Test(expectedExceptions = BillingException.class)
  public void testGetProviderCustomerIdNotFound() throws Exception {
    when(mockProvider.getCustomerId(TEST_CUSTOMER_NAME)).thenReturn(null);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.getProviderCustomerId();
  }

  @Test(expectedExceptions = BillingException.class)
  public void testGetProviderCustomerIdApiFailure() throws Exception {
    when(mockProvider.getCustomerId(TEST_CUSTOMER_NAME))
        .thenThrow(new BillingException("API connection failed"));
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.getProviderCustomerId();
  }

  @Test
  public void testProvisionThenGetCustomerId() throws Exception {
    when(mockProvider.provisionCustomer(anyString())).thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer();
    String customerId = billingHandler.getProviderCustomerId();

    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockProvider, times(1)).provisionCustomer(eq(TEST_CUSTOMER_NAME));
  }

  @Test
  public void testReportUsageSuccess() throws Exception {
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    String eventType = "ai_message";
    String transactionId = "txn-12345";
    int quantity = 1;
    java.util.Map<String, Object> properties = new java.util.HashMap<>();
    properties.put("conversation_id", "conv-123");
    properties.put("user_id", "user-456");

    billingHandler.reportUsage(eventType, transactionId, quantity, properties);

    verify(mockProvider, times(1))
        .reportUsage(
            eq(TEST_CUSTOMER_NAME), eq(eventType), eq(transactionId), eq(quantity), eq(properties));
  }

  @Test
  public void testReportUsageWhenDisabled() throws Exception {
    billingHandler = new BillingHandler(false, mockProvider, TEST_CUSTOMER_NAME);

    String eventType = "ai_message";
    String transactionId = "txn-12345";
    int quantity = 1;
    java.util.Map<String, Object> properties = new java.util.HashMap<>();

    billingHandler.reportUsage(eventType, transactionId, quantity, properties);

    verify(mockProvider, never())
        .reportUsage(anyString(), anyString(), anyString(), anyInt(), anyMap());
  }

  @Test
  public void testReportUsageProviderFailure() throws Exception {
    doThrow(new BillingException("Provider API error"))
        .when(mockProvider)
        .reportUsage(anyString(), anyString(), anyString(), anyInt(), anyMap());
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    String eventType = "ai_message";
    String transactionId = "txn-12345";
    int quantity = 1;
    java.util.Map<String, Object> properties = new java.util.HashMap<>();

    // Should NOT throw - errors are logged but not propagated
    billingHandler.reportUsage(eventType, transactionId, quantity, properties);

    verify(mockProvider, times(1))
        .reportUsage(
            eq(TEST_CUSTOMER_NAME), eq(eventType), eq(transactionId), eq(quantity), eq(properties));
  }

  @Test
  public void testHasRemainingCreditsWithBillingProduct() throws Exception {
    when(mockProvider.provisionCustomer(anyString())).thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    when(mockProvider.hasRemainingCredits(
            eq(TEST_PROVIDER_CUSTOMER_ID), eq(BillingProduct.ASK_DATAHUB)))
        .thenReturn(true);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);
    billingHandler.provisionCustomer();

    boolean hasCredits = billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB);

    assertTrue(hasCredits);
    verify(mockProvider, times(1))
        .hasRemainingCredits(eq(TEST_PROVIDER_CUSTOMER_ID), eq(BillingProduct.ASK_DATAHUB));
  }

  @Test
  public void testHasRemainingCreditsWhenDisabled() throws Exception {
    billingHandler = new BillingHandler(false, mockProvider, TEST_CUSTOMER_NAME);

    boolean hasCredits = billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB);

    assertTrue(hasCredits);
    verify(mockProvider, never()).hasRemainingCredits(anyString(), any(BillingProduct.class));
  }

  @Test
  public void testHasRemainingCreditsProviderFailure() throws Exception {
    when(mockProvider.provisionCustomer(anyString())).thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    when(mockProvider.hasRemainingCredits(anyString(), any(BillingProduct.class)))
        .thenThrow(new BillingException("Provider API error"));
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);
    billingHandler.provisionCustomer();

    // Should fail open
    boolean hasCredits = billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB);

    assertTrue(hasCredits);
  }
}
