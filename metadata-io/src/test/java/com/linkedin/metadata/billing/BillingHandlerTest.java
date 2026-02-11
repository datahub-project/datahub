package com.linkedin.metadata.billing;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.billing.contract.ContractSpec;
import com.linkedin.metadata.billing.contract.RecurringCreditSpec;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for BillingHandler. */
public class BillingHandlerTest {

  private BillingProvider mockProvider;
  private BillingHandler billingHandler;
  private ContractSpec mockContract;
  private ContractSpec mockContract2;

  private static final String TEST_CUSTOMER_NAME = "test.datahub.com";
  private static final String TEST_PROVIDER_CUSTOMER_ID = "test-customer-123";
  private static final String TEST_ASK_DATAHUB_PRODUCT_ID = "fccd322e-7119-4a2b-a7ee-95f22ecb800e";

  @BeforeMethod
  public void setUp() throws Exception {
    mockProvider = mock(BillingProvider.class);

    // Default: provider resolves ASK_DATAHUB to the test product ID
    when(mockProvider.resolveProductId(BillingProduct.ASK_DATAHUB))
        .thenReturn(TEST_ASK_DATAHUB_PRODUCT_ID);

    // Create mock contracts for testing
    mockContract = mock(ContractSpec.class);
    when(mockContract.getName()).thenReturn("Test Contract");
    when(mockContract.getRateCardId()).thenReturn("rc-test");
    when(mockContract.getStartDate()).thenReturn(LocalDate.now());
    when(mockContract.getRecurringCredits())
        .thenReturn(Collections.singletonList(new RecurringCreditSpec("prod-1", "credit-1", 1000)));

    mockContract2 = mock(ContractSpec.class);
    when(mockContract2.getName()).thenReturn("Test Contract 2");
    when(mockContract2.getRateCardId()).thenReturn("rc-test-2");
    when(mockContract2.getStartDate()).thenReturn(LocalDate.now());
    when(mockContract2.getRecurringCredits())
        .thenReturn(Collections.singletonList(new RecurringCreditSpec("prod-2", "credit-2", 2000)));
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
  public void testResolveProductIdDelegatesToProvider() throws Exception {
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    String productId = billingHandler.resolveProductId(BillingProduct.ASK_DATAHUB);

    assertEquals(productId, TEST_ASK_DATAHUB_PRODUCT_ID);
    verify(mockProvider, times(1)).resolveProductId(BillingProduct.ASK_DATAHUB);
  }

  @Test(expectedExceptions = BillingException.class)
  public void testResolveProductIdProviderThrows() throws Exception {
    when(mockProvider.resolveProductId(BillingProduct.ASK_DATAHUB))
        .thenThrow(new BillingException("No product ID configured"));
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.resolveProductId(BillingProduct.ASK_DATAHUB);
  }

  @Test
  public void testProvisionCustomerWhenDisabled() throws Exception {
    billingHandler = new BillingHandler(false, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    verify(mockProvider, never()).provisionCustomer(anyString(), anyList());
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerProviderFailure() throws Exception {
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenThrow(new BillingException("Provider API error"));
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.singletonList(mockContract));
  }

  @Test
  public void testGetProviderCustomerIdFromCache() throws Exception {
    // Provision customer first to populate cache
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    String customerId = billingHandler.getProviderCustomerId();

    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    // getCustomerId should not be called since cache is populated from provisioning
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
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);

    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.singletonList(mockContract));
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Idempotency is handled inside the provider, BillingHandler always delegates
    verify(mockProvider, times(2)).provisionCustomer(anyString(), anyList());
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
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.singletonList(mockContract));
    String customerId = billingHandler.getProviderCustomerId();

    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockProvider, times(1)).provisionCustomer(eq(TEST_CUSTOMER_NAME), anyList());
  }

  @Test
  public void testProvisionCustomerWithMultipleContracts() throws Exception {
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    List<ContractSpec> contracts = Arrays.asList(mockContract, mockContract2);

    billingHandler.provisionCustomer(contracts);

    verify(mockProvider, times(1)).provisionCustomer(eq(TEST_CUSTOMER_NAME), eq(contracts));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testProvisionCustomerWithEmptyContractList() throws Exception {
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.emptyList());
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
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    when(mockProvider.hasRemainingCredits(
            eq(TEST_PROVIDER_CUSTOMER_ID), eq(TEST_ASK_DATAHUB_PRODUCT_ID)))
        .thenReturn(true);
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    boolean hasCredits = billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB);

    assertTrue(hasCredits);
    verify(mockProvider, times(1)).resolveProductId(BillingProduct.ASK_DATAHUB);
    verify(mockProvider, times(1))
        .hasRemainingCredits(eq(TEST_PROVIDER_CUSTOMER_ID), eq(TEST_ASK_DATAHUB_PRODUCT_ID));
  }

  @Test
  public void testHasRemainingCreditsWhenDisabled() throws Exception {
    billingHandler = new BillingHandler(false, mockProvider, TEST_CUSTOMER_NAME);

    boolean hasCredits = billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB);

    assertTrue(hasCredits);
    verify(mockProvider, never()).resolveProductId(any());
    verify(mockProvider, never()).hasRemainingCredits(anyString(), anyString());
  }

  @Test
  public void testHasRemainingCreditsProviderFailure() throws Exception {
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    when(mockProvider.hasRemainingCredits(anyString(), anyString()))
        .thenThrow(new BillingException("Provider API error"));
    billingHandler = new BillingHandler(true, mockProvider, TEST_CUSTOMER_NAME);
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Should fail open
    boolean hasCredits = billingHandler.hasRemainingCredits(BillingProduct.ASK_DATAHUB);

    assertTrue(hasCredits);
  }
}
