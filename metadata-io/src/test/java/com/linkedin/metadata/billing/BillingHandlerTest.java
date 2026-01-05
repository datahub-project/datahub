package com.linkedin.metadata.billing;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.billing.contract.ContractSpec;
import com.linkedin.metadata.billing.contract.RecurringCreditSpec;
import com.linkedin.metadata.billing.metronome.MetronomeClient;
import com.linkedin.metadata.config.BillingConfiguration;
import java.time.LocalDate;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for BillingHandler. */
public class BillingHandlerTest {

  private BillingConfiguration mockConfig;
  private BillingProvider mockProvider;
  private MetronomeClient mockMetronomeClient;
  private BillingHandler billingHandler;
  private ContractSpec mockContract;
  private ContractSpec mockContract2;

  private static final String TEST_CUSTOMER_NAME = "test.datahub.com";
  private static final String TEST_PROVIDER_CUSTOMER_ID = "test-customer-123";

  @BeforeMethod
  public void setUp() {
    mockConfig = mock(BillingConfiguration.class);
    mockProvider = mock(BillingProvider.class);
    mockMetronomeClient = mock(MetronomeClient.class);

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
    // Test that constructor properly initializes with valid parameters
    when(mockConfig.isEnabled()).thenReturn(true);

    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    assertNotNull(billingHandler);
    assertTrue(billingHandler.isEnabled());
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullConfig() {
    // Should throw NullPointerException when config is null
    new BillingHandler(null, mockProvider, TEST_CUSTOMER_NAME);
  }

  @Test
  public void testIsEnabled() {
    when(mockConfig.isEnabled()).thenReturn(true);

    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    assertTrue(billingHandler.isEnabled());
  }

  @Test
  public void testProvisionCustomerWhenDisabled() throws Exception {
    // Setup: Billing disabled
    when(mockConfig.isEnabled()).thenReturn(false);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Execute: Attempt to provision
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Verify: Provider was never called
    verify(mockProvider, never()).provisionCustomer(anyString(), anyList());
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerProviderFailure() throws Exception {
    // Setup: Provider throws exception
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenThrow(new BillingException("Provider API error"));
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Execute: Should throw BillingException
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));
  }

  @Test
  public void testGetProviderCustomerIdFromCache() throws Exception {
    // Setup: Provision customer first to populate cache
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Execute: Get customer ID (should come from cache)
    String customerId = billingHandler.getProviderCustomerId();

    // Verify: Returns cached ID, no call to Metronome API
    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockMetronomeClient, never()).getCustomerByIngestAlias(anyString());
  }

  @Test
  public void testGetProviderCustomerIdFromMetronomeApi() throws Exception {
    // Setup: Customer exists in Metronome - use MetronomeClient as provider
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockMetronomeClient.getCustomerByIngestAlias(TEST_CUSTOMER_NAME))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(mockConfig, mockMetronomeClient, TEST_CUSTOMER_NAME);

    // Execute: Get customer ID (should query Metronome)
    String customerId = billingHandler.getProviderCustomerId();

    // Verify: Returns ID from Metronome API
    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockMetronomeClient, times(1)).getCustomerByIngestAlias(TEST_CUSTOMER_NAME);
  }

  @Test
  public void testProvisionCustomerIdempotency() throws Exception {
    // Setup: Enable billing - idempotency is now handled inside MetronomeClient
    when(mockConfig.isEnabled()).thenReturn(true);

    // Mock MetronomeClient.provisionCustomer to return customer ID
    when(mockMetronomeClient.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);

    billingHandler = new BillingHandler(mockConfig, mockMetronomeClient, TEST_CUSTOMER_NAME);

    // Execute: Provision twice
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Verify: BillingHandler calls MetronomeClient.provisionCustomer twice
    // (idempotency is now handled inside MetronomeClient, not in BillingHandler)
    verify(mockMetronomeClient, times(2)).provisionCustomer(anyString(), anyList());
  }

  @Test(expectedExceptions = BillingException.class)
  public void testGetProviderCustomerIdNotFoundInMetronome() throws Exception {
    // Setup: Customer not found in Metronome - use MetronomeClient as provider
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockMetronomeClient.getCustomerByIngestAlias(TEST_CUSTOMER_NAME)).thenReturn(null);
    billingHandler = new BillingHandler(mockConfig, mockMetronomeClient, TEST_CUSTOMER_NAME);

    // Execute: Should throw BillingException
    billingHandler.getProviderCustomerId();
  }

  @Test(expectedExceptions = BillingException.class)
  public void testGetProviderCustomerIdMetronomeApiFailure() throws Exception {
    // Setup: Metronome API throws exception - use MetronomeClient as provider
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockMetronomeClient.getCustomerByIngestAlias(TEST_CUSTOMER_NAME))
        .thenThrow(new BillingException("API connection failed"));
    billingHandler = new BillingHandler(mockConfig, mockMetronomeClient, TEST_CUSTOMER_NAME);

    // Execute: Should propagate BillingException
    billingHandler.getProviderCustomerId();
  }

  @Test
  public void testProvisionThenGetCustomerId() throws Exception {
    // Setup: Test the full flow of provisioning then getting the ID
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockMetronomeClient.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(mockConfig, mockMetronomeClient, TEST_CUSTOMER_NAME);

    // Execute: Provision first, then get ID
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));
    String customerId = billingHandler.getProviderCustomerId();

    // Verify: ID comes from cache set during provisioning
    assertEquals(customerId, TEST_PROVIDER_CUSTOMER_ID);
    verify(mockMetronomeClient, times(1)).provisionCustomer(eq(TEST_CUSTOMER_NAME), anyList());
    // Note: getCustomerByIngestAlias is called inside provisionCustomer's implementation,
    // but we can't verify it here since provisionCustomer is mocked (real implementation doesn't
    // run)
  }

  @Test
  public void testProvisionCustomerWithMultipleContracts() throws Exception {
    // Setup: Enable billing with multiple contracts
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    List<ContractSpec> contracts = Arrays.asList(mockContract, mockContract2);

    // Execute: Provision with multiple contracts
    billingHandler.provisionCustomer(contracts);

    // Verify: Provider called with all contracts
    verify(mockProvider, times(1)).provisionCustomer(eq(TEST_CUSTOMER_NAME), eq(contracts));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testProvisionCustomerWithEmptyContractList() throws Exception {
    // Setup: Enable billing
    when(mockConfig.isEnabled()).thenReturn(true);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Execute: Should throw IllegalArgumentException for empty list
    billingHandler.provisionCustomer(Collections.emptyList());
  }

  @Test
  public void testReportUsageSuccess() throws Exception {
    // Setup: Enable billing
    when(mockConfig.isEnabled()).thenReturn(true);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Test data
    String eventType = "ai_message";
    String transactionId = "txn-12345";
    int quantity = 1;
    java.util.Map<String, Object> properties = new java.util.HashMap<>();
    properties.put("conversation_id", "conv-123");
    properties.put("user_id", "user-456");

    // Execute: Report usage
    billingHandler.reportUsage(eventType, transactionId, quantity, properties);

    // Verify: Provider's reportUsage was called with correct parameters
    verify(mockProvider, times(1))
        .reportUsage(
            eq(TEST_CUSTOMER_NAME), eq(eventType), eq(transactionId), eq(quantity), eq(properties));
  }

  @Test
  public void testReportUsageWhenDisabled() throws Exception {
    // Setup: Billing disabled
    when(mockConfig.isEnabled()).thenReturn(false);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Test data
    String eventType = "ai_message";
    String transactionId = "txn-12345";
    int quantity = 1;
    java.util.Map<String, Object> properties = new java.util.HashMap<>();

    // Execute: Report usage
    billingHandler.reportUsage(eventType, transactionId, quantity, properties);

    // Verify: Provider was never called
    verify(mockProvider, never())
        .reportUsage(anyString(), anyString(), anyString(), anyInt(), anyMap());
  }

  @Test
  public void testReportUsageProviderFailure() throws Exception {
    // Setup: Provider throws exception
    when(mockConfig.isEnabled()).thenReturn(true);
    doThrow(new BillingException("Provider API error"))
        .when(mockProvider)
        .reportUsage(anyString(), anyString(), anyString(), anyInt(), anyMap());
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Test data
    String eventType = "ai_message";
    String transactionId = "txn-12345";
    int quantity = 1;
    java.util.Map<String, Object> properties = new java.util.HashMap<>();

    // Execute: Should NOT throw - errors are logged but not propagated
    billingHandler.reportUsage(eventType, transactionId, quantity, properties);

    // Verify: Provider was called despite failure
    verify(mockProvider, times(1))
        .reportUsage(
            eq(TEST_CUSTOMER_NAME), eq(eventType), eq(transactionId), eq(quantity), eq(properties));
  }

  @Test
  public void testHasRemainingCreditsSuccess() throws Exception {
    // Setup: Enable billing and provision customer
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    when(mockProvider.hasRemainingCredits(eq(TEST_PROVIDER_CUSTOMER_ID), eq("product-123")))
        .thenReturn(true);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Execute: Check if credits remain
    boolean hasCredits = billingHandler.hasRemainingCredits("product-123");

    // Verify: Returns true
    assertTrue(hasCredits);
    verify(mockProvider, times(1))
        .hasRemainingCredits(eq(TEST_PROVIDER_CUSTOMER_ID), eq("product-123"));
  }

  @Test
  public void testHasRemainingCreditsWhenDisabled() throws Exception {
    // Setup: Billing disabled
    when(mockConfig.isEnabled()).thenReturn(false);
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Execute: Check if credits remain
    boolean hasCredits = billingHandler.hasRemainingCredits("product-123");

    // Verify: Returns true (fails open) and provider was never called
    assertTrue(hasCredits);
    verify(mockProvider, never()).hasRemainingCredits(anyString(), anyString());
  }

  @Test
  public void testHasRemainingCreditsProviderFailure() throws Exception {
    // Setup: Provider throws exception
    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockProvider.provisionCustomer(anyString(), anyList()))
        .thenReturn(TEST_PROVIDER_CUSTOMER_ID);
    when(mockProvider.hasRemainingCredits(anyString(), anyString()))
        .thenThrow(new BillingException("Provider API error"));
    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);
    billingHandler.provisionCustomer(Collections.singletonList(mockContract));

    // Execute: Check if credits remain (should fail open)
    boolean hasCredits = billingHandler.hasRemainingCredits("product-123");

    // Verify: Returns true (fails open) despite error
    assertTrue(hasCredits);
  }

  @Test
  public void testGetProductIdByName() throws Exception {
    // Setup: Create config with named product
    BillingConfiguration.MetronomeConfiguration mockMetronomeConfig =
        mock(BillingConfiguration.MetronomeConfiguration.class);
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration mockContractConfig =
        mock(BillingConfiguration.MetronomeConfiguration.ContractConfiguration.class);
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
        mockRecurringCredit =
            mock(
                BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
                    .class);

    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockConfig.getMetronome()).thenReturn(mockMetronomeConfig);
    when(mockMetronomeConfig.getContracts())
        .thenReturn(Collections.singletonMap("freeTrial", mockContractConfig));
    when(mockContractConfig.getRecurringCredits())
        .thenReturn(Collections.singletonList(mockRecurringCredit));
    when(mockRecurringCredit.getProductName()).thenReturn("askDataHub");
    when(mockRecurringCredit.getProductId()).thenReturn("ask_datahub_product");

    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Execute: Get product ID by name
    String productId = billingHandler.getProductId("freeTrial", "askDataHub");

    // Verify: Returns correct product ID
    assertEquals(productId, "ask_datahub_product");
  }

  @Test
  public void testGetProductIdByNameNotFound() throws Exception {
    // Setup: Create config with different product name
    BillingConfiguration.MetronomeConfiguration mockMetronomeConfig =
        mock(BillingConfiguration.MetronomeConfiguration.class);
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration mockContractConfig =
        mock(BillingConfiguration.MetronomeConfiguration.ContractConfiguration.class);
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
        mockRecurringCredit =
            mock(
                BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
                    .class);

    when(mockConfig.isEnabled()).thenReturn(true);
    when(mockConfig.getMetronome()).thenReturn(mockMetronomeConfig);
    when(mockMetronomeConfig.getContracts())
        .thenReturn(Collections.singletonMap("freeTrial", mockContractConfig));
    when(mockContractConfig.getRecurringCredits())
        .thenReturn(Collections.singletonList(mockRecurringCredit));
    when(mockRecurringCredit.getProductName()).thenReturn("someOtherProduct");

    billingHandler = new BillingHandler(mockConfig, mockProvider, TEST_CUSTOMER_NAME);

    // Execute: Get product ID by name that doesn't exist
    String productId = billingHandler.getProductId("freeTrial", "askDataHub");

    // Verify: Returns null
    assertNull(productId);
  }
}
