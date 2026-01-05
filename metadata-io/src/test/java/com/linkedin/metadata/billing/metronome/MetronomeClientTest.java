package com.linkedin.metadata.billing.metronome;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.billing.BillingException;
import com.linkedin.metadata.billing.contract.ContractSpec;
import com.linkedin.metadata.billing.contract.FreeTrialContractSpec;
import com.linkedin.metadata.config.BillingConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/** Unit tests for MetronomeClient. */
public class MetronomeClientTest {

  private CloseableHttpClient mockHttpClient;
  private BillingConfiguration.MetronomeConfiguration mockConfig;
  private MetronomeClient metronomeClient;

  private static final String TEST_BASE_URL = "https://api.metronome.com";
  private static final String TEST_API_KEY = "test-api-key-12345";
  private static final String TEST_CUSTOMER_NAME = "test.datahub.com";
  private static final String TEST_METRONOME_CUSTOMER_ID = "metronome-cust-789";
  private static final String TEST_RATE_CARD_ID = "rate-card-123";
  private static final String TEST_PRODUCT_ID = "product-456";
  private static final String TEST_CREDIT_TYPE_ID = "credit-type-789";

  @BeforeMethod
  public void setUp() {
    mockHttpClient = mock(CloseableHttpClient.class);
    mockConfig = mock(BillingConfiguration.MetronomeConfiguration.class);
    when(mockConfig.getBaseUrl()).thenReturn(TEST_BASE_URL);
    when(mockConfig.getApiKey()).thenReturn(TEST_API_KEY);
  }

  @Test
  public void testConstructorWithValidParameters() {
    // Test that constructor initializes correctly
    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    assertNotNull(metronomeClient);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullConfig() {
    // Should throw NullPointerException when config is null
    new MetronomeClient(mockHttpClient, null);
  }

  @Test
  public void testProvisionCustomerSuccess() throws Exception {
    // Setup: Mock GET request for checking if customer exists (returns empty list)
    String getCustomerResponse = "{\"data\": []}";
    CloseableHttpResponse getCustomerHttpResponse =
        mockResponse(HttpStatus.SC_OK, getCustomerResponse);

    // Setup: Mock successful customer creation response
    String customerResponse = "{\"data\": {\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}}";
    CloseableHttpResponse customerHttpResponse = mockResponse(HttpStatus.SC_OK, customerResponse);

    // Setup: Mock successful contract creation response
    String contractResponse = "{\"data\": {\"id\": \"contract-123\"}}";
    CloseableHttpResponse contractHttpResponse = mockResponse(HttpStatus.SC_OK, contractResponse);

    // Setup: Chain the responses - GET to check existence, then POSTs for customer + contract
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(getCustomerHttpResponse);
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenReturn(customerHttpResponse)
        .thenReturn(contractHttpResponse);

    // Setup: Create contract spec
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        createMockContractConfig();
    ContractSpec contract = new FreeTrialContractSpec(contractConfig);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Provision customer with contract
    String result =
        metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Collections.singletonList(contract));

    // Verify: Returns Metronome customer ID
    assertEquals(result, TEST_METRONOME_CUSTOMER_ID);

    // Verify: HTTP client was called once for GET and twice for POSTs
    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
    verify(mockHttpClient, times(2)).execute(any(HttpPost.class));
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testProvisionCustomerWithNullCustomerName() throws Exception {
    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        createMockContractConfig();
    ContractSpec contract = new FreeTrialContractSpec(contractConfig);

    // Should throw NullPointerException when customerName is null
    metronomeClient.provisionCustomer(null, Collections.singletonList(contract));
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerWithMissingData() throws Exception {
    // Setup: Response missing 'data' field
    String invalidResponse = "{\"status\": \"success\"}";
    mockHttpResponse(HttpStatus.SC_OK, invalidResponse);

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        createMockContractConfig();
    ContractSpec contract = new FreeTrialContractSpec(contractConfig);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Should throw BillingException
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Collections.singletonList(contract));
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerWithHttpError() throws Exception {
    // Setup: Mock HTTP error response
    String errorResponse = "{\"error\": \"Invalid API key\"}";
    mockHttpResponse(HttpStatus.SC_UNAUTHORIZED, errorResponse);

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        createMockContractConfig();
    ContractSpec contract = new FreeTrialContractSpec(contractConfig);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Should throw BillingException
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Collections.singletonList(contract));
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerWithContractFailure() throws Exception {
    // Setup: Customer creation succeeds, but contract creation fails
    String customerResponse = "{\"data\": {\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}}";
    CloseableHttpResponse successResponse = mockResponse(HttpStatus.SC_OK, customerResponse);

    String errorResponse = "{\"error\": \"Invalid rate card\"}";
    CloseableHttpResponse errorResponse2 = mockResponse(HttpStatus.SC_BAD_REQUEST, errorResponse);

    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenReturn(successResponse)
        .thenReturn(errorResponse2);

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        createMockContractConfig();
    ContractSpec contract = new FreeTrialContractSpec(contractConfig);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Should throw BillingException from contract creation
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Collections.singletonList(contract));
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testProvisionCustomerWithEmptyContractList() throws Exception {
    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Should throw IllegalArgumentException for empty contract list
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Collections.emptyList());
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void testProvisionCustomerWithInvalidContractSpec() throws Exception {
    // Setup: Empty recurring credits
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        mock(BillingConfiguration.MetronomeConfiguration.ContractConfiguration.class);
    when(contractConfig.getRateCardId()).thenReturn(TEST_RATE_CARD_ID);
    when(contractConfig.getRecurringCredits()).thenReturn(Arrays.asList());

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Should throw IllegalArgumentException when creating contract with empty credits
    new FreeTrialContractSpec(contractConfig);
  }

  @Test
  public void testProvisionCustomerWithMultipleRecurringCredits() throws Exception {
    // Setup: Mock GET request for checking if customer exists (returns empty list)
    String getCustomerResponse = "{\"data\": []}";
    CloseableHttpResponse getCustomerHttpResponse =
        mockResponse(HttpStatus.SC_OK, getCustomerResponse);

    // Setup: Mock customer creation
    String customerResponse = "{\"data\": {\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}}";
    CloseableHttpResponse customerHttpResponse = mockResponse(HttpStatus.SC_OK, customerResponse);

    // Setup: Mock contract creation
    String contractResponse = "{\"data\": {\"id\": \"contract-123\"}}";
    CloseableHttpResponse contractHttpResponse = mockResponse(HttpStatus.SC_OK, contractResponse);

    // Setup: Chain the responses
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(getCustomerHttpResponse);
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenReturn(customerHttpResponse)
        .thenReturn(contractHttpResponse);

    // Setup: Multiple recurring credits
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit credit1 =
        createMockRecurringCredit("product-1", "credit-type-1", 1000);
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit credit2 =
        createMockRecurringCredit("product-2", "credit-type-2", 2000);

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        mock(BillingConfiguration.MetronomeConfiguration.ContractConfiguration.class);
    when(contractConfig.getRateCardId()).thenReturn(TEST_RATE_CARD_ID);
    when(contractConfig.getRecurringCredits()).thenReturn(Arrays.asList(credit1, credit2));

    ContractSpec contract = new FreeTrialContractSpec(contractConfig);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Provision customer with contract having multiple credits
    String result =
        metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Collections.singletonList(contract));

    // Verify: Success with multiple credits
    assertEquals(result, TEST_METRONOME_CUSTOMER_ID);
    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
    verify(mockHttpClient, times(2)).execute(any(HttpPost.class));
  }

  @Test
  public void testProvisionCustomerWithMultipleContracts() throws Exception {
    // Setup: Mock GET request for checking if customer exists (returns empty list)
    String getCustomerResponse = "{\"data\": []}";
    CloseableHttpResponse getCustomerHttpResponse =
        mockResponse(HttpStatus.SC_OK, getCustomerResponse);

    // Setup: Mock customer creation
    String customerResponse = "{\"data\": {\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}}";
    CloseableHttpResponse customerHttpResponse = mockResponse(HttpStatus.SC_OK, customerResponse);

    // Setup: Mock multiple contract creations
    String contractResponse1 = "{\"data\": {\"id\": \"contract-123\"}}";
    String contractResponse2 = "{\"data\": {\"id\": \"contract-456\"}}";
    CloseableHttpResponse contractHttpResponse1 = mockResponse(HttpStatus.SC_OK, contractResponse1);
    CloseableHttpResponse contractHttpResponse2 = mockResponse(HttpStatus.SC_OK, contractResponse2);

    // Setup: Chain the responses (customer + 2 contracts)
    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(getCustomerHttpResponse);
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenReturn(customerHttpResponse)
        .thenReturn(contractHttpResponse1)
        .thenReturn(contractHttpResponse2);

    // Setup: Create two different contracts
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration config1 =
        createMockContractConfig();
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration config2 =
        createMockContractConfig();

    ContractSpec contract1 = new FreeTrialContractSpec(config1, "Contract 1");
    ContractSpec contract2 = new FreeTrialContractSpec(config2, "Contract 2");

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    // Execute: Provision customer with multiple contracts
    String result =
        metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, Arrays.asList(contract1, contract2));

    // Verify: Success with both contracts created
    assertEquals(result, TEST_METRONOME_CUSTOMER_ID);
    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
    verify(mockHttpClient, times(3)).execute(any(HttpPost.class)); // 1 customer + 2 contracts
  }

  // Helper methods

  private void mockHttpResponse(int statusCode, String responseBody) throws IOException {
    CloseableHttpResponse response = mockResponse(statusCode, responseBody);
    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(response);
  }

  private CloseableHttpResponse mockResponse(int statusCode, String responseBody)
      throws IOException {
    CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    StatusLine statusLine = mock(StatusLine.class);
    when(statusLine.getStatusCode()).thenReturn(statusCode);
    when(response.getStatusLine()).thenReturn(statusLine);

    if (responseBody != null) {
      HttpEntity entity = mock(HttpEntity.class);
      when(entity.getContent())
          .thenReturn(new ByteArrayInputStream(responseBody.getBytes(StandardCharsets.UTF_8)));
      when(response.getEntity()).thenReturn(entity);
    }

    return response;
  }

  private BillingConfiguration.MetronomeConfiguration.ContractConfiguration
      createMockContractConfig() {
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit credit =
        createMockRecurringCredit(TEST_PRODUCT_ID, TEST_CREDIT_TYPE_ID, 1000);

    BillingConfiguration.MetronomeConfiguration.ContractConfiguration contractConfig =
        mock(BillingConfiguration.MetronomeConfiguration.ContractConfiguration.class);
    when(contractConfig.getRateCardId()).thenReturn(TEST_RATE_CARD_ID);
    when(contractConfig.getRecurringCredits()).thenReturn(Arrays.asList(credit));

    return contractConfig;
  }

  private BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
      createMockRecurringCredit(String productId, String creditTypeId, int monthlyCredits) {
    BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit credit =
        mock(
            BillingConfiguration.MetronomeConfiguration.ContractConfiguration.RecurringCredit
                .class);
    when(credit.getProductId()).thenReturn(productId);
    when(credit.getCreditTypeId()).thenReturn(creditTypeId);
    when(credit.getMonthlyCredits()).thenReturn(monthlyCredits);
    return credit;
  }
}
