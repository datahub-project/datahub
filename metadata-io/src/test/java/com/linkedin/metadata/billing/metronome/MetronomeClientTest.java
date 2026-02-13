package com.linkedin.metadata.billing.metronome;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.billing.BillingException;
import com.linkedin.metadata.config.BillingConfiguration;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
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
  private static final String TEST_PACKAGE_ALIAS = "standard";

  @BeforeMethod
  public void setUp() {
    mockHttpClient = mock(CloseableHttpClient.class);
    mockConfig = mock(BillingConfiguration.MetronomeConfiguration.class);
    when(mockConfig.getBaseUrl()).thenReturn(TEST_BASE_URL);
    when(mockConfig.getApiKey()).thenReturn(TEST_API_KEY);
  }

  @Test
  public void testConstructorWithValidParameters() {
    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    assertNotNull(metronomeClient);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testConstructorWithNullConfig() {
    new MetronomeClient(mockHttpClient, null);
  }

  @Test
  public void testProvisionCustomerSuccess() throws Exception {
    // Mock GET request for checking if customer exists (returns empty list)
    String getCustomerResponse = "{\"data\": []}";
    CloseableHttpResponse getCustomerHttpResponse =
        mockResponse(HttpStatus.SC_OK, getCustomerResponse);

    // Mock successful customer creation response
    String customerResponse = "{\"data\": {\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}}";
    CloseableHttpResponse customerHttpResponse = mockResponse(HttpStatus.SC_OK, customerResponse);

    // Mock successful contract creation response
    String contractResponse = "{\"data\": {\"id\": \"contract-123\"}}";
    CloseableHttpResponse contractHttpResponse = mockResponse(HttpStatus.SC_OK, contractResponse);

    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(getCustomerHttpResponse);
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenReturn(customerHttpResponse)
        .thenReturn(contractHttpResponse);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    String result = metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, TEST_PACKAGE_ALIAS);

    assertEquals(result, TEST_METRONOME_CUSTOMER_ID);
    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
    verify(mockHttpClient, times(2)).execute(any(HttpPost.class)); // customer + contract
  }

  @Test
  public void testProvisionCustomerExistingCustomer() throws Exception {
    // Customer already exists
    String getCustomerResponse = "{\"data\": [{\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}]}";
    CloseableHttpResponse getCustomerHttpResponse =
        mockResponse(HttpStatus.SC_OK, getCustomerResponse);

    // Mock contract creation response
    String contractResponse = "{\"data\": {\"id\": \"contract-123\"}}";
    CloseableHttpResponse contractHttpResponse = mockResponse(HttpStatus.SC_OK, contractResponse);

    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(getCustomerHttpResponse);
    when(mockHttpClient.execute(any(HttpPost.class))).thenReturn(contractHttpResponse);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);

    String result = metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, TEST_PACKAGE_ALIAS);

    assertEquals(result, TEST_METRONOME_CUSTOMER_ID);
    verify(mockHttpClient, times(1)).execute(any(HttpGet.class));
    verify(mockHttpClient, times(1)).execute(any(HttpPost.class)); // only contract, no customer
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testProvisionCustomerWithNullCustomerName() throws Exception {
    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    metronomeClient.provisionCustomer(null, TEST_PACKAGE_ALIAS);
  }

  @Test(expectedExceptions = NullPointerException.class)
  public void testProvisionCustomerWithNullPackageAlias() throws Exception {
    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, null);
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerWithMissingData() throws Exception {
    // Response missing 'data' field
    String invalidResponse = "{\"status\": \"success\"}";
    mockHttpResponse(HttpStatus.SC_OK, invalidResponse);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, TEST_PACKAGE_ALIAS);
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerWithHttpError() throws Exception {
    String errorResponse = "{\"error\": \"Invalid API key\"}";
    mockHttpResponse(HttpStatus.SC_UNAUTHORIZED, errorResponse);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, TEST_PACKAGE_ALIAS);
  }

  @Test(expectedExceptions = BillingException.class)
  public void testProvisionCustomerWithContractFailure() throws Exception {
    // Customer creation succeeds, but contract creation fails
    String getCustomerResponse = "{\"data\": []}";
    CloseableHttpResponse getCustomerHttpResponse =
        mockResponse(HttpStatus.SC_OK, getCustomerResponse);

    String customerResponse = "{\"data\": {\"id\": \"" + TEST_METRONOME_CUSTOMER_ID + "\"}}";
    CloseableHttpResponse customerHttpResponse = mockResponse(HttpStatus.SC_OK, customerResponse);

    String errorResponse = "{\"error\": \"Invalid package\"}";
    CloseableHttpResponse errorHttpResponse =
        mockResponse(HttpStatus.SC_BAD_REQUEST, errorResponse);

    when(mockHttpClient.execute(any(HttpGet.class))).thenReturn(getCustomerHttpResponse);
    when(mockHttpClient.execute(any(HttpPost.class)))
        .thenReturn(customerHttpResponse)
        .thenReturn(errorHttpResponse);

    metronomeClient = new MetronomeClient(mockHttpClient, mockConfig);
    metronomeClient.provisionCustomer(TEST_CUSTOMER_NAME, TEST_PACKAGE_ALIAS);
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
}
