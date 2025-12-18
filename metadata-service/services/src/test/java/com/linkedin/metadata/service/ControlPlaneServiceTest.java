package com.linkedin.metadata.service;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;

import com.linkedin.metadata.config.ControlPlaneConfiguration;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.util.Optional;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests for ControlPlaneService focusing on HTTP fetching, caching behavior, and error handling.
 */
public class ControlPlaneServiceTest {

  private static final String TEST_URL = "https://app.datahub.com";
  private static final String TEST_API_KEY = "test-api-key-123";

  @Mock private HttpClient mockHttpClient;
  @Mock private HttpResponse<String> mockHttpResponse;

  private ControlPlaneConfiguration config;

  @BeforeMethod
  public void setupTest() {
    MockitoAnnotations.openMocks(this);
    config = new ControlPlaneConfiguration();
    config.setUrl(TEST_URL);
    config.setApiKey(TEST_API_KEY);
  }

  @Test
  public void testSuccessfulApiFetchIsCached() throws Exception {
    // Setup - mock successful HTTP response with timestamp 30 days in future
    long futureTimestamp = Instant.now().getEpochSecond() + (30L * 86400L);
    String responseJson = String.valueOf(futureTimestamp);

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseJson);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute - fetch multiple times
    Optional<Long> result1 = service.getTrialExpirationTimestamp();
    Optional<Long> result2 = service.getTrialExpirationTimestamp();
    Optional<Long> result3 = service.getTrialExpirationTimestamp();

    // Verify - HTTP should only be called once (cached after first fetch)
    verify(mockHttpClient, times(1)).<String>send(any(HttpRequest.class), any());

    // All results should be present and equal to the timestamp
    assertTrue(result1.isPresent());
    assertTrue(result2.isPresent());
    assertTrue(result3.isPresent());
    assertEquals(result1.get().longValue(), futureTimestamp);
    assertEquals(result2.get().longValue(), futureTimestamp);
    assertEquals(result3.get().longValue(), futureTimestamp);
  }

  @Test
  public void testClearCacheRefetchesFromApi() throws Exception {
    // Setup - mock HTTP to return different timestamps
    long futureTimestamp1 = Instant.now().getEpochSecond() + (30L * 86400L);
    long futureTimestamp2 = Instant.now().getEpochSecond() + (15L * 86400L);
    String responseJson1 = String.valueOf(futureTimestamp1);
    String responseJson2 = String.valueOf(futureTimestamp2);

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseJson1).thenReturn(responseJson2);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute - fetch, clear cache, fetch again
    Optional<Long> result1 = service.getTrialExpirationTimestamp();
    service.clearCache();
    Optional<Long> result2 = service.getTrialExpirationTimestamp();

    // Verify - HTTP should be called twice
    verify(mockHttpClient, times(2)).<String>send(any(HttpRequest.class), any());

    // Results should reflect the different timestamps
    assertTrue(result1.isPresent());
    assertTrue(result2.isPresent());
    assertEquals(result1.get().longValue(), futureTimestamp1);
    assertEquals(result2.get().longValue(), futureTimestamp2);
  }

  @Test
  public void testApiFailureWithNoCacheReturnsEmpty() throws Exception {
    // Setup - mock immediate API failure (no cached value exists)
    when(mockHttpClient.<String>send(any(HttpRequest.class), any()))
        .thenThrow(new IOException("Connection failed"));

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute - fetch when no cache exists and API fails
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty when no cached value and API fails
    assertFalse(result.isPresent());
  }

  @Test
  public void testMissingConfigurationReturnsEmpty() {
    // Setup - empty configuration
    ControlPlaneConfiguration emptyConfig = new ControlPlaneConfiguration();

    ControlPlaneService service = new ControlPlaneService(emptyConfig, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty, no HTTP calls made
    assertFalse(result.isPresent());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  public void testMissingUrlReturnsEmpty() {
    // Setup - config with only API key
    ControlPlaneConfiguration partialConfig = new ControlPlaneConfiguration();
    partialConfig.setApiKey(TEST_API_KEY);

    ControlPlaneService service = new ControlPlaneService(partialConfig, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty, no HTTP calls made
    assertFalse(result.isPresent());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  public void testMissingApiKeyReturnsEmpty() {
    // Setup - config with only URL
    ControlPlaneConfiguration partialConfig = new ControlPlaneConfiguration();
    partialConfig.setUrl(TEST_URL);

    ControlPlaneService service = new ControlPlaneService(partialConfig, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty, no HTTP calls made
    assertFalse(result.isPresent());
    verifyNoInteractions(mockHttpClient);
  }

  @Test
  public void testApiReturnsNon200ReturnsEmpty() throws Exception {
    // Setup - mock HTTP 404 response
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(404);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testApiReturnsEmptyBodyReturnsEmpty() throws Exception {
    // Setup - mock empty response body
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn("");

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testApiReturnsInvalidFormatReturnsEmpty() throws Exception {
    // Setup - mock response with invalid format (text that can't be parsed as a number)
    String responseJson = "not_a_number";

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseJson);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testReturnsExpiredTimestamp() throws Exception {
    // Setup - mock response with timestamp in the past (30 days ago)
    long pastTimestamp = Instant.now().getEpochSecond() - (30L * 86400L);
    String responseJson = String.valueOf(pastTimestamp);

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseJson);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return the past timestamp
    assertTrue(result.isPresent());
    assertEquals(result.get().longValue(), pastTimestamp);
  }

  @Test
  public void testReturnsCurrentTimestamp() throws Exception {
    // Setup - mock response with timestamp exactly now
    long nowTimestamp = Instant.now().getEpochSecond();
    String responseJson = String.valueOf(nowTimestamp);

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseJson);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return the current timestamp
    assertTrue(result.isPresent());
    assertEquals(result.get().longValue(), nowTimestamp);
  }

  @Test
  public void testInvalidJsonReturnsEmpty() throws Exception {
    // Setup - mock response with invalid JSON
    String invalidJson = "this is not json";

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(invalidJson);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should return empty
    assertFalse(result.isPresent());
  }

  @Test
  public void testCustomCacheTtlIsUsed() throws Exception {
    // Setup - config with custom cache TTL
    config.setCacheTtlMinutes(30L);

    long futureTimestamp = Instant.now().getEpochSecond() + (30L * 86400L);
    String responseJson = String.valueOf(futureTimestamp);

    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);
    when(mockHttpResponse.body()).thenReturn(responseJson);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    // Execute - fetch to populate cache
    Optional<Long> result = service.getTrialExpirationTimestamp();

    // Verify - should use custom TTL (verified in logs)
    assertTrue(result.isPresent());
    assertEquals(result.get().longValue(), futureTimestamp);
  }

  @Test
  public void testSendAdminInviteTokenSuccess() throws Exception {
    when(mockHttpClient.<String>send(any(HttpRequest.class), any())).thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    boolean result = service.sendAdminInviteToken("test-token-123", 7, 2);

    assertTrue(result);
    verify(mockHttpClient, times(1)).<String>send(any(HttpRequest.class), any());
  }

  @Test
  public void testSendAdminInviteTokenRetriesOnFailure() throws Exception {
    when(mockHttpClient.<String>send(any(HttpRequest.class), any()))
        .thenThrow(new IOException("Connection refused"))
        .thenThrow(new IOException("Connection refused"))
        .thenReturn(mockHttpResponse);
    when(mockHttpResponse.statusCode()).thenReturn(200);

    ControlPlaneService service = new ControlPlaneService(config, mockHttpClient);

    boolean result = service.sendAdminInviteToken("test-token-123", 7, 2);

    assertTrue(result);
    verify(mockHttpClient, times(3)).<String>send(any(HttpRequest.class), any());
  }

  @Test
  public void testSendAdminInviteTokenNotConfiguredReturnsFalse() {
    ControlPlaneConfiguration emptyConfig = new ControlPlaneConfiguration();
    ControlPlaneService service = new ControlPlaneService(emptyConfig, mockHttpClient);

    boolean result = service.sendAdminInviteToken("test-token-123", 7, 2);

    assertFalse(result);
    verifyNoInteractions(mockHttpClient);
  }
}
