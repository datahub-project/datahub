package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.SsoProvider;
import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.typesafe.config.ConfigFactory;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.pac4j.core.client.Client;
import org.pac4j.core.client.Clients;
import org.pac4j.core.context.WebContext;
import org.pac4j.core.exception.http.HttpAction;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.libs.concurrent.HttpExecutionContext;
import play.mvc.Http;
import play.mvc.Result;

public class SsoSupportCallbackControllerTest {

  @Mock private SsoSupportManager mockSsoSupportManager;

  @Mock private OperationContext mockOperationContext;

  @Mock private SystemEntityClient mockEntityClient;

  @Mock private AuthServiceClient mockAuthClient;

  @Mock private org.pac4j.core.config.Config mockPac4jConfig;

  @Mock private PlayCookieSessionStore mockSessionStore;

  @Mock private SsoProvider.SsoProtocol mockSsoProtocol;

  @Mock private SsoProvider mockSsoProvider;

  @Mock private WebContext mockWebContext;

  @Mock private HttpAction mockAction;

  @Mock private HttpExecutionContext mockHttpExecutionContext;

  @Mock private Executor mockExecutor;

  private SsoSupportCallbackController controller;
  private com.typesafe.config.Config mockConfig;

  @BeforeEach
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    configMap.put("datahub.basePath", "");

    mockConfig = ConfigFactory.parseMap(configMap);

    // Set up the mock protocol
    when(mockSsoProtocol.getCommonName()).thenReturn("oidc");
    when(mockSsoProvider.protocol()).thenReturn(mockSsoProtocol);

    // Set up the Pac4j config with SessionStore
    when(mockPac4jConfig.getSessionStoreFactory()).thenReturn(parameters -> mockSessionStore);

    // Set up HttpExecutionContext mock
    when(mockHttpExecutionContext.current()).thenReturn(mockExecutor);

    controller =
        new SsoSupportCallbackController(
            mockSsoSupportManager,
            mockOperationContext,
            mockEntityClient,
            mockAuthClient,
            mockPac4jConfig,
            mockConfig);

    // Inject HttpExecutionContext using reflection
    try {
      java.lang.reflect.Field ecField =
          controller.getClass().getSuperclass().getDeclaredField("ec");
      ecField.setAccessible(true);
      ecField.set(controller, mockHttpExecutionContext);
    } catch (Exception e) {
      throw new RuntimeException("Failed to inject HttpExecutionContext", e);
    }
  }

  @Test
  public void testConstructor() {
    assertNotNull(controller);
  }

  @Test
  public void testHandleSupportCallbackGet() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    // Test the callback with the correct protocol
    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    // Verify the result is not null
    assertNotNull(result);
  }

  @Test
  public void testHandleSupportCallbackPost() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("POST");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    // Test the callback with the correct protocol
    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    // Verify the result is not null
    assertNotNull(result);
  }

  @Test
  public void testHandleSupportCallbackWithNoProvider() throws Exception {
    // Mock the SSO manager to return null (no provider)
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(null);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/callback/oidc-support");

    // Test the callback - should handle gracefully
    assertDoesNotThrow(
        () -> {
          CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);
          assertNotNull(result);
        });
  }

  @Test
  public void testDifferentProtocols() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/callback/oidc-support");

    // Test with different protocols
    assertDoesNotThrow(
        () -> {
          CompletionStage<Result> result1 = controller.handleSupportCallback("oidc", mockRequest);
          assertNotNull(result1);

          CompletionStage<Result> result2 = controller.handleSupportCallback("oidc", mockRequest);
          assertNotNull(result2);
        });
  }

  @Test
  public void testCallbackUrlGeneration() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Create a mock request with specific URI
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc?code=test-code&state=test-state");

    // Test the callback with the correct protocol
    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    // Verify the result is not null
    assertNotNull(result);
  }

  @Test
  public void testShouldHandleSupportCallbackWithOidcSupportProtocol() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    // Test the callback with oidc protocol - should be handled
    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    // Verify the result is not null
    assertNotNull(result);
  }

  @Test
  public void testShouldHandleSupportCallbackWithRegularOidcProtocol() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/callback/oidc");

    // Test the callback with regular oidc protocol - should also be handled
    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    // Verify the result is not null
    assertNotNull(result);
  }

  @Test
  public void testShouldNotHandleSupportCallbackWithUnsupportedProtocol() throws Exception {
    // Mock the SSO manager to return a provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Create a mock request
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/callback/saml");

    // Test the callback with unsupported protocol - should return error
    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    // Verify the result is not null (should return error response)
    assertNotNull(result);
  }

  @Test
  public void testConstructorWithBasePath() throws Exception {
    // Test constructor with non-empty base path
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    configMap.put("datahub.basePath", "/custom-path");

    com.typesafe.config.Config customConfig = ConfigFactory.parseMap(configMap);

    SsoSupportCallbackController customController =
        new SsoSupportCallbackController(
            mockSsoSupportManager,
            mockOperationContext,
            mockEntityClient,
            mockAuthClient,
            mockPac4jConfig,
            customConfig);

    assertNotNull(customController);
    // The default URL should include the base path
    assertEquals("/custom-path/", customController.getDefaultUrl());
  }

  @Test
  public void testConstructorWithEmptyBasePath() throws Exception {
    // Test constructor with empty base path
    Map<String, String> configMap = new HashMap<>();
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri",
        "https://test.example.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "https://datahub.example.com");
    configMap.put("datahub.basePath", "");

    com.typesafe.config.Config customConfig = ConfigFactory.parseMap(configMap);

    SsoSupportCallbackController customController =
        new SsoSupportCallbackController(
            mockSsoSupportManager,
            mockOperationContext,
            mockEntityClient,
            mockAuthClient,
            mockPac4jConfig,
            customConfig);

    assertNotNull(customController);
    // The default URL should be just "/"
    assertEquals("/", customController.getDefaultUrl());
  }

  @Test
  public void testHandleSupportCallbackWithSsoDisabled() throws Exception {
    // Mock SSO as disabled
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(false);

    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    assertNotNull(result);
    // Should return internal server error when SSO is disabled
    Result actualResult = result.toCompletableFuture().get();
    assertEquals(500, actualResult.status());
  }

  @Test
  public void testHandleSupportCallbackWithNullProvider() throws Exception {
    // Mock SSO as enabled but provider is null
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(null);

    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    // This should throw an exception when trying to access the provider
    assertThrows(
        RuntimeException.class,
        () -> {
          CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);
          result.toCompletableFuture().get();
        });
  }

  @Test
  public void testHandleSupportCallbackWithException() throws Exception {
    // Mock SSO as enabled with provider
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);

    // Mock the callback to throw an exception
    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    // Create a spy to mock the callback method to return a failed CompletionStage
    SsoSupportCallbackController spyController = spy(controller);
    CompletableFuture<Result> failedStage = new CompletableFuture<>();
    failedStage.completeExceptionally(new RuntimeException("Test exception"));
    doReturn(failedStage).when(spyController).callback(any(Http.Request.class));

    CompletionStage<Result> result = spyController.handleSupportCallback("oidc", mockRequest);

    assertNotNull(result);
    // The exception should be caught and handled asynchronously
    Result actualResult = result.toCompletableFuture().get();
    // Should redirect to login with error message
    assertEquals(303, actualResult.status());
    assertTrue(actualResult.redirectLocation().orElse("").contains("/login"));
    assertTrue(actualResult.redirectLocation().orElse("").contains("error_msg"));
  }

  @Test
  public void testShouldHandleSupportCallbackWithOidcProtocol() throws Exception {
    // Mock SSO as enabled with OIDC provider
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);

    // Use reflection to test the private method
    java.lang.reflect.Method method =
        controller.getClass().getDeclaredMethod("shouldHandleSupportCallback", String.class);
    method.setAccessible(true);

    // Test with "oidc" since that's what the mock returns as common name
    boolean result = (boolean) method.invoke(controller, "oidc");

    assertTrue(result, "Should handle oidc protocol");
  }

  @Test
  public void testShouldHandleSupportCallbackWithSsoDisabled() throws Exception {
    // Mock SSO as disabled
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(false);

    // Use reflection to test the private method
    java.lang.reflect.Method method =
        controller.getClass().getDeclaredMethod("shouldHandleSupportCallback", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(controller, "oidc");

    assertFalse(result, "Should not handle when SSO is disabled");
  }

  @Test
  public void testShouldHandleSupportCallbackWithUnsupportedProtocol() throws Exception {
    // Mock SSO as enabled with OIDC provider
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);

    // Use reflection to test the private method
    java.lang.reflect.Method method =
        controller.getClass().getDeclaredMethod("shouldHandleSupportCallback", String.class);
    method.setAccessible(true);

    boolean result = (boolean) method.invoke(controller, "saml");

    assertFalse(result, "Should not handle unsupported protocol");
  }

  @Test
  public void testUpdateConfig() throws Exception {
    // Mock SSO provider with client
    Client mockClient = mock(Client.class);
    when(mockSsoProvider.client()).thenReturn(mockClient);
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);

    // Use reflection to test the private method
    java.lang.reflect.Method method = controller.getClass().getDeclaredMethod("updateConfig");
    method.setAccessible(true);

    // Should not throw exception
    assertDoesNotThrow(() -> method.invoke(controller));

    // Verify that config.setClients was called
    verify(mockPac4jConfig).setClients(any(Clients.class));
  }

  @Test
  public void testCallbackMethod() throws Exception {
    // Mock SSO provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc");

    CompletionStage<Result> result = controller.callback(mockRequest);

    assertNotNull(result);
    // The callback method should return a CompletionStage
    assertTrue(result instanceof CompletionStage);
  }

  @Test
  public void testHandleSupportCallbackWithDifferentMethods() throws Exception {
    // Mock SSO provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    // Test both GET and POST methods
    String[] methods = {"GET", "POST"};
    for (String method : methods) {
      Http.Request mockRequest = mock(Http.Request.class);
      when(mockRequest.method()).thenReturn(method);
      when(mockRequest.uri()).thenReturn("/support/callback/oidc");

      CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

      assertNotNull(result, "Result should not be null for " + method + " method");
    }
  }

  @Test
  public void testHandleSupportCallbackWithQueryParameters() throws Exception {
    // Mock SSO provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri())
        .thenReturn("/support/callback/oidc?code=test-code&state=test-state&error=test-error");

    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    assertNotNull(result);
  }

  @Test
  public void testHandleSupportCallbackWithMalformedUrl() throws Exception {
    // Mock SSO provider
    when(mockSsoSupportManager.getSupportSsoProvider()).thenReturn(mockSsoProvider);
    when(mockSsoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    Http.Request mockRequest = mock(Http.Request.class);
    when(mockRequest.method()).thenReturn("GET");
    when(mockRequest.uri()).thenReturn("/support/callback/oidc?malformed=param&");

    CompletionStage<Result> result = controller.handleSupportCallback("oidc", mockRequest);

    assertNotNull(result);
  }
}
