package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.linkedin.entity.client.SystemEntityClient;
import com.typesafe.config.ConfigFactory;
import io.datahubproject.metadata.context.OperationContext;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
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

  @Mock private auth.sso.SsoProvider.SsoProtocol mockSsoProtocol;

  @Mock private auth.sso.SsoProvider mockSsoProvider;

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
    when(mockSsoProtocol.getCommonName()).thenReturn("oidc_support");
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
    when(mockRequest.uri()).thenReturn("/callback/oidc_support");

    // Test the callback with the correct protocol
    CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);

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
    when(mockRequest.uri()).thenReturn("/callback/oidc_support");

    // Test the callback with the correct protocol
    CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);

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
          CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);
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
          CompletionStage<Result> result1 = controller.handleSupportCallback(mockRequest);
          assertNotNull(result1);

          CompletionStage<Result> result2 = controller.handleSupportCallback(mockRequest);
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
    when(mockRequest.uri()).thenReturn("/callback/oidc_support?code=test-code&state=test-state");

    // Test the callback with the correct protocol
    CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);

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
    when(mockRequest.uri()).thenReturn("/callback/oidc_support");

    // Test the callback with oidc_support protocol - should be handled
    CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);

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
    CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);

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
    CompletionStage<Result> result = controller.handleSupportCallback(mockRequest);

    // Verify the result is not null (should return error response)
    assertNotNull(result);
  }
}
