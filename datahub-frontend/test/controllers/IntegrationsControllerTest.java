package controllers;

import static auth.AuthUtils.SESSION_COOKIE_GMS_TOKEN_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import play.mvc.Http;

/**
 * Unit tests for IntegrationsController, focusing on authorization header handling when proxying
 * requests to the integrations service.
 */
public class IntegrationsControllerTest {

  private IntegrationsController controller;
  private Config mockConfig;

  @BeforeEach
  void setUp() {
    // Create a minimal config
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("integrations.service.host", "localhost");
    configMap.put("integrations.service.port", 5000);
    configMap.put("integrations.service.useSsl", false);
    mockConfig = ConfigFactory.parseMap(configMap);

    controller = new IntegrationsController(mockConfig);
  }

  @Test
  void testAuthorizationHeaderFromExplicitHeader() {
    // Test that explicit Authorization header takes precedence
    Http.Request request = mock(Http.Request.class);
    Http.Headers headers = mock(Http.Headers.class);
    Http.Session session = mock(Http.Session.class);

    when(request.getHeaders()).thenReturn(headers);
    when(request.session()).thenReturn(session);

    // Set up explicit Authorization header
    when(headers.contains(Http.HeaderNames.AUTHORIZATION)).thenReturn(true);
    when(headers.get(Http.HeaderNames.AUTHORIZATION))
        .thenReturn(Optional.of("Bearer explicit-token"));

    // Also set up session with a different token
    Map<String, String> sessionData = new HashMap<>();
    sessionData.put(SESSION_COOKIE_GMS_TOKEN_NAME, "session-token");
    when(session.data()).thenReturn(sessionData);

    // Use reflection to test the private method
    String authValue = invokeGetAuthorizationHeaderValueToProxy(request);

    // Explicit header should take precedence
    assertEquals("Bearer explicit-token", authValue);
  }

  @Test
  void testAuthorizationHeaderFromSession() {
    // Test that session token is used when no explicit header
    Http.Request request = mock(Http.Request.class);
    Http.Headers headers = mock(Http.Headers.class);
    Http.Session session = mock(Http.Session.class);

    when(request.getHeaders()).thenReturn(headers);
    when(request.session()).thenReturn(session);

    // No explicit Authorization header
    when(headers.contains(Http.HeaderNames.AUTHORIZATION)).thenReturn(false);

    // Set up session with token
    Map<String, String> sessionData = new HashMap<>();
    sessionData.put(SESSION_COOKIE_GMS_TOKEN_NAME, "session-jwt-token");
    when(session.data()).thenReturn(sessionData);

    String authValue = invokeGetAuthorizationHeaderValueToProxy(request);

    // Should use session token with Bearer prefix
    assertEquals("Bearer session-jwt-token", authValue);
  }

  @Test
  void testAuthorizationHeaderEmpty() {
    // Test that empty string is returned when neither header nor session exists
    Http.Request request = mock(Http.Request.class);
    Http.Headers headers = mock(Http.Headers.class);
    Http.Session session = mock(Http.Session.class);

    when(request.getHeaders()).thenReturn(headers);
    when(request.session()).thenReturn(session);

    // No explicit Authorization header
    when(headers.contains(Http.HeaderNames.AUTHORIZATION)).thenReturn(false);

    // Empty session
    Map<String, String> sessionData = new HashMap<>();
    when(session.data()).thenReturn(sessionData);

    String authValue = invokeGetAuthorizationHeaderValueToProxy(request);

    assertEquals("", authValue);
  }

  @Test
  void testExplicitHeaderTakesPrecedenceOverSession() {
    // Explicitly verify the precedence order
    Http.Request request = mock(Http.Request.class);
    Http.Headers headers = mock(Http.Headers.class);
    Http.Session session = mock(Http.Session.class);

    when(request.getHeaders()).thenReturn(headers);
    when(request.session()).thenReturn(session);

    // Both explicit header and session token exist
    when(headers.contains(Http.HeaderNames.AUTHORIZATION)).thenReturn(true);
    when(headers.get(Http.HeaderNames.AUTHORIZATION)).thenReturn(Optional.of("Basic abc123"));

    Map<String, String> sessionData = new HashMap<>();
    sessionData.put(SESSION_COOKIE_GMS_TOKEN_NAME, "session-token");
    when(session.data()).thenReturn(sessionData);

    String authValue = invokeGetAuthorizationHeaderValueToProxy(request);

    // Explicit header (Basic auth in this case) should take precedence
    assertEquals("Basic abc123", authValue);
    // Verify we never even checked the session since header exists
    verify(session, never()).data();
  }

  /**
   * Helper method to invoke the private getAuthorizationHeaderValueToProxy method. Uses reflection
   * since the method is private.
   */
  private String invokeGetAuthorizationHeaderValueToProxy(Http.Request request) {
    try {
      java.lang.reflect.Method method =
          IntegrationsController.class.getDeclaredMethod(
              "getAuthorizationHeaderValueToProxy", Http.Request.class);
      method.setAccessible(true);
      return (String) method.invoke(controller, request);
    } catch (Exception e) {
      throw new RuntimeException("Failed to invoke getAuthorizationHeaderValueToProxy", e);
    }
  }
}
