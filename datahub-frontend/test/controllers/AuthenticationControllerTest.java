package controllers;

import static auth.AuthUtils.REDIRECT_URL_COOKIE_NAME;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

import auth.AuthUtils;
import auth.sso.SsoManager;
import auth.sso.SsoProvider;
import client.AuthServiceClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.compress.utils.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.pac4j.core.client.Client;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.core.exception.http.RedirectionAction;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.store.DataEncrypter;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.mvc.Http;
import play.mvc.Result;

public class AuthenticationControllerTest {

  private AuthenticationController controller;
  private Config mockConfig;
  private org.pac4j.core.config.Config ssoConfig;
  private PlayCookieSessionStore playCookieSessionStore;
  private SsoManager ssoManager;
  private AuthServiceClient authClient;

  private class MockSerializer implements org.pac4j.core.util.serializer.Serializer {
    @Override
    public String serializeToString(Object var1) {
      return "";
    }

    @Override
    public Object deserializeFromString(String var1) {
      return new Object();
    }

    @Override
    public byte[] serializeToBytes(Object o) {
      return "serialized-bytes".getBytes();
    }

    @Override
    public Object deserializeFromBytes(byte[] bytes) {
      return new FoundAction("/redirectedPath");
    }
  }

  @BeforeEach
  public void setUp() {
    // Create mock configurations
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");
    configMap.put("datahub.basePath", "");

    mockConfig = ConfigFactory.parseMap(configMap);

    // Mock SSO config
    ssoConfig = new org.pac4j.core.config.Config();

    // Mock session store with custom serializer
    playCookieSessionStore = new PlayCookieSessionStore(mock(DataEncrypter.class));
    playCookieSessionStore.setSerializer(new MockSerializer());

    // Mock SSO manager
    ssoManager = mock(SsoManager.class);
    SsoProvider mockProvider = mock(SsoProvider.class);
    when(ssoManager.getSsoProvider()).thenReturn(mockProvider);
    Client mockClient = mock(Client.class);
    when(mockProvider.client()).thenReturn(mockClient);
    when(mockClient.getName()).thenReturn("oidcClient");

    // Mock found action for redirection
    FoundAction foundAction = new FoundAction("/redirectPath");
    RedirectionAction redirectAction = foundAction;

    // Setup the client mock to return our redirection action
    when(mockClient.getRedirectionAction(any(CallContext.class)))
        .thenReturn(Optional.of(redirectAction));

    // Mock auth service client
    authClient = mock(AuthServiceClient.class);
    when(authClient.generateSessionTokenForUser(anyString(), anyString())).thenReturn("mock-token");

    // Create the controller
    controller = new AuthenticationController(mockConfig);
    controller.sessionStore = playCookieSessionStore;
    controller.ssoManager = ssoManager;
    controller.authClient = authClient;
  }

  @Test
  public void testRedirectCookieCreation() {
    // Create mock HTTP context
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/dashboard");

    Http.Request request = requestBuilder.build();

    // Create mock context and result
    CallContext callContext = new CallContext(new PlayWebContext(request), playCookieSessionStore);

    Result initialResult = new Result(302);

    // Call the method to test
    Result result = controller.addRedirectCookie(initialResult, callContext, "/dashboard");

    // Verify the cookie was added
    Optional<Http.Cookie> redirectCookie =
        Lists.newArrayList(result.cookies().iterator()).stream()
            .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
            .findFirst();

    assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present");
    Http.Cookie cookie = redirectCookie.get();
    assertEquals("/", cookie.path());
    assertTrue(cookie.secure());
    assertTrue(cookie.httpOnly());
    assertEquals(Http.Cookie.SameSite.NONE, cookie.sameSite().get());
    assertNotNull(cookie.value(), "Cookie value should not be null");
  }

  @Test
  public void testSsoRedirectWithCookie() {
    // Configure SSO to be enabled
    when(ssoManager.isSsoEnabled()).thenReturn(true);

    // Create mock HTTP context
    Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method("GET").uri("/sso");

    Http.Request request = requestBuilder.build();

    // Test the SSO method
    Result result = controller.sso(request);

    // Verify the result code
    assertEquals(302, result.status());

    // Verify the redirect cookie is added
    Optional<Http.Cookie> redirectCookie =
        Lists.newArrayList(result.cookies().iterator()).stream()
            .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
            .findFirst();

    assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present for SSO");
  }

  @Test
  public void testAuthenticateWithRedirect() {
    // Configure SSO to be enabled
    when(ssoManager.isSsoEnabled()).thenReturn(true);

    // Create mock HTTP context
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/dashboard");

    Http.Request request = requestBuilder.build();

    // Test the authenticate method
    Result result = controller.authenticate(request);

    // Verify the result code
    assertEquals(302, result.status());

    // Verify the redirect cookie is added
    Optional<Http.Cookie> redirectCookie =
        Lists.newArrayList(result.cookies().iterator()).stream()
            .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
            .findFirst();

    assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present for authenticate");

    // Verify cookie value contains the encoded path
    Http.Cookie cookie = redirectCookie.get();
    assertNotNull(cookie.value(), "Cookie value should not be null");
  }

  @Test
  public void testRedirectOnlySetsSecureCookies() {
    // Create mock HTTP context
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/dashboard");

    Http.Request request = requestBuilder.build();

    // Create mock context and result
    CallContext callContext = new CallContext(new PlayWebContext(request), playCookieSessionStore);

    Result initialResult = new Result(302);

    // Call the method to test
    Result result = controller.addRedirectCookie(initialResult, callContext, "/dashboard");

    // Verify all cookies are secure
    boolean allCookiesSecure =
        Lists.newArrayList(result.cookies().iterator()).stream().allMatch(Http.Cookie::secure);

    assertTrue(allCookiesSecure, "All cookies should be secure");
  }

  @Test
  public void testGetBasePathWithEmptyConfig() {
    // Test with empty basePath configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath(""))
          .thenReturn("");

      // Use reflection to test the private method
      java.lang.reflect.Method getBasePathMethod =
          AuthenticationController.class.getDeclaredMethod("getBasePath");
      getBasePathMethod.setAccessible(true);
      String result = (String) getBasePathMethod.invoke(testController);

      assertEquals("", result);
    } catch (Exception e) {
      fail("Failed to test getBasePath method: " + e.getMessage());
    }
  }

  @Test
  public void testGetBasePathWithCustomPath() {
    // Test with custom basePath configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/datahub");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/datahub"))
          .thenReturn("/datahub");

      // Use reflection to test the private method
      java.lang.reflect.Method getBasePathMethod =
          AuthenticationController.class.getDeclaredMethod("getBasePath");
      getBasePathMethod.setAccessible(true);
      String result = (String) getBasePathMethod.invoke(testController);

      assertEquals("/datahub", result);
    } catch (Exception e) {
      fail("Failed to test getBasePath method: " + e.getMessage());
    }
  }

  @Test
  public void testGetLoginUrlWithBasePath() {
    // Test getLoginUrl with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/api/v2");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/api/v2"))
          .thenReturn("/api/v2");
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.addBasePath("/login", "/api/v2"))
          .thenReturn("/api/v2/login");

      // Use reflection to test the private method
      java.lang.reflect.Method getLoginUrlMethod =
          AuthenticationController.class.getDeclaredMethod("getLoginUrl");
      getLoginUrlMethod.setAccessible(true);
      String result = (String) getLoginUrlMethod.invoke(testController);

      assertEquals("/api/v2/login", result);
    } catch (Exception e) {
      fail("Failed to test getLoginUrl method: " + e.getMessage());
    }
  }

  @Test
  public void testGetLoginUrlWithoutBasePath() {
    // Test getLoginUrl without basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath(""))
          .thenReturn("");
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.addBasePath("/login", ""))
          .thenReturn("/login");

      // Use reflection to test the private method
      java.lang.reflect.Method getLoginUrlMethod =
          AuthenticationController.class.getDeclaredMethod("getLoginUrl");
      getLoginUrlMethod.setAccessible(true);
      String result = (String) getLoginUrlMethod.invoke(testController);

      assertEquals("/login", result);
    } catch (Exception e) {
      fail("Failed to test getLoginUrl method: " + e.getMessage());
    }
  }

  @Test
  public void testAuthenticateWithBasePathRedirect() {
    // Test authenticate method with basePath in redirect
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/datahub");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);
    testController.ssoManager = ssoManager;
    testController.authClient = authClient;
    testController.sessionStore = playCookieSessionStore;

    // Configure SSO to be enabled
    when(ssoManager.isSsoEnabled()).thenReturn(true);

    // Create mock HTTP context with redirect_uri
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/dashboard");

    Http.Request request = requestBuilder.build();

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/datahub"))
          .thenReturn("/datahub");
      basePathUtilsMock
          .when(
              () -> com.linkedin.metadata.utils.BasePathUtils.addBasePath(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String path = invocation.getArgument(0);
                String basePath = invocation.getArgument(1);
                return basePath + path;
              });

      // Test the authenticate method
      Result result = testController.authenticate(request);

      // Verify the result code
      assertEquals(302, result.status());

      // Verify the redirect cookie is added with proper basePath
      Optional<Http.Cookie> redirectCookie =
          Lists.newArrayList(result.cookies().iterator()).stream()
              .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
              .findFirst();

      assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present for authenticate");
    }
  }

  @Test
  public void testAuthenticateWithLogOutRedirect() {
    // Test authenticate method with /logOut redirect (special case)
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/datahub");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);
    testController.ssoManager = ssoManager;
    testController.authClient = authClient;
    testController.sessionStore = playCookieSessionStore;

    // Configure SSO to be enabled
    when(ssoManager.isSsoEnabled()).thenReturn(true);

    // Create mock HTTP context with /logOut redirect
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/logOut");

    Http.Request request = requestBuilder.build();

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/datahub"))
          .thenReturn("/datahub");
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.addBasePath("/logOut", "/datahub"))
          .thenReturn("/datahub/logOut");
      basePathUtilsMock
          .when(
              () -> com.linkedin.metadata.utils.BasePathUtils.addBasePath(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String path = invocation.getArgument(0);
                String basePath = invocation.getArgument(1);
                return basePath + path;
              });

      // Test the authenticate method
      Result result = testController.authenticate(request);

      // Verify the result code
      assertEquals(302, result.status());

      // Verify the redirect cookie is added with proper basePath
      Optional<Http.Cookie> redirectCookie =
          Lists.newArrayList(result.cookies().iterator()).stream()
              .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
              .findFirst();

      assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present for authenticate");
    }
  }

  @Test
  public void testSsoWithBasePath() {
    // Test sso method with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/api/v2");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);
    testController.ssoManager = ssoManager;
    testController.sessionStore = playCookieSessionStore;

    // Configure SSO to be enabled
    when(ssoManager.isSsoEnabled()).thenReturn(true);

    // Create mock HTTP context
    Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method("GET").uri("/sso");
    Http.Request request = requestBuilder.build();

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/api/v2"))
          .thenReturn("/api/v2");
      basePathUtilsMock
          .when(
              () -> com.linkedin.metadata.utils.BasePathUtils.addBasePath(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String path = invocation.getArgument(0);
                String basePath = invocation.getArgument(1);
                return basePath + path;
              });

      // Test the SSO method
      Result result = testController.sso(request);

      // Verify the result code
      assertEquals(302, result.status());

      // Verify the redirect cookie is added
      Optional<Http.Cookie> redirectCookie =
          Lists.newArrayList(result.cookies().iterator()).stream()
              .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
              .findFirst();

      assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present for SSO");
    }
  }

  @Test
  public void testSsoWithBasePathDisabled() {
    // Test sso method when SSO is disabled with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/datahub");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);
    testController.ssoManager = ssoManager;

    // Configure SSO to be disabled
    when(ssoManager.isSsoEnabled()).thenReturn(false);

    // Create mock HTTP context
    Http.RequestBuilder requestBuilder = new Http.RequestBuilder().method("GET").uri("/sso");
    Http.Request request = requestBuilder.build();

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/datahub"))
          .thenReturn("/datahub");
      basePathUtilsMock
          .when(
              () -> com.linkedin.metadata.utils.BasePathUtils.addBasePath(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String path = invocation.getArgument(0);
                String basePath = invocation.getArgument(1);
                return basePath + path;
              });

      // Test the SSO method
      Result result = testController.sso(request);

      // Verify the result code (Play Framework's Results.redirect() returns 303 by default)
      assertEquals(303, result.status());

      // Verify redirect to login URL with error message
      String redirectLocation = result.redirectLocation().orElse("");
      assertTrue(redirectLocation.contains("/datahub/login"));
      assertTrue(redirectLocation.contains("error_msg"));
    }
  }

  @Test
  public void testAddRedirectCookieWithBasePath() {
    // Test addRedirectCookie with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/api/v2");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);

    // Create mock HTTP context
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/dashboard");

    Http.Request request = requestBuilder.build();

    // Create mock context and result
    CallContext callContext = new CallContext(new PlayWebContext(request), playCookieSessionStore);
    Result initialResult = new Result(302);

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/api/v2"))
          .thenReturn("/api/v2");
      basePathUtilsMock
          .when(
              () -> com.linkedin.metadata.utils.BasePathUtils.addBasePath("/dashboard", "/api/v2"))
          .thenReturn("/api/v2/dashboard");
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.addBasePath("/", "/api/v2"))
          .thenReturn("/api/v2/");

      // Call the method to test
      Result result = testController.addRedirectCookie(initialResult, callContext, "/dashboard");

      // Verify the cookie was added
      Optional<Http.Cookie> redirectCookie =
          Lists.newArrayList(result.cookies().iterator()).stream()
              .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
              .findFirst();

      assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present");
      Http.Cookie cookie = redirectCookie.get();
      assertEquals("/api/v2/", cookie.path());
      assertTrue(cookie.secure());
      assertTrue(cookie.httpOnly());
      assertEquals(Http.Cookie.SameSite.NONE, cookie.sameSite().get());
      assertNotNull(cookie.value(), "Cookie value should not be null");
    }
  }

  @Test
  public void testRedirectToIdentityProviderWithBasePath() {
    // Test redirectToIdentityProvider with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "/datahub");
    configMap.put("auth.cookie.ttlInHours", 24);
    configMap.put("auth.cookie.secure", true);
    configMap.put("auth.cookie.sameSite", "Lax");

    Config config = ConfigFactory.parseMap(configMap);
    AuthenticationController testController = new AuthenticationController(config);
    testController.ssoManager = ssoManager;
    testController.sessionStore = playCookieSessionStore;

    // Configure SSO to be enabled
    when(ssoManager.isSsoEnabled()).thenReturn(true);

    // Create mock HTTP context
    Http.RequestBuilder requestBuilder =
        new Http.RequestBuilder().method("GET").uri("/authenticate?redirect_uri=/dashboard");

    Http.Request request = requestBuilder.build();

    try (MockedStatic<com.linkedin.metadata.utils.BasePathUtils> basePathUtilsMock =
        mockStatic(com.linkedin.metadata.utils.BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> com.linkedin.metadata.utils.BasePathUtils.normalizeBasePath("/datahub"))
          .thenReturn("/datahub");
      basePathUtilsMock
          .when(
              () -> com.linkedin.metadata.utils.BasePathUtils.addBasePath(anyString(), anyString()))
          .thenAnswer(
              invocation -> {
                String path = invocation.getArgument(0);
                String basePath = invocation.getArgument(1);
                return basePath + path;
              });

      // Use reflection to test the private method
      java.lang.reflect.Method redirectToIdentityProviderMethod =
          AuthenticationController.class.getDeclaredMethod(
              "redirectToIdentityProvider", Http.RequestHeader.class, String.class);
      redirectToIdentityProviderMethod.setAccessible(true);

      @SuppressWarnings("unchecked")
      Optional<Result> result =
          (Optional<Result>)
              redirectToIdentityProviderMethod.invoke(testController, request, "/dashboard");

      assertTrue(result.isPresent(), "Result should be present");
      Result redirectResult = result.get();
      assertEquals(302, redirectResult.status());

      // Verify the redirect cookie is added
      Optional<Http.Cookie> redirectCookie =
          Lists.newArrayList(redirectResult.cookies().iterator()).stream()
              .filter(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME))
              .findFirst();

      assertTrue(redirectCookie.isPresent(), "Redirect cookie should be present");
    } catch (Exception e) {
      fail("Failed to test redirectToIdentityProvider method: " + e.getMessage());
    }
  }

  @Test
  public void testAuthenticateWithAbsoluteRedirectUriResetsToRoot() {
    // No SSO
    when(ssoManager.isSsoEnabled()).thenReturn(false);

    // Absolute URI triggers RedirectException in authenticate()
    Http.Request request =
        new Http.RequestBuilder()
            .method("GET")
            .uri("/authenticate?redirect_uri=http://evil.com")
            .build();

    try (MockedStatic<AuthUtils> authUtilsMock = mockStatic(auth.AuthUtils.class)) {
      // Make hasValidSessionCookie return true so we take the direct redirect path
      authUtilsMock.when(() -> auth.AuthUtils.hasValidSessionCookie(any())).thenReturn(true);

      Result result = controller.authenticate(request);

      // Should redirect (303) to "/" after catching RedirectException
      assertEquals(303, result.status());
      assertEquals("/", result.redirectLocation().orElse(""));

      // We should not have any redirect cookie here because we bypass SSO
      boolean hasRedirectCookie =
          Lists.newArrayList(result.cookies().iterator()).stream()
              .anyMatch(cookie -> cookie.name().equals(REDIRECT_URL_COOKIE_NAME));
      assertFalse(hasRedirectCookie, "No redirect cookie expected when redirectPath reset to '/'");
    }
  }

  @Test
  public void testAuthenticateWithProtocolRelativeRedirectUriResetsToRoot() {
    when(ssoManager.isSsoEnabled()).thenReturn(false);

    // Use URL-encoded form so redirect_uri value is unambiguously ///google.com or //google.com
    for (String redirectUriEncoded : new String[] {"%2F%2F%2Fgoogle.com", "%2F%2Fgoogle.com"}) {
      Http.Request request =
          new Http.RequestBuilder()
              .method("GET")
              .uri("/authenticate?redirect_uri=" + redirectUriEncoded)
              .build();

      try (MockedStatic<AuthUtils> authUtilsMock = mockStatic(auth.AuthUtils.class)) {
        authUtilsMock.when(() -> auth.AuthUtils.hasValidSessionCookie(any())).thenReturn(true);

        Result result = controller.authenticate(request);

        assertEquals(303, result.status());
        assertEquals("/", result.redirectLocation().orElse(""));
        assertFalse(
            result.redirectLocation().orElse("").startsWith("//"),
            "Must not redirect to protocol-relative URL: " + redirectUriEncoded);
      }
    }
  }
}
