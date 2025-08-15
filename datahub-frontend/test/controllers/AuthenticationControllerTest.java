package controllers;

import static auth.AuthUtils.REDIRECT_URL_COOKIE_NAME;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
    controller.playCookieSessionStore = playCookieSessionStore;
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
}
