package controllers;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import auth.sso.SsoProvider;
import auth.sso.SsoSupportManager;
import client.AuthServiceClient;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.pac4j.core.client.Client;
import org.pac4j.core.context.CallContext;
import org.pac4j.core.exception.http.FoundAction;
import org.pac4j.play.PlayWebContext;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.mvc.Http;
import play.mvc.Result;

public class SupportAuthenticationControllerTest {

  private SupportAuthenticationController controller;
  private Config mockConfig;
  private org.pac4j.core.config.Config ssoConfig;
  private PlayCookieSessionStore playCookieSessionStore;
  private SsoSupportManager ssoSupportManager;
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
    public byte[] serializeToBytes(Object var1) {
      return new byte[0];
    }

    @Override
    public Object deserializeFromBytes(byte[] var1) {
      return new Object();
    }
  }

  @BeforeEach
  public void setUp() {
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("datahub.basePath", "");
    configMap.put("auth.oidc.support.enabled", "true");
    configMap.put("auth.oidc.support.clientId", "test-client-id");
    configMap.put("auth.oidc.support.clientSecret", "test-client-secret");
    configMap.put(
        "auth.oidc.support.discoveryUri", "https://test-idp.com/.well-known/openid_configuration");
    configMap.put("auth.baseUrl", "http://localhost:9002");
    configMap.put("auth.oidc.support.group", "support-staff");
    configMap.put("auth.oidc.support.roleClaim", "role");
    // JIT provisioning is always enabled, pre-provisioning is always disabled
    // Group extraction is not used for support staff - they use a fixed group

    mockConfig = ConfigFactory.parseMap(configMap);
    ssoConfig = mock(org.pac4j.core.config.Config.class);
    playCookieSessionStore = mock(PlayCookieSessionStore.class);
    ssoSupportManager = mock(SsoSupportManager.class);
    authClient = mock(AuthServiceClient.class);

    controller = new SupportAuthenticationController(mockConfig);
    controller.playCookieSessionStore = playCookieSessionStore;
    controller.ssoSupportManager = ssoSupportManager;
    controller.authClient = authClient;

    when(playCookieSessionStore.getSerializer()).thenReturn(new MockSerializer());
  }

  @Test
  public void testAuthenticateSupportWithSsoEnabled() {
    // Arrange
    Http.Request request = mock(Http.Request.class);
    Http.Session session = mock(Http.Session.class);
    when(request.session()).thenReturn(session);
    when(request.getQueryString("redirect_uri")).thenReturn("/dashboard");
    when(ssoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    SsoProvider mockProvider = mock(SsoProvider.class);
    Client mockClient = mock(Client.class);
    when(ssoSupportManager.getSupportSsoProvider()).thenReturn(mockProvider);
    when(mockProvider.client()).thenReturn(mockClient);
    when(mockClient.getName()).thenReturn("oidc-support");

    // Mock the redirect action
    FoundAction mockAction = mock(FoundAction.class);
    when(mockAction.getLocation()).thenReturn("https://test-idp.com/authorize");

    CallContext mockCallContext = mock(CallContext.class);
    PlayWebContext mockWebContext = mock(PlayWebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockWebContext.getRequestCookies()).thenReturn(new java.util.ArrayList<>());

    // Create a spy to mock the buildCallContext method
    SupportAuthenticationController spyController = spy(controller);
    doReturn(mockCallContext).when(spyController).buildCallContext(any(Http.RequestHeader.class));

    when(mockClient.getRedirectionAction(any(CallContext.class)))
        .thenReturn(Optional.of(mockAction));

    // Act
    Result result = spyController.authenticateSupport(request);

    // Assert
    assertNotNull(result);
    assertEquals(303, result.status()); // Redirect status
  }

  @Test
  public void testSsoSupportWithSsoEnabled() {
    // Arrange
    Http.Request request = mock(Http.Request.class);
    Http.Session session = mock(Http.Session.class);
    when(request.session()).thenReturn(session);
    when(ssoSupportManager.isSupportSsoEnabled()).thenReturn(true);

    SsoProvider mockProvider = mock(SsoProvider.class);
    Client mockClient = mock(Client.class);
    when(ssoSupportManager.getSupportSsoProvider()).thenReturn(mockProvider);
    when(mockProvider.client()).thenReturn(mockClient);
    when(mockClient.getName()).thenReturn("oidc-support");

    // Mock the redirect action
    FoundAction mockAction = mock(FoundAction.class);
    when(mockAction.getLocation()).thenReturn("https://test-idp.com/authorize");

    CallContext mockCallContext = mock(CallContext.class);
    PlayWebContext mockWebContext = mock(PlayWebContext.class);
    when(mockCallContext.webContext()).thenReturn(mockWebContext);
    when(mockWebContext.getRequestCookies()).thenReturn(new java.util.ArrayList<>());

    // Create a spy to mock the buildCallContext method
    SupportAuthenticationController spyController = spy(controller);
    doReturn(mockCallContext).when(spyController).buildCallContext(any(Http.RequestHeader.class));

    when(mockClient.getRedirectionAction(any(CallContext.class)))
        .thenReturn(Optional.of(mockAction));

    // Act
    Result result = spyController.ssoSupport(request);

    // Assert
    assertNotNull(result);
    assertEquals(303, result.status()); // Redirect status
  }

  @Test
  public void testSsoSupportWithSsoDisabled() {
    // Arrange
    Http.Request request = mock(Http.Request.class);
    Http.Session session = mock(Http.Session.class);
    when(request.session()).thenReturn(session);
    when(ssoSupportManager.isSupportSsoEnabled()).thenReturn(false);

    // Act
    Result result = controller.ssoSupport(request);

    // Assert
    assertNotNull(result);
    assertEquals(303, result.status()); // Redirect status
    assertTrue(result.redirectLocation().orElse("").contains("/login-support"));
    assertTrue(result.redirectLocation().orElse("").contains("error_msg"));
  }
}
