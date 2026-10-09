package security;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import auth.sso.SsoManager;
import auth.sso.SsoProvider;
import auth.sso.oidc.OidcProvider;
import com.datahub.authentication.Authentication;
import com.typesafe.config.ConfigFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

public class SsoManagerProviderCacheTest {

  @Test
  public void reusesOidcProviderUntilClientIdChanges() throws Exception {
    final CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    final Authentication authentication = mock(Authentication.class);
    when(authentication.getCredentials()).thenReturn("Bearer test-token");
    when(httpClient.execute(any(HttpPost.class))).thenAnswer(invocation -> okResponse());

    // No auth.oidc.enabled path: UI-configured tenants refresh from GMS on every isSsoEnabled().
    final SsoManager ssoManager =
        new SsoManager(
            ConfigFactory.empty(),
            authentication,
            "http://localhost:8080/auth/getSsoSettings",
            httpClient);

    final String settings = ssoSettingsJson("test-client");
    final String updatedSettings = ssoSettingsJson("test-client-updated");

    try (MockedStatic<EntityUtils> entityUtils = mockStatic(EntityUtils.class)) {
      entityUtils
          .when(() -> EntityUtils.toString(any(HttpEntity.class)))
          .thenReturn(settings, settings, updatedSettings);

      assertTrue(ssoManager.isSsoEnabled());
      final SsoProvider<?> first = ssoManager.getSsoProvider();
      assertTrue(first instanceof OidcProvider);

      assertTrue(ssoManager.isSsoEnabled());
      final SsoProvider<?> second = ssoManager.getSsoProvider();
      assertSame(first, second);

      assertTrue(ssoManager.isSsoEnabled());
      final SsoProvider<?> third = ssoManager.getSsoProvider();
      assertNotNull(third);
      assertNotSame(first, third);
    }
  }

  private static CloseableHttpResponse okResponse() {
    final CloseableHttpResponse response = mock(CloseableHttpResponse.class);
    final StatusLine statusLine = mock(StatusLine.class);
    final HttpEntity entity = mock(HttpEntity.class);
    when(response.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);
    when(response.getEntity()).thenReturn(entity);
    return response;
  }

  private static String ssoSettingsJson(final String clientId) {
    return String.format(
        """
        {
          "baseUrl": "https://datahub.example.com",
          "oidcEnabled": true,
          "clientId": "%s",
          "clientSecret": "test-secret",
          "discoveryUri": "https://idp.example.com/.well-known/openid-configuration"
        }
        """,
        clientId);
  }
}
