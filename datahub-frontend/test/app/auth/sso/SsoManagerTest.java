package app.auth.sso;

import auth.sso.SsoManager;
import auth.sso.oidc.OidcProvider;
import com.datahub.authentication.Authentication;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.json.JSONObject;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static auth.AuthUtils.*;
import static org.mockito.Mockito.*;
import static org.testng.Assert.*;


public class SsoManagerTest {

  private static final String SSO_SETTINGS_REQUEST_URL = "http://localhost:9002/sso/settings";
  private static final String BASE_URL_VALUE = "http://localhost:9002";
  private static final String CLIENT_ID_VALUE = "clientId";
  private static final String CLIENT_SECRET_VALUE = "clientSecret";
  private static final String DISCOVERY_URI_VALUE = "https://idp.com/.well-known/openid-configuration";
  private static final String AUTH_BASE_URL_CONFIG_PATH = "auth.baseUrl";
  public static final String OIDC_CLIENT_ID_CONFIG_PATH = "auth.oidc.clientId";
  public static final String OIDC_CLIENT_SECRET_CONFIG_PATH = "auth.oidc.clientSecret";
  public static final String OIDC_DISCOVERY_URI_CONFIG_PATH = "auth.oidc.discoveryUri";
  public static final String OIDC_ENABLED_CONFIG_PATH = "auth.oidc.enabled";

  private SsoManager _ssoManager;
  private CloseableHttpResponse _httpResponse;

  private JSONObject _jsonResponse;

  @BeforeMethod
  public void setup() throws Exception {
    _jsonResponse = new JSONObject();
    _jsonResponse.put(BASE_URL, BASE_URL_VALUE);
    _jsonResponse.put(OIDC_ENABLED, true);
    _jsonResponse.put(CLIENT_ID, CLIENT_ID_VALUE);
    _jsonResponse.put(CLIENT_SECRET, CLIENT_SECRET_VALUE);
    _jsonResponse.put(DISCOVERY_URI, DISCOVERY_URI_VALUE);

    Authentication authentication = mock(Authentication.class);
    CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
    _httpResponse = mock(CloseableHttpResponse.class);
    when(httpClient.execute(any(HttpPost.class))).thenReturn(_httpResponse);
    StatusLine statusLine = mock(StatusLine.class);
    when(_httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

    _ssoManager = new SsoManager(authentication, SSO_SETTINGS_REQUEST_URL, httpClient);
  }

  @Test
  public void testGetSsoProviderDefault() {
    assertNull(_ssoManager.getSsoProvider());
  }

  @Test
  public void testIsSsoEnabledNotFoundResponse() {
    StatusLine statusLine = mock(StatusLine.class);
    when(_httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

    assertFalse(_ssoManager.isSsoEnabled());
    assertNull(_ssoManager.getSsoProvider());
  }

  @Test
  public void testRefreshSsoProviderNoResponse() {
    when(_httpResponse.getEntity()).thenReturn(null);

    assertFalse(_ssoManager.isSsoEnabled());
    assertNull(_ssoManager.getSsoProvider());
  }

  @Test
  public void testIsSsoEnabledEmptyResponse() throws Exception {
    when(_httpResponse.getEntity()).thenReturn(new StringEntity("{}"));

    assertFalse(_ssoManager.isSsoEnabled());
    assertNull(_ssoManager.getSsoProvider());
  }

  @Test
  public void testIsSsoEnabled() throws Exception {
    when(_httpResponse.getEntity()).thenReturn(new StringEntity(_jsonResponse.toString()));

    assertTrue(_ssoManager.isSsoEnabled());
    assertTrue(_ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testIsSsoEnabledMissingSsoField() throws Exception {
    _jsonResponse.remove(BASE_URL);
    when(_httpResponse.getEntity()).thenReturn(new StringEntity(_jsonResponse.toString()));

    assertFalse(_ssoManager.isSsoEnabled());
    assertNull(_ssoManager.getSsoProvider());
  }

  @Test
  public void testIsSsoEnabledMissingOidcField() throws Exception {
    _jsonResponse.remove(CLIENT_ID);
    when(_httpResponse.getEntity()).thenReturn(new StringEntity(_jsonResponse.toString()));

    assertFalse(_ssoManager.isSsoEnabled());
    assertNull(_ssoManager.getSsoProvider());
  }

  @Test
  public void testRefreshSsoProvider() throws Exception {
    when(_httpResponse.getEntity()).thenReturn(new StringEntity(_jsonResponse.toString()));

    assertTrue(_ssoManager.isSsoEnabled());
    assertTrue(_ssoManager.getSsoProvider() instanceof OidcProvider);
  }

  @Test
  public void testParseConfigsMissingSsoValue() {
    com.typesafe.config.Config configs = mock(com.typesafe.config.Config.class);
    when(configs.hasPath(OIDC_ENABLED_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_ENABLED_CONFIG_PATH)).thenReturn("true");
    when(configs.hasPath(OIDC_CLIENT_ID_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_CLIENT_ID_CONFIG_PATH)).thenReturn(CLIENT_ID_VALUE);
    when(configs.hasPath(OIDC_CLIENT_SECRET_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_CLIENT_SECRET_CONFIG_PATH)).thenReturn(CLIENT_SECRET_VALUE);
    when(configs.hasPath(OIDC_DISCOVERY_URI_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_DISCOVERY_URI_CONFIG_PATH)).thenReturn(DISCOVERY_URI_VALUE);
    when(_httpResponse.getEntity()).thenReturn(null);

    _ssoManager.initializeSsoProvider(configs);
    assertFalse(_ssoManager.isSsoEnabled());
  }

  @Test
  public void testParseConfigsMissingOidcValue() {
    com.typesafe.config.Config configs = mock(com.typesafe.config.Config.class);
    when(configs.hasPath(AUTH_BASE_URL_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(AUTH_BASE_URL_CONFIG_PATH)).thenReturn(BASE_URL_VALUE);
    when(configs.hasPath(OIDC_ENABLED_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_ENABLED_CONFIG_PATH)).thenReturn("true");
    when(configs.hasPath(OIDC_CLIENT_ID_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_CLIENT_ID_CONFIG_PATH)).thenReturn(CLIENT_ID_VALUE);
    when(configs.hasPath(OIDC_CLIENT_SECRET_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_CLIENT_SECRET_CONFIG_PATH)).thenReturn(CLIENT_SECRET_VALUE);
    when(_httpResponse.getEntity()).thenReturn(null);

    _ssoManager.initializeSsoProvider(configs);
    assertFalse(_ssoManager.isSsoEnabled());
  }

  @Test
  public void testParseConfigs() {
    com.typesafe.config.Config configs = mock(com.typesafe.config.Config.class);
    when(configs.hasPath(AUTH_BASE_URL_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(AUTH_BASE_URL_CONFIG_PATH)).thenReturn(BASE_URL_VALUE);
    when(configs.hasPath(OIDC_ENABLED_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_ENABLED_CONFIG_PATH)).thenReturn("true");
    when(configs.hasPath(OIDC_CLIENT_ID_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_CLIENT_ID_CONFIG_PATH)).thenReturn(CLIENT_ID_VALUE);
    when(configs.hasPath(OIDC_CLIENT_SECRET_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_CLIENT_SECRET_CONFIG_PATH)).thenReturn(CLIENT_SECRET_VALUE);
    when(configs.hasPath(OIDC_DISCOVERY_URI_CONFIG_PATH)).thenReturn(true);
    when(configs.getString(OIDC_DISCOVERY_URI_CONFIG_PATH)).thenReturn(DISCOVERY_URI_VALUE);
    StatusLine statusLine = mock(StatusLine.class);
    when(_httpResponse.getStatusLine()).thenReturn(statusLine);
    when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_NOT_FOUND);

    _ssoManager.initializeSsoProvider(configs);
    assertTrue(_ssoManager.isSsoEnabled());
    assertTrue(_ssoManager.getSsoProvider() instanceof OidcProvider);
  }
}
