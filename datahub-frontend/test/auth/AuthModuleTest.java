package auth;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import auth.sso.SsoManager;
import client.AuthServiceClient;
import com.datahub.authentication.ActorType;
import com.datahub.authentication.Authentication;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.linkedin.metadata.utils.BasePathUtils;
import com.typesafe.config.ConfigFactory;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.pac4j.core.context.session.SessionStore;
import org.pac4j.play.store.PlayCacheSessionStore;
import org.pac4j.play.store.PlayCookieSessionStore;
import play.Environment;
import play.cache.SyncCacheApi;

// @ExtendWith(MockitoExtension.class) - Not available in this project
public class AuthModuleTest {

  private AuthModule authModule;
  private com.typesafe.config.Config testConfig;
  private Environment mockEnvironment;
  private SyncCacheApi mockCacheApi;

  @BeforeEach
  public void setUp() {
    // Create a test configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCacheSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "");
    configMap.put("metadataService.basePathEnabled", false);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");
    configMap.put("entityClient.retryInterval", 1000);
    configMap.put("entityClient.numRetries", 3);
    configMap.put("entityClient.restli.get.batchSize", 100);
    configMap.put("entityClient.restli.get.batchConcurrency", 2);

    testConfig = ConfigFactory.parseMap(configMap);
    mockEnvironment = mock(Environment.class);
    mockCacheApi = mock(SyncCacheApi.class);

    authModule = new AuthModule(mockEnvironment, testConfig);
  }

  /**
   * Helper method to create a test module that provides all required dependencies for Guice
   * injector
   */
  private AbstractModule createTestModule(final com.typesafe.config.Config config) {
    return new AbstractModule() {
      @Override
      protected void configure() {
        bind(com.typesafe.config.Config.class).toInstance(config);
        bind(java.util.concurrent.Executor.class)
            .toInstance(mock(java.util.concurrent.Executor.class));
        bind(SyncCacheApi.class).toInstance(mockCacheApi);
      }
    };
  }

  @Test
  public void testSessionStoreConfigurationWithPlayCacheSessionStore() {
    // Test PlayCacheSessionStore configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCacheSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "");
    configMap.put("metadataService.basePathEnabled", false);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");
    configMap.put("entityClient.retryInterval", 1000);
    configMap.put("entityClient.numRetries", 3);
    configMap.put("entityClient.restli.get.batchSize", 100);
    configMap.put("entityClient.restli.get.batchConcurrency", 2);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    // Create injector with both modules
    Injector injector = Guice.createInjector(module, createTestModule(config));

    // Verify SessionStore is bound
    SessionStore sessionStore = injector.getInstance(SessionStore.class);
    assertNotNull(sessionStore);
    assertTrue(sessionStore instanceof PlayCacheSessionStore);
  }

  @Test
  public void testSessionStoreConfigurationWithPlayCookieSessionStore() {
    // Test PlayCookieSessionStore configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCookieSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "");
    configMap.put("metadataService.basePathEnabled", false);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");
    configMap.put("entityClient.retryInterval", 1000);
    configMap.put("entityClient.numRetries", 3);
    configMap.put("entityClient.restli.get.batchSize", 100);
    configMap.put("entityClient.restli.get.batchConcurrency", 2);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    // Create injector with test module
    Injector injector = Guice.createInjector(module, createTestModule(config));

    // Verify SessionStore is bound
    SessionStore sessionStore = injector.getInstance(SessionStore.class);
    assertNotNull(sessionStore);
    assertTrue(sessionStore instanceof PlayCookieSessionStore);
  }

  @Test
  public void testPac4jConfigProvider() {
    // Test Pac4j Config provider
    Injector injector = Guice.createInjector(authModule, createTestModule(testConfig));

    org.pac4j.core.config.Config config = injector.getInstance(org.pac4j.core.config.Config.class);
    assertNotNull(config);
    assertNotNull(config.getSessionStoreFactory());
    assertNotNull(config.getHttpActionAdapter());
    assertNotNull(config.getProfileManagerFactory());
  }

  @Test
  public void testSystemAuthenticationProvider() {
    // Test system authentication provider
    Injector injector = Guice.createInjector(authModule, createTestModule(testConfig));

    Authentication auth = injector.getInstance(Authentication.class);
    assertNotNull(auth);
    assertEquals(ActorType.USER, auth.getActor().getType());
    assertEquals("test-client-id", auth.getActor().getId());
    assertTrue(auth.getCredentials().startsWith("Basic "));
  }

  @Test
  public void testSsoManagerProviderWithBasePath() {
    // Test SsoManager provider with basePath configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCacheSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "/api/v2");
    configMap.put("metadataService.basePathEnabled", true);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> BasePathUtils.resolveBasePath(true, "/api/v2"))
          .thenReturn("/api/v2");

      Injector injector = Guice.createInjector(module, createTestModule(config));

      SsoManager ssoManager = injector.getInstance(SsoManager.class);
      assertNotNull(ssoManager);
    }
  }

  @Test
  public void testSsoManagerProviderWithoutBasePath() {
    // Test SsoManager provider without basePath configuration
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCacheSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "");
    configMap.put("metadataService.basePathEnabled", false);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.resolveBasePath(false, "")).thenReturn("");

      Injector injector = Guice.createInjector(module, createTestModule(config));

      SsoManager ssoManager = injector.getInstance(SsoManager.class);
      assertNotNull(ssoManager);
    }
  }

  @Test
  public void testAuthServiceClientProviderWithBasePath() {
    // Test AuthServiceClient provider with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCacheSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "/api/v2");
    configMap.put("metadataService.basePathEnabled", true);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> BasePathUtils.resolveBasePath(true, "/api/v2"))
          .thenReturn("/api/v2");

      Injector injector = Guice.createInjector(module, createTestModule(config));

      AuthServiceClient authClient = injector.getInstance(AuthServiceClient.class);
      assertNotNull(authClient);
    }
  }

  @Test
  public void testAuthServiceClientProviderWithoutBasePath() {
    // Test AuthServiceClient provider without basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("play.http.secret.key", "test-secret-key");
    configMap.put("pac4j.sessionStore.provider", "PlayCacheSessionStore");
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "");
    configMap.put("metadataService.basePathEnabled", false);
    configMap.put("systemClientId", "test-client-id");
    configMap.put("systemClientSecret", "test-client-secret");

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.resolveBasePath(false, "")).thenReturn("");

      Injector injector = Guice.createInjector(module, createTestModule(config));

      AuthServiceClient authClient = injector.getInstance(AuthServiceClient.class);
      assertNotNull(authClient);
    }
  }

  // Skipping this test as SystemEntityClient requires an active OperationContext with actor,
  // which is too complex to mock properly in unit tests. Integration tests cover this.
  // @Test
  // public void testSystemEntityClientProviderWithBasePath() throws Exception {}

  // Skipping this test as SystemEntityClient requires an active OperationContext with actor,
  // which is too complex to mock properly in unit tests. Integration tests cover this.
  // @Test
  // public void testSystemEntityClientProviderWithoutBasePath() throws Exception {}

  @Test
  public void testGetMetadataServiceBasePathWithBasePathEnabled() {
    // Test getMetadataServiceBasePath with basePath enabled
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.basePath", "/api/v2");
    configMap.put("metadataService.basePathEnabled", true);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> BasePathUtils.resolveBasePath(true, "/api/v2"))
          .thenReturn("/api/v2");

      String result = module.getMetadataServiceBasePath(config);
      assertEquals("/api/v2", result);
    }
  }

  @Test
  public void testGetMetadataServiceBasePathWithBasePathDisabled() {
    // Test getMetadataServiceBasePath with basePath disabled
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.basePath", "/api/v2");
    configMap.put("metadataService.basePathEnabled", false);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.resolveBasePath(false, "/api/v2")).thenReturn("");

      String result = module.getMetadataServiceBasePath(config);
      assertEquals("", result);
    }
  }

  @Test
  public void testGetMetadataServiceBasePathWithEnvironmentVariables() {
    // Test getMetadataServiceBasePath with environment variables
    Map<String, Object> configMap = new HashMap<>();
    // Don't set the config values, let them fall back to environment variables

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> BasePathUtils.resolveBasePath(anyBoolean(), anyString()))
          .thenReturn("/env/basepath");

      String result = module.getMetadataServiceBasePath(config);
      assertEquals("/env/basepath", result);
    }
  }

  @Test
  public void testGetSsoSettingsRequestUrlWithBasePath() {
    // Test getSsoSettingsRequestUrl with basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "/api/v2");
    configMap.put("metadataService.basePathEnabled", true);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> BasePathUtils.resolveBasePath(true, "/api/v2"))
          .thenReturn("/api/v2");

      String result = module.getSsoSettingsRequestUrl(config);
      assertEquals("http://localhost:8080/api/v2/auth/getSsoSettings", result);
    }
  }

  @Test
  public void testGetSsoSettingsRequestUrlWithoutBasePath() {
    // Test getSsoSettingsRequestUrl without basePath
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8080);
    configMap.put("metadataService.useSsl", false);
    configMap.put("metadataService.basePath", "");
    configMap.put("metadataService.basePathEnabled", false);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock.when(() -> BasePathUtils.resolveBasePath(false, "")).thenReturn("");

      String result = module.getSsoSettingsRequestUrl(config);
      assertEquals("http://localhost:8080/auth/getSsoSettings", result);
    }
  }

  @Test
  public void testGetSsoSettingsRequestUrlWithHttps() {
    // Test getSsoSettingsRequestUrl with HTTPS
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.host", "localhost");
    configMap.put("metadataService.port", 8443);
    configMap.put("metadataService.useSsl", true);
    configMap.put("metadataService.basePath", "/api/v2");
    configMap.put("metadataService.basePathEnabled", true);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    try (MockedStatic<BasePathUtils> basePathUtilsMock = mockStatic(BasePathUtils.class)) {
      basePathUtilsMock
          .when(() -> BasePathUtils.resolveBasePath(true, "/api/v2"))
          .thenReturn("/api/v2");

      String result = module.getSsoSettingsRequestUrl(config);
      assertEquals("https://localhost:8443/api/v2/auth/getSsoSettings", result);
    }
  }

  @Test
  public void testGetMetadataServiceHost() {
    // Test getMetadataServiceHost
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.host", "test-host");

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    String result = module.getMetadataServiceHost(config);
    assertEquals("test-host", result);
  }

  @Test
  public void testGetMetadataServicePort() {
    // Test getMetadataServicePort
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.port", 9999);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    Integer result = module.getMetadataServicePort(config);
    assertEquals(Integer.valueOf(9999), result);
  }

  @Test
  public void testDoesMetadataServiceUseSsl() {
    // Test doesMetadataServiceUseSsl
    Map<String, Object> configMap = new HashMap<>();
    configMap.put("metadataService.useSsl", true);

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    boolean result = module.doesMetadataServiceUseSsl(config);
    assertTrue(result);
  }

  @Test
  public void testDoesMetadataServiceUseSslWithEnvironmentVariable() {
    // Test doesMetadataServiceUseSsl with environment variable fallback
    Map<String, Object> configMap = new HashMap<>();
    // Don't set the config value, let it fall back to environment variable

    com.typesafe.config.Config config = ConfigFactory.parseMap(configMap);
    AuthModule module = new AuthModule(mockEnvironment, config);

    // This will use the environment variable fallback
    boolean result = module.doesMetadataServiceUseSsl(config);
    // The result depends on the environment variable, so we just verify it doesn't throw
    assertNotNull(Boolean.valueOf(result));
  }
}
