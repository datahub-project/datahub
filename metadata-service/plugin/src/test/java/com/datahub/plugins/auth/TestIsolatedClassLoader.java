package com.datahub.plugins.auth;

import com.datahub.authentication.Authentication;
import com.datahub.authentication.AuthenticationException;
import com.datahub.authentication.AuthenticationRequest;
import com.datahub.authentication.AuthenticatorContext;
import com.datahub.authorization.AuthorizationRequest;
import com.datahub.authorization.AuthorizerContext;
import com.datahub.plugins.PluginConstant;
import com.datahub.plugins.auth.authentication.Authenticator;
import com.datahub.plugins.auth.authorization.Authorizer;
import com.datahub.plugins.auth.configuration.AuthenticatorPluginConfig;
import com.datahub.plugins.auth.configuration.AuthorizerPluginConfig;
import com.datahub.plugins.common.PluginConfig;
import com.datahub.plugins.common.PluginPermissionManager;
import com.datahub.plugins.common.PluginType;
import com.datahub.plugins.common.SecurityMode;
import com.datahub.plugins.configuration.Config;
import com.datahub.plugins.configuration.ConfigProvider;
import com.datahub.plugins.factory.PluginConfigFactory;
import com.datahub.plugins.loader.IsolatedClassLoader;
import com.datahub.plugins.loader.PluginPermissionManagerImpl;
import com.google.common.collect.ImmutableMap;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test case covers below scenarios
 * 1. Loading plugin configuration and validating the loaded configuration against the expected configuration.
 *    This scenario is covered in @{link com.datahub.plugins.auth.TestIsolatedClassLoader#testConfigurationLoading()}
 *    test
 *
 * 2. Plugin name should be unique in config.yaml. The plugin framework should raise error if more than one plugin
 * has the same name.
 *    This scenario is covered in @{link com.datahub.plugins.auth.TestIsolatedClassLoader#testDuplicatePluginName()}
 *    test
 *
 * 3. Developer can provide plugin jar file name in config.yaml.
 *    This scenario is covered in @{link com.datahub.plugins.auth.TestIsolatedClassLoader#testJarFileName()} test
 *
 * 4. Test @{link com.datahub.plugins.auth.TestIsolatedClassLoader#testAuthenticatorPlugin()} covers the valid
 * authenticator plugin execution.
 *    Plugin used in this test-case is metadata-service/plugin/src/test/sample-test-plugins/src/main/java/com/datahub
 *    /plugins/test/TestAuthenticator.java
 *
 * 5. Test @{link com.datahub.plugins.auth.TestIsolatedClassLoader#testAuthorizerPlugin()} covers the valid
 * authorizer plugin execution
 *    Plugin used in this test-case is metadata-service/plugin/src/test/sample-test-plugins/src/main/java/com/datahub
 *    /plugins/test/TestAuthorizer.java
 *
 * 6. The plugin framework should raise error if authenticator plugin is configured as authorizer plugin or vice-versa.
 *    This scenario is covered in @{link com.datahub.plugins.auth.TestIsolatedClassLoader#testIncorrectImplementation
 *    ()}.
 *    The test case tries to load authorizer plugin as authenticator plugin
 */
class TestIsolatedClassLoader {

  @BeforeClass
  public void setSecurityManager() {
    System.setSecurityManager(new SecurityManager());
  }

  @Test
  public void testDuplicatePluginName() {
    Path yamlConfig = Paths.get("src", "test", "resources", "duplicate-plugin-name");
    try {
      (new ConfigProvider(yamlConfig)).load();
    } catch (RuntimeException e) {
      assert e.getCause()
          .getMessage()
          .contains(
              "Duplicate entry of apache-ranger-authorizer is found in plugins. plugins should not contain duplicate");
    }
  }

  @Test
  public void testJarFileName() throws Exception {
    Path configPath = Paths.get("src", "test", "resources", "plugin-jar-from-jarFileName");

    Path authenticatorPluginJarPath = Paths.get(configPath.toAbsolutePath().toString(), "apache-ranger-authenticator",
        "apache-ranger-authenticator-v1.0.1.jar");
    Config config = (new ConfigProvider(configPath)).load().orElseThrow(() -> new Exception("Should not be empty"));
    List<PluginConfig> pluginConfig = (new PluginConfigFactory(config)).loadPluginConfigs(PluginType.AUTHENTICATOR);
    pluginConfig.forEach((pluginConfigWithJar) -> {
      assert pluginConfigWithJar.getPluginJarPath().equals(authenticatorPluginJarPath);
    });

    Path authorizerPluginJarPath = Paths.get(configPath.toAbsolutePath().toString(), "apache-ranger-authorizer",
        "apache-ranger-authorizer-v2.0.1.jar");
    List<PluginConfig> authorizerPluginConfigs =
        (new PluginConfigFactory(config)).loadPluginConfigs(PluginType.AUTHORIZER);

    authorizerPluginConfigs.forEach((pluginConfigWithJar) -> {
      assert pluginConfigWithJar.getPluginJarPath().equals(authorizerPluginJarPath);
    });
  }

  public static Path getSamplePluginDirectory() {
    // plugin directory
    return Paths.get("src", "test", "resources", "sample-plugins").toAbsolutePath();
  }

  public static Path getSamplePluginJar() {
    // plugin jar path
    return Paths.get(getSamplePluginDirectory().toString(), "sample-plugins.jar");
  }

  public static Optional<Map<String, Object>> getConfigs() {
    // plugin configs
    return Optional.of(ImmutableMap.of("key1", "value1", "key2", "value2", "key3", "value3"));
  }

  public static AuthorizerPluginConfig getAuthorizerPluginConfig() {
    AuthorizerPluginConfig authorizerPluginConfig = new AuthorizerPluginConfig();
    authorizerPluginConfig.setClassName("com.datahub.plugins.test.TestAuthorizer");
    authorizerPluginConfig.setConfigs(getConfigs());
    authorizerPluginConfig.setPluginHomeDirectory(getSamplePluginDirectory());
    authorizerPluginConfig.setPluginJarPath(getSamplePluginJar());
    // plugin name
    authorizerPluginConfig.setName("sample-plugin-authorizer");

    return authorizerPluginConfig;
  }

  public static AuthenticatorPluginConfig getAuthenticatorPluginConfig() {
    AuthenticatorPluginConfig authenticatorPluginConfig = new AuthenticatorPluginConfig();
    authenticatorPluginConfig.setClassName("com.datahub.plugins.test.TestAuthenticator");
    authenticatorPluginConfig.setConfigs(getConfigs());
    authenticatorPluginConfig.setPluginHomeDirectory(getSamplePluginDirectory());
    authenticatorPluginConfig.setPluginJarPath(getSamplePluginJar());
    // plugin name
    authenticatorPluginConfig.setName("sample-plugin-authenticator");
    return authenticatorPluginConfig;
  }

  @Test
  public void testAuthenticatorPlugin() throws ClassNotFoundException, AuthenticationException {
    // authenticator plugin config instance
    AuthenticatorPluginConfig authenticatorPluginConfig = getAuthenticatorPluginConfig();
    // create IsolatedClassLoader
    PluginPermissionManager permissionManager = new PluginPermissionManagerImpl(SecurityMode.RESTRICTED);
    IsolatedClassLoader isolatedClassLoader = new IsolatedClassLoader(permissionManager, authenticatorPluginConfig);
    // initiate and invoke the init and authenticate methods
    Authenticator authenticator = (Authenticator) isolatedClassLoader.instantiatePlugin(Authenticator.class);
    AuthenticatorContext authenticatorContext = new AuthenticatorContext(
        ImmutableMap.of(PluginConstant.PLUGIN_HOME, authenticatorPluginConfig.getPluginHomeDirectory().toString()));
    AuthenticationRequest request = new AuthenticationRequest(ImmutableMap.of("foo", "bar"));
    authenticator.init(authenticatorPluginConfig.getConfigs().orElse(new HashMap<>()), authenticatorContext);

    Authentication authentication = authenticator.authenticate(request);
    assert authentication.getActor().getId().equals("fake");
  }

  @Test
  public void testAuthorizerPlugin() throws ClassNotFoundException, AuthenticationException {
    // authenticator plugin config instance
    AuthorizerPluginConfig authorizerPluginConfig = getAuthorizerPluginConfig();
    // create IsolatedClassLoader
    PluginPermissionManager permissionManager = new PluginPermissionManagerImpl(SecurityMode.RESTRICTED);
    IsolatedClassLoader isolatedClassLoader = new IsolatedClassLoader(permissionManager, authorizerPluginConfig);
    // initiate and invoke the init and authenticate methods
    Authorizer authorizer = (Authorizer) isolatedClassLoader.instantiatePlugin(Authorizer.class);
    AuthorizerContext authorizerContext = new AuthorizerContext(
        ImmutableMap.of(PluginConstant.PLUGIN_HOME, authorizerPluginConfig.getPluginHomeDirectory().toString()), null);
    AuthorizationRequest authorizationRequest = new AuthorizationRequest("urn:li:user:fake", "test", Optional.empty());
    authorizer.init(authorizerPluginConfig.getConfigs().orElse(new HashMap<>()), authorizerContext);
    assert authorizer.authorize(authorizationRequest).getMessage().equals("fake message");
  }

  @Test
  public void testIncorrectImplementation() {
    AuthorizerPluginConfig authorizerPluginConfig = getAuthorizerPluginConfig();
    // create IsolatedClassLoader
    PluginPermissionManager permissionManager = new PluginPermissionManagerImpl(SecurityMode.RESTRICTED);
    IsolatedClassLoader isolatedClassLoader = new IsolatedClassLoader(permissionManager, authorizerPluginConfig);
    // initiate and invoke the init and authenticate methods
    try {
      // Authorizer configuration is provided, however here we were expecting that plugin should be of type
      // Authenticator.class
      Authorizer authorizer = (Authorizer) isolatedClassLoader.instantiatePlugin(Authenticator.class);
      assert authorizer != null;
    } catch (RuntimeException | ClassNotFoundException e) {
      assert e.getCause() instanceof java.lang.InstantiationException;
    }
  }

  @Test
  public void testLenientMode() throws ClassNotFoundException, AuthenticationException {
    // authenticator plugin config instance
    AuthenticatorPluginConfig authenticatorPluginConfig = getAuthenticatorPluginConfig();
    authenticatorPluginConfig.setClassName("com.datahub.plugins.test.TestLenientModeAuthenticator");
    // create IsolatedClassLoader
    PluginPermissionManager permissionManager = new PluginPermissionManagerImpl(SecurityMode.LENIENT);
    IsolatedClassLoader isolatedClassLoader = new IsolatedClassLoader(permissionManager, authenticatorPluginConfig);
    // initiate and invoke the init and authenticate methods
    Authenticator authenticator = (Authenticator) isolatedClassLoader.instantiatePlugin(Authenticator.class);
    authenticator.init(authenticatorPluginConfig.getConfigs().orElse(new HashMap<>()), null);
    AuthenticationRequest request = new AuthenticationRequest(ImmutableMap.of("foo", "bar"));
    assert authenticator.authenticate(request) != null;
  }
}
