package auth;

import static org.junit.jupiter.api.Assertions.*;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@SetEnvironmentVariable(
    key = "DATAHUB_SECRET",
    value = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef")
@SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVER", value = "")
@SetEnvironmentVariable(key = "DATAHUB_ANALYTICS_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_ENABLED", value = "true")
@SetEnvironmentVariable(key = "AUTH_OIDC_JIT_PROVISIONING_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_ID", value = "testclient")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_SECRET", value = "testsecret")
@SetEnvironmentVariable(key = "AUTH_VERBOSE_LOGGING", value = "true")
class ProxyConfigsTest {

  @BeforeEach
  @ClearEnvironmentVariable(key = "AUTH_PROXY_ENABLED")
  @ClearEnvironmentVariable(key = "AUTH_PROXY_USER_HEADER")
  @ClearEnvironmentVariable(key = "AUTH_PROXY_JIT_PROVISIONING_ENABLED")
  public void clearConfigCache() {
    ConfigFactory.invalidateCaches();
  }

  @Test
  public void testProxyConfigDisabledByDefault() {
    Config config = ConfigFactory.load();
    ProxyConfigs proxyConfigs = new ProxyConfigs(config);
    assertFalse(proxyConfigs.isEnabled());
  }

  @Test
  @SetEnvironmentVariable(key = "AUTH_PROXY_ENABLED", value = "true")
  public void testProxyConfigEnabledWithDefaults() {
    Config config = ConfigFactory.load();
    ProxyConfigs proxyConfigs = new ProxyConfigs(config);
    assertTrue(proxyConfigs.isEnabled());
    assertEquals("X-Forwarded-User", proxyConfigs.getUserHeader());
    assertTrue(proxyConfigs.isJitProvisioningEnabled());
  }

  @Test
  @SetEnvironmentVariable(key = "AUTH_PROXY_ENABLED", value = "true")
  @SetEnvironmentVariable(key = "AUTH_PROXY_USER_HEADER", value = "X-Remote-User")
  @SetEnvironmentVariable(key = "AUTH_PROXY_JIT_PROVISIONING_ENABLED", value = "false")
  public void testProxyConfigWithCustomValues() {
    Config config = ConfigFactory.load();
    ProxyConfigs proxyConfigs = new ProxyConfigs(config);
    assertTrue(proxyConfigs.isEnabled());
    assertEquals("X-Remote-User", proxyConfigs.getUserHeader());
    assertFalse(proxyConfigs.isJitProvisioningEnabled());
  }
}
