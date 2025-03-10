package security;

import static org.junit.jupiter.api.Assertions.*;

import auth.GuestAuthenticationConfigs;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junitpioneer.jupiter.ClearEnvironmentVariable;
import org.junitpioneer.jupiter.SetEnvironmentVariable;

@TestInstance(TestInstance.Lifecycle.PER_METHOD)
@SetEnvironmentVariable(key = "DATAHUB_SECRET", value = "test")
@SetEnvironmentVariable(key = "KAFKA_BOOTSTRAP_SERVER", value = "")
@SetEnvironmentVariable(key = "DATAHUB_ANALYTICS_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_ENABLED", value = "true")
@SetEnvironmentVariable(key = "AUTH_OIDC_JIT_PROVISIONING_ENABLED", value = "false")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_ID", value = "testclient")
@SetEnvironmentVariable(key = "AUTH_OIDC_CLIENT_SECRET", value = "testsecret")
@SetEnvironmentVariable(key = "AUTH_VERBOSE_LOGGING", value = "true")
class GuestAuthenticationConfigsTest {

  @BeforeEach
  @ClearEnvironmentVariable(key = "GUEST_AUTHENTICATION_ENABLED")
  @ClearEnvironmentVariable(key = "GUEST_AUTHENTICATION_USER")
  @ClearEnvironmentVariable(key = "GUEST_AUTHENTICATION_PATH")
  public void clearConfigCache() {
    ConfigFactory.invalidateCaches();
  }

  @Test
  public void testGuestConfigDisabled() {
    Config config = ConfigFactory.load();
    GuestAuthenticationConfigs guestAuthConfig = new GuestAuthenticationConfigs(config);
    assertFalse(guestAuthConfig.isGuestEnabled());
  }

  @Test
  @SetEnvironmentVariable(key = "GUEST_AUTHENTICATION_ENABLED", value = "true")
  public void testGuestConfigEnabled() {
    Config config = ConfigFactory.load();
    GuestAuthenticationConfigs guestAuthConfig = new GuestAuthenticationConfigs(config);
    assertTrue(guestAuthConfig.isGuestEnabled());
    assertEquals("guest", guestAuthConfig.getGuestUser());
    assertEquals("/public", guestAuthConfig.getGuestPath());
  }

  @Test
  @SetEnvironmentVariable(key = "GUEST_AUTHENTICATION_ENABLED", value = "true")
  @SetEnvironmentVariable(key = "GUEST_AUTHENTICATION_USER", value = "publicUser")
  @SetEnvironmentVariable(key = "GUEST_AUTHENTICATION_PATH", value = "/publicPath")
  public void testGuestConfigWithUserEnabled() {
    Config config = ConfigFactory.load();
    GuestAuthenticationConfigs guestAuthConfig = new GuestAuthenticationConfigs(config);
    assertTrue(guestAuthConfig.isGuestEnabled());
    assertEquals("publicUser", guestAuthConfig.getGuestUser());
    assertEquals("/publicPath", guestAuthConfig.getGuestPath());
  }
}
