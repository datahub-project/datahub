package com.linkedin.metadata.config.ratelimit;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertThrows;
import static org.testng.Assert.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import org.springframework.mock.env.MockEnvironment;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RateLimitEffectiveConfigTest {

  @AfterMethod
  public void tearDown() {
    RateLimitEffectiveConfig.reset();
  }

  @Test
  public void overlayFileEnablesEndpoint() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV,
        "rate-limit-loader-enable-endpoint.yaml");
    assertTrue(RateLimitEffectiveConfig.get(environment).getEndpoint().isEnabled());
  }

  @Test
  public void jsonEnablesScoped() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_JSON_ENV, "{\"scoped\":{\"enabled\":true}}");
    assertTrue(RateLimitEffectiveConfig.get(environment).getScoped().isEnabled());
  }

  @Test
  public void defaultClasspathFileLeavesDistributedFlagsOff() {
    RateLimitProperties config = RateLimitEffectiveConfig.get(new MockEnvironment());
    assertFalse(config.getEndpoint().isEnabled());
    assertFalse(config.getScoped().isEnabled());
  }

  @Test
  public void jsonOverridesFileEndpointFlag() throws Exception {
    Path yaml = Files.createTempFile("rate-limits-enable-endpoint", ".yaml");
    Files.writeString(yaml, "endpoint:\n  enabled: true\n");
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV, yaml.toAbsolutePath().toString());
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_JSON_ENV, "{\"endpoint\":{\"enabled\":false}}");
    assertFalse(RateLimitEffectiveConfig.get(environment).getEndpoint().isEnabled());
  }

  @Test
  public void boundEnvironmentFlagEnablesEndpointWithoutOverlay() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty("datahub.gms.rateLimits.endpoint.enabled", "true");
    assertTrue(RateLimitEffectiveConfig.get(environment).getEndpoint().isEnabled());
  }

  @Test
  public void sameEnvironmentReturnsCachedInstance() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_JSON_ENV, "{\"scoped\":{\"enabled\":true}}");
    RateLimitProperties first = RateLimitEffectiveConfig.get(environment);
    RateLimitProperties second =
        RateLimitEffectiveConfig.get(environment, new RateLimitProperties());
    assertSame(first, second);
  }

  @Test
  public void missingMountedFileFailsGet() {
    MockEnvironment environment = new MockEnvironment();
    environment.setProperty(
        RateLimitConfigLoader.RATE_LIMITS_CONFIG_FILE_ENV,
        "file:/tmp/datahub-missing-rate-limits-does-not-exist.yaml");
    assertThrows(IllegalStateException.class, () -> RateLimitEffectiveConfig.get(environment));
  }
}
