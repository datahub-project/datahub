package com.linkedin.gms.factory.ratelimit;

import static org.testng.Assert.assertEquals;

import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.spring.YamlPropertySourceFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.testng.annotations.Test;

/**
 * Integration test for the real {@code @PropertySource} precedence behind the Tier-1/Tier-2 design.
 *
 * <p>In the running GMS, the bundled defaults ({@code application.yaml}) are declared on {@code
 * CommonApplicationConfig} and the mounted override ({@code RATE_LIMITS_CONFIG_FILE}) is declared
 * on {@code RateLimitEngineFactory}, which is component-scanned — so its {@code @PropertySource} is
 * processed AFTER the root config's, and Spring gives the later-processed source higher precedence.
 * A unit binder test can force precedence, but only a context that actually processes two
 * {@code @PropertySource} configs in that order proves the override wins. This reproduces that
 * ordering (defaults registered first, override second) using the real {@code application.yaml} and
 * the same {@link YamlPropertySourceFactory} production uses.
 */
public class RateLimitConfigOverridePrecedenceTest {

  @Configuration
  @PropertySource(value = "classpath:/application.yaml", factory = YamlPropertySourceFactory.class)
  static class BundledDefaults {}

  @Configuration
  @PropertySource(
      value = "classpath:/rate-limit-precedence-override.yaml",
      factory = YamlPropertySourceFactory.class)
  static class MountedOverride {}

  private static RateLimitProperties bindLayered() {
    try (AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext()) {
      // Defaults first, override second — the same relative processing order as application.yaml
      // (root config) vs the scanned RateLimitEngineFactory override.
      ctx.register(BundledDefaults.class, MountedOverride.class);
      ctx.refresh();
      return Binder.get(ctx.getEnvironment())
          .bind("datahub.gms.rate-limits", RateLimitProperties.class)
          .get();
    }
  }

  @Test
  public void testMountedOverrideScalarWinsOverBundledApplicationYaml() {
    // application.yaml default is 60; the mounted override sets 120 → the override must win.
    assertEquals(bindLayered().getMinRetryAfterSeconds(), 120);
  }

  @Test
  public void testUnsetScalarKeepsBundledDefault() {
    // The override does not set retryAfterJitterPercent, so the application.yaml default (10)
    // stands
    // — proving the override is a per-key overlay, not a wholesale replacement.
    assertEquals(bindLayered().getRetryAfterJitterPercent(), 10);
  }
}
