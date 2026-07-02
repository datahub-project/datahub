package com.linkedin.metadata.config.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Map;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.MapPropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;
import org.testng.annotations.Test;

/**
 * Proves the config-loading contract behind the per-tenant design: rate-limit settings live in a
 * YAML file with {@code ${ENV:default}} placeholders, and Spring resolves those placeholders
 * against the environment at bind time. That is what lets a single {@code rate-limit-config.yaml}
 * be shared across every deployment — each tenant sets different env values (e.g. {@code
 * RATE_LIMITS_SCOPED_SDK_CAPACITY}) and gets a different bound {@link RateLimitProperties}, with no
 * per-tenant duplication of the file. Loading via {@link YamlPropertySourceLoader} + {@link Binder}
 * exercises the exact same resolution path Spring uses for {@code application.yaml}.
 */
public class RateLimitPropertiesEnvBindingTest {

  private static final String RESOURCE = "rate-limit-env-binding-test.yaml";
  private static final String PREFIX = "datahub.gms.rateLimits";

  /**
   * Binds {@link RateLimitProperties} from the test YAML with the given env vars overlaid at
   * highest precedence — modelling what each deployment's pod environment supplies.
   */
  private static RateLimitProperties bindWithEnv(Map<String, Object> env) {
    StandardEnvironment environment = new StandardEnvironment();
    if (!env.isEmpty()) {
      environment.getPropertySources().addFirst(new MapPropertySource("test-env", env));
    }
    try {
      new YamlPropertySourceLoader()
          .load("rate-limit-test", new ClassPathResource(RESOURCE))
          .forEach(environment.getPropertySources()::addLast);
    } catch (Exception e) {
      throw new IllegalStateException("failed to load " + RESOURCE, e);
    }
    return Binder.get(environment).bind(PREFIX, RateLimitProperties.class).get();
  }

  @Test
  public void testScopedSizesFallBackToFileDefaultsWhenEnvUnset() {
    RateLimitProperties config = bindWithEnv(Map.of());
    assertEquals(config.getScoped().getSdk().getCapacity(), 500);
    assertEquals(config.getScoped().getActor().getCapacity(), 2000);
    assertFalse(config.getScoped().isEnabled());
  }

  @Test
  public void testScopedSdkCapacityComesFromEnv() {
    RateLimitProperties config = bindWithEnv(Map.of("RATE_LIMITS_SCOPED_SDK_CAPACITY", "100"));
    assertEquals(config.getScoped().getSdk().getCapacity(), 100);
    assertEquals(config.getScoped().getSdk().getRefillTokens(), 100);
    // An unrelated bucket keeps its file default — the env var only touches what it names.
    assertEquals(config.getScoped().getActor().getCapacity(), 2000);
  }

  @Test
  public void testDifferentTenantsGetDifferentLimitsFromSameFile() {
    // Same file, different env only: tenant A gets a tight SDK quota, tenant B a generous one.
    RateLimitProperties tenantA = bindWithEnv(Map.of("RATE_LIMITS_SCOPED_SDK_CAPACITY", "100"));
    RateLimitProperties tenantB = bindWithEnv(Map.of("RATE_LIMITS_SCOPED_SDK_CAPACITY", "1000"));
    assertEquals(tenantA.getScoped().getSdk().getCapacity(), 100);
    assertEquals(tenantB.getScoped().getSdk().getCapacity(), 1000);
  }

  @Test
  public void testScopedEnabledToggleFromEnv() {
    assertFalse(bindWithEnv(Map.of()).getScoped().isEnabled());
    assertTrue(bindWithEnv(Map.of("RATE_LIMITS_SCOPED_ENABLED", "true")).getScoped().isEnabled());
  }

  @Test
  public void testHeavyResolverMapBindsFromFile() {
    // A map can't be expressed as a scalar env var, so heavy resolvers ride in the file itself and
    // must still bind cleanly alongside the env-driven scalars.
    RateLimitProperties.BucketLimits heavy =
        bindWithEnv(Map.of()).getScoped().getHeavyResolvers().get("searchAcrossEntities");
    assertEquals(heavy.getCapacity(), 100);
    assertEquals(heavy.getRefillTokens(), 100);
  }

  @Test
  public void testHeavyResolversEmptyByDefaultWhenAbsent() {
    // heavyResolvers is optional; when no block is present the map is empty (and non-null, so the
    // scoped chain never NPEs looking one up).
    Map<String, RateLimitProperties.BucketLimits> resolvers =
        new RateLimitProperties.ScopedLimits().getHeavyResolvers();
    assertTrue(resolvers.isEmpty());
  }

  @Test
  public void testAllHeavyResolversBind() {
    // Every entry in the map binds — not just the first.
    Map<String, RateLimitProperties.BucketLimits> resolvers =
        bindWithEnv(Map.of()).getScoped().getHeavyResolvers();
    assertEquals(resolvers.size(), 2);
    assertTrue(resolvers.containsKey("searchAcrossEntities"));
    assertTrue(resolvers.containsKey("getEntities"));
  }

  @Test
  public void testHeavyResolverPeriodDefaultsTo60WhenOmitted() {
    // A resolver entry may omit refillPeriodSeconds; it must default to 60, not 0 (a 0-second
    // period would be an invalid/degenerate bucket).
    RateLimitProperties.BucketLimits heavy =
        bindWithEnv(Map.of()).getScoped().getHeavyResolvers().get("getEntities");
    assertEquals(heavy.getRefillPeriodSeconds(), 60);
  }

  @Test
  public void testHeavyResolverRefillTokensIndependentOfCapacity() {
    // Per-resolver capacity and refillTokens are independently configurable.
    RateLimitProperties.BucketLimits heavy =
        bindWithEnv(Map.of()).getScoped().getHeavyResolvers().get("getEntities");
    assertEquals(heavy.getCapacity(), 250);
    assertEquals(heavy.getRefillTokens(), 50);
  }

  @Test
  public void testHeavyResolverExemptSystemActorBinds() {
    // The optional exemptSystemActor flag binds per resolver, defaulting to false when unset.
    Map<String, RateLimitProperties.BucketLimits> resolvers =
        bindWithEnv(Map.of()).getScoped().getHeavyResolvers();
    assertTrue(resolvers.get("getEntities").isExemptSystemActor());
    assertFalse(resolvers.get("searchAcrossEntities").isExemptSystemActor());
  }
}
