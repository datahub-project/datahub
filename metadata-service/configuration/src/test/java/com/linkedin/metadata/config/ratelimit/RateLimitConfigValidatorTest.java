package com.linkedin.metadata.config.ratelimit;

import static org.testng.Assert.assertThrows;

import java.util.List;
import org.testng.annotations.Test;

/**
 * Validation coverage for {@link RateLimitConfigValidator} — the startup check the engine factory
 * runs on the Spring-bound {@link RateLimitProperties} (replacing the validation the old custom
 * loader did). A valid config passes; each class of misconfiguration fails fast.
 */
public class RateLimitConfigValidatorTest {

  private static RateLimitProperties validConfig() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    return config;
  }

  @Test
  public void testValidConfigPasses() {
    RateLimitConfigValidator.validate(validConfig());
  }

  @Test
  public void testMissingGraphqlPathPatternFails() {
    RateLimitProperties config = validConfig();
    config.getCapacity().getGraphql().setPathPattern("");
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testRetryAfterJitterOutOfRangeFails() {
    RateLimitProperties config = validConfig();
    config.setRetryAfterJitterPercent(150);
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testCapacityMinAboveInitialFails() {
    RateLimitProperties config = validConfig();
    config.getCapacity().getDefaultCapacity().setMinLimit(100);
    config.getCapacity().getDefaultCapacity().setInitialLimit(10);
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testEndpointRuleMissingCapacityFieldsFails() {
    RateLimitProperties config = validConfig();
    RateLimitProperties.Rule rule = new RateLimitProperties.Rule();
    rule.setId("signup");
    rule.setPathPattern("/signup");
    rule.setMethods(List.of("POST"));
    // capacity/refillTokens/refillPeriodSeconds intentionally omitted.
    config.getEndpoint().getRules().add(rule);
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testNegativeMinRetryAfterFails() {
    RateLimitProperties config = validConfig();
    config.setMinRetryAfterSeconds(-1);
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testCapacityRuleZeroInitialLimitFails() {
    RateLimitProperties config = validConfig();
    config
        .getCapacity()
        .getRules()
        .add(
            RateLimitProperties.Rule.builder()
                .id("bad-capacity")
                .pathPattern("/api/graphql")
                .methods(List.of("POST"))
                .initialLimit(0)
                .maxLimit(100)
                .build());
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testGraphqlOperationNamesOnNonGraphqlPathFails() {
    RateLimitProperties config = validConfig();
    config
        .getEndpoint()
        .getRules()
        .add(
            RateLimitProperties.Rule.builder()
                .id("bad-op")
                .pathPattern("/auth/signUp") // not the graphql path
                .methods(List.of("POST"))
                .graphqlOperationNames(List.of("searchAcrossEntities"))
                .capacity(10)
                .refillTokens(10)
                .refillPeriodSeconds(60)
                .build());
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testScopedEnabledBucketWithZeroCapacityFails() {
    RateLimitProperties config = validConfig();
    // Enabling the chain with the default (capacity-0, not-disabled) actor bucket must fail.
    config.getScoped().setEnabled(true);
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }

  @Test
  public void testTenantIdContainingColonFails() {
    RateLimitProperties config = validConfig();
    config.setTenantId("tenant:evil");
    assertThrows(IllegalStateException.class, () -> RateLimitConfigValidator.validate(config));
  }
}
