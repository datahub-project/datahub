package com.linkedin.metadata.config.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import io.datahubproject.metadata.context.ObjectMapperContext;
import java.nio.file.Files;
import java.nio.file.Path;
import org.testng.annotations.Test;

public class RateLimitConfigLoaderTest {

  private final RateLimitConfigLoader loader =
      new RateLimitConfigLoader(
          ObjectMapperContext.DEFAULT.getObjectMapper(),
          ObjectMapperContext.DEFAULT.getYamlMapper());

  @Test
  public void testBuilderAppliesFieldDefaults() {
    RateLimitProperties config = RateLimitProperties.builder().build();
    assertTrue(config.isFailOpen());
    assertEquals(config.getMinRetryAfterSeconds(), 60);
    assertEquals(config.getRetryAfterJitterPercent(), 10);
    assertEquals(
        config.getExcludedPaths(),
        "/health,/health/live,/actuator/prometheus,/openapi/v1/rate-limits/**");

    CapacityLimitConfig capacityDefaults = config.getCapacity().getDefaultCapacity();
    assertTrue(capacityDefaults.isEnabled());
    assertEquals(capacityDefaults.getInitialLimit(), 200);
    assertEquals(capacityDefaults.getMinLimit(), 20);
    assertEquals(capacityDefaults.getMaxLimit(), 5000);

    CapacityLimitConfig builtCapacity = CapacityLimitConfig.builder().build();
    assertTrue(builtCapacity.isEnabled());
    assertEquals(builtCapacity.getInitialLimit(), 200);
    assertEquals(builtCapacity.getMinLimit(), 20);
    assertEquals(builtCapacity.getMaxLimit(), 5000);
  }

  @Test
  public void testExternalFileReplacesRules() throws Exception {
    RateLimitProperties base = new RateLimitProperties();
    base.getConfigFile().setEnabled(true);
    base.getCapacity().getGraphql().setPathPattern("/api/graphql");

    Path temp = Files.createTempFile("rate-limits", ".yaml");
    Files.writeString(
        temp,
        """
        rateLimits:
          endpoint:
            enabled: true
            rules:
              - id: auth-signup
                pathPattern: /auth/signUp
                methods: [POST]
                capacity: 100
                refillTokens: 100
                refillPeriodSeconds: 60
        """);
    base.getConfigFile().setPath(temp.toString());

    RateLimitProperties effective = loader.loadEffective(base);
    assertTrue(effective.getEndpoint().isEnabled());
    assertEquals(effective.getEndpoint().getRules().size(), 1);
    assertEquals(effective.getEndpoint().getRules().get(0).getId(), "auth-signup");
  }

  @Test
  public void testPartialJsonOverlayPreservesUnsetFields() {
    RateLimitProperties base = new RateLimitProperties();
    base.setFailOpen(true);
    base.getCapacity().setEnabled(true);
    base.getCapacity().getDefaultCapacity().setInitialLimit(200);

    loader.applyJsonOverlay(
        """
        {
          "endpoint": {
            "rules": [
              {
                "id": "auth-signup",
                "pathPattern": "/auth/signUp",
                "methods": ["POST"],
                "capacity": 50,
                "refillTokens": 50,
                "refillPeriodSeconds": 60
              }
            ]
          }
        }
        """,
        base,
        "test");

    assertTrue(base.isFailOpen());
    assertTrue(base.getCapacity().isEnabled());
    assertEquals(base.getCapacity().getDefaultCapacity().getInitialLimit(), 200);
    assertEquals(base.getEndpoint().getRules().size(), 1);
  }

  @Test
  public void testJsonOverlayCanDisableCapacity() {
    RateLimitProperties base = new RateLimitProperties();
    base.getCapacity().setEnabled(true);

    loader.applyJsonOverlay("{\"capacity\": {\"enabled\": false}}", base, "test");

    assertFalse(base.getCapacity().isEnabled());
  }

  @Test
  public void testMissingExternalFileContinuesWithDefaults() {
    RateLimitProperties base = new RateLimitProperties();
    base.getConfigFile().setEnabled(true);
    base.getConfigFile().setPath("/nonexistent/rate-limits-does-not-exist.yaml");
    base.getCapacity().setEnabled(true);
    base.getCapacity().getGraphql().setPathPattern("/api/graphql");

    RateLimitProperties effective = loader.loadEffective(base);

    assertTrue(effective.getCapacity().isEnabled());
    assertTrue(effective.getEndpoint().getRules().isEmpty());
  }

  @Test
  public void testValidateAcceptsNullRules() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config.getCapacity().setRules(null);
    config.getEndpoint().setRules(null);
    loader.validate(config);
    assertTrue(config.getCapacity().getRules().isEmpty());
    assertTrue(config.getEndpoint().getRules().isEmpty());
  }

  @Test
  public void testValidationRejectsInvalidEndpointRule() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config
        .getEndpoint()
        .setRules(
            java.util.List.of(
                RateLimitProperties.Rule.builder()
                    .id("bad-endpoint")
                    .pathPattern("/auth/signUp")
                    .methods(java.util.List.of("POST"))
                    .build()));
    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("bad-endpoint"));
    }
  }

  @Test
  public void testValidationRejectsMissingGraphqlPathPattern() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern(null);
    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("capacity.graphql.pathPattern is required"));
    }
  }

  @Test
  public void testValidationRejectsInvalidRetryAfterJitterPercent() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config.setRetryAfterJitterPercent(101);

    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("retryAfterJitterPercent must be between 0 and 100"));
    }
  }

  @Test
  public void testValidationRejectsInvalidDefaultCapacityOrdering() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config.getCapacity().getDefaultCapacity().setMinLimit(500);
    config.getCapacity().getDefaultCapacity().setInitialLimit(100);

    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("capacity.default minLimit must be <= initialLimit"));
    }
  }

  @Test
  public void testMalformedJsonOverlayThrows() {
    RateLimitProperties base = new RateLimitProperties();
    try {
      loader.applyJsonOverlay("{not-json", base, "test");
      throw new AssertionError("Expected parse failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Failed to parse rate limit overlay"));
    }
  }

  @Test
  public void testValidationRejectsCapacityRuleMissingLimitFields() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config
        .getCapacity()
        .setRules(
            java.util.List.of(
                RateLimitProperties.Rule.builder()
                    .id("missing-limits")
                    .pathPattern("/entities")
                    .methods(java.util.List.of("POST"))
                    .build()));

    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("missing-limits"));
    }
  }

  @Test
  public void testValidationRejectsGraphqlOperationNamesOnNonGraphqlPath() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config
        .getCapacity()
        .setRules(
            java.util.List.of(
                RateLimitProperties.Rule.builder()
                    .id("bad-graphql-path")
                    .pathPattern("/entities")
                    .methods(java.util.List.of("POST"))
                    .graphqlOperationNames(java.util.List.of("getMe"))
                    .initialLimit(10)
                    .maxLimit(100)
                    .build()));

    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(
          e.getMessage().contains("graphqlOperationNames must use capacity.graphql.pathPattern"));
    }
  }

  @Test
  public void testValidationRejectsNegativeMinRetryAfterSeconds() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config.setMinRetryAfterSeconds(-1);

    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("minRetryAfterSeconds must be >= 0"));
    }
  }

  @Test
  public void testWrapperGettersAndSetters() {
    RateLimitConfigLoader.RateLimitPropertiesWrapper wrapper =
        new RateLimitConfigLoader.RateLimitPropertiesWrapper();
    RateLimitProperties properties = new RateLimitProperties();
    wrapper.setRateLimits(properties);
    assertEquals(wrapper.getRateLimits(), properties);
  }

  @Test
  public void testValidationRejectsNonPositiveCapacityRuleLimits() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config
        .getCapacity()
        .setRules(
            java.util.List.of(
                RateLimitProperties.Rule.builder()
                    .id("bad-capacity")
                    .pathPattern("/api/graphql")
                    .methods(java.util.List.of("POST"))
                    .initialLimit(0)
                    .maxLimit(100)
                    .build()));

    try {
      loader.validate(config);
      throw new AssertionError("Expected validation failure");
    } catch (IllegalStateException e) {
      assertTrue(
          e.getMessage()
              .contains("Capacity rate limit rule bad-capacity initialLimit must be > 0"));
    }
  }
}
