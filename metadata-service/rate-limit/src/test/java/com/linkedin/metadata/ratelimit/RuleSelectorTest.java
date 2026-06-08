package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitGraphQLConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import java.util.List;
import org.testng.annotations.Test;

public class RuleSelectorTest {

  @Test
  public void testFinestGrainCapacitySelection() {
    RateLimitProperties config = baseConfig();
    config
        .getCapacity()
        .setRules(
            List.of(
                RateLimitProperties.Rule.builder()
                    .id("graphql-search-capacity")
                    .pathPattern("/api/graphql")
                    .methods(List.of("POST"))
                    .graphqlOperationNames(List.of("searchAcrossEntities"))
                    .initialLimit(30)
                    .maxLimit(400)
                    .build()));

    RuleSelector selector = new RuleSelector(config);

    CompiledRateLimitRule searchRule =
        selector.selectCapacityRule("/api/graphql", "POST", "searchAcrossEntities");
    assertEquals(searchRule.getId(), "graphql-search-capacity");

    CompiledRateLimitRule genericRule =
        selector.selectCapacityRule("/api/graphql", "POST", "getMe");
    assertEquals(genericRule.getId(), CompiledRateLimitRule.GRAPHQL_CAPACITY_ID);

    CompiledRateLimitRule restRule = selector.selectCapacityRule("/entities", "POST", null);
    assertEquals(restRule.getId(), CompiledRateLimitRule.DEFAULT_CAPACITY_ID);
  }

  @Test
  public void testEndpointRuleSelection() {
    RateLimitProperties config = baseConfig();
    config
        .getEndpoint()
        .setRules(
            List.of(
                RateLimitProperties.Rule.builder()
                    .id("auth-signup")
                    .pathPattern("/auth/signUp")
                    .methods(List.of("POST"))
                    .capacity(200)
                    .refillTokens(200)
                    .refillPeriodSeconds(60)
                    .build()));

    RuleSelector selector = new RuleSelector(config);
    CompiledRateLimitRule rule = selector.selectEndpointRule("/auth/signUp", "POST", null);
    assertEquals(rule.getId(), "auth-signup");
  }

  @Test
  public void testGraphQLPostDetection() {
    RuleSelector selector = new RuleSelector(baseConfig());
    assertTrue(selector.isGraphQLPost("/api/graphql", "POST"));
  }

  @Test
  public void testCapacityLimiterConfigsRespectEnabledFlags() {
    RateLimitProperties config = baseConfig();
    config.getCapacity().getDefaultCapacity().setEnabled(false);

    RuleSelector selector = new RuleSelector(config);

    assertFalse(
        selector
            .getCapacityLimiterConfigs()
            .containsKey(CompiledRateLimitRule.DEFAULT_CAPACITY_ID));
    assertTrue(
        selector
            .getCapacityLimiterConfigs()
            .containsKey(CompiledRateLimitRule.GRAPHQL_CAPACITY_ID));
    assertEquals(selector.selectCapacityRule("/entities", "POST", null), null);
  }

  @Test
  public void testCapacityDisabledSkipsAllCapacityRules() {
    RateLimitProperties config = baseConfig();
    config.getCapacity().setEnabled(false);

    RuleSelector selector = new RuleSelector(config);

    assertTrue(selector.getCapacityLimiterConfigs().isEmpty());
    assertEquals(selector.selectCapacityRule("/entities", "POST", null), null);
    assertEquals(selector.selectCapacityRule("/api/graphql", "POST", "getMe"), null);
  }

  @Test
  public void testEndpointDisabledSkipsEndpointRules() {
    RateLimitProperties config = baseConfig();
    config.getEndpoint().setEnabled(false);
    config
        .getEndpoint()
        .setRules(
            List.of(
                RateLimitProperties.Rule.builder()
                    .id("auth-signup")
                    .pathPattern("/auth/signUp")
                    .methods(List.of("POST"))
                    .capacity(200)
                    .refillTokens(200)
                    .refillPeriodSeconds(60)
                    .build()));

    RuleSelector selector = new RuleSelector(config);
    assertEquals(selector.selectEndpointRule("/auth/signUp", "POST", null), null);
  }

  private RateLimitProperties baseConfig() {
    RateLimitProperties config = new RateLimitProperties();
    config.setCapacity(new RateLimitProperties.Capacity());
    config.getCapacity().setEnabled(true);
    config.getCapacity().setDefaultCapacity(new CapacityLimitConfig());
    config.setEndpoint(new RateLimitProperties.Endpoint());
    config.getEndpoint().setEnabled(true);
    RateLimitGraphQLConfig graphql = new RateLimitGraphQLConfig();
    graphql.setPathPattern("/api/graphql");
    graphql.setOperationRulesEnabled(true);
    graphql.setInitialLimit(100);
    config.getCapacity().setGraphql(graphql);
    return config;
  }
}
