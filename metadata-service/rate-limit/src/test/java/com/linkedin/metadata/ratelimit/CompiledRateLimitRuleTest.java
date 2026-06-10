package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import java.util.List;
import org.springframework.util.AntPathMatcher;
import org.testng.annotations.Test;

public class CompiledRateLimitRuleTest {

  private final AntPathMatcher pathMatcher = new AntPathMatcher();

  @Test
  public void testCapacityRuleMatching() {
    RateLimitProperties.Rule config =
        RateLimitProperties.Rule.builder()
            .id("graphql-search")
            .pathPattern("/api/graphql")
            .methods(List.of("post"))
            .graphqlOperationNames(List.of("searchAcrossEntities"))
            .initialLimit(10)
            .maxLimit(100)
            .build();

    CompiledRateLimitRule rule =
        CompiledRateLimitRule.fromCapacityRuleConfig(config, "/api/graphql");

    assertEquals(rule.getType(), RateLimitRuleType.capacity);
    assertTrue(rule.matchesPath(pathMatcher, "/api/graphql"));
    assertTrue(rule.matchesMethod("POST"));
    assertTrue(rule.matchesOperation("searchAcrossEntities"));
    assertFalse(rule.matchesOperation("getMe"));
    assertTrue(rule.isOperationScoped());
  }

  @Test
  public void testEndpointRuleMatchingUsesDefaultPostMethod() {
    RateLimitProperties.Rule config =
        RateLimitProperties.Rule.builder()
            .id("auth-signup")
            .pathPattern("/auth/signUp")
            .capacity(10)
            .refillTokens(10)
            .refillPeriodSeconds(60)
            .build();

    CompiledRateLimitRule rule =
        CompiledRateLimitRule.fromEndpointRuleConfig(config, "/api/graphql");

    assertEquals(rule.getType(), RateLimitRuleType.endpoint);
    assertTrue(rule.matchesMethod("POST"));
    assertFalse(rule.matchesMethod("GET"));
    assertTrue(rule.matchesOperation(null));
    assertFalse(rule.isOperationScoped());
  }

  @Test
  public void testCompareSpecificityPrefersHigherRank() {
    CompiledRateLimitRule generic =
        CompiledRateLimitRule.materializedCapacityRule(
            "generic", "/**", 1, new CapacityLimitConfig());
    CompiledRateLimitRule specific =
        CompiledRateLimitRule.materializedCapacityRule(
            "specific", "/entities", 3, new CapacityLimitConfig());

    assertTrue(CompiledRateLimitRule.compareSpecificity(generic, specific) < 0);
    assertTrue(CompiledRateLimitRule.compareSpecificity(specific, generic) > 0);
  }
}
