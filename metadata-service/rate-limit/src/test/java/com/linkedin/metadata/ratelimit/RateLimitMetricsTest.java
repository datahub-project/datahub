package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.List;
import org.testng.annotations.Test;

public class RateLimitMetricsTest {

  @Test
  public void testDeniesAlwaysRecordedWhenNotDetailed() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    RateLimitMetrics metrics = new RateLimitMetrics(registry, false);
    RateLimitDecision deny =
        RateLimitDecision.builder().allowed(false).denyingRuleId("rule-a").build();

    for (int i = 0; i < 50; i++) {
      metrics.recordDecision(deny, "none");
    }

    assertEquals(registry.find("gms.rate_limit.requests").counters().size(), 1);
    assertEquals(registry.find("gms.rate_limit.requests").counter().count(), 50.0);
  }

  @Test
  public void testDetailedModeRecordsAllAllows() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    RateLimitMetrics metrics = new RateLimitMetrics(registry, true);
    RateLimitDecision allow =
        RateLimitDecision.builder().allowed(true).capacityRuleId("rule-a").build();

    for (int i = 0; i < 10; i++) {
      metrics.recordDecision(allow, "none");
    }

    assertEquals(registry.find("gms.rate_limit.requests").counter().count(), 10.0);
  }

  @Test
  public void testNonDetailedModeSamplesAllows() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    RateLimitMetrics metrics = new RateLimitMetrics(registry, false);
    RateLimitDecision allow =
        RateLimitDecision.builder().allowed(true).capacityRuleId("rule-a").build();

    for (int i = 0; i < 10_000; i++) {
      metrics.recordDecision(allow, "none");
    }

    double recorded = registry.find("gms.rate_limit.requests").counter().count();
    assertTrue(recorded > 20 && recorded < 300, "Expected ~1% sample, got " + recorded);
  }

  @Test
  public void testGraphqlOperationTagWhenOperationScopedRuleMatches() {
    CompiledRateLimitRule operationRule =
        CompiledRateLimitRule.fromCapacityRuleConfig(
            RateLimitProperties.Rule.builder()
                .id("graphql-search-capacity")
                .pathPattern("/api/graphql")
                .methods(List.of("POST"))
                .graphqlOperationNames(List.of("searchAcrossEntities"))
                .initialLimit(30)
                .build(),
            "/api/graphql");
    CompiledRateLimitRule genericGraphqlRule =
        CompiledRateLimitRule.materializedCapacityRule(
            CompiledRateLimitRule.GRAPHQL_CAPACITY_ID,
            "/api/graphql",
            3,
            new CapacityLimitConfig());

    assertEquals(
        "searchAcrossEntities",
        RateLimitMetrics.graphqlOperationTag("searchAcrossEntities", operationRule));
    assertEquals("none", RateLimitMetrics.graphqlOperationTag("getDataset", genericGraphqlRule));
    assertEquals("none", RateLimitMetrics.graphqlOperationTag(null, operationRule));
    assertEquals("none", RateLimitMetrics.graphqlOperationTag("", operationRule));
  }

  @Test
  public void testGraphqlOperationTagRecordedOnCounter() {
    SimpleMeterRegistry registry = new SimpleMeterRegistry();
    RateLimitMetrics metrics = new RateLimitMetrics(registry, true);
    RateLimitDecision allow =
        RateLimitDecision.builder().allowed(true).capacityRuleId("graphql-search-capacity").build();

    metrics.recordDecision(allow, "searchAcrossEntities");

    assertEquals(
        1.0,
        registry
            .find("gms.rate_limit.requests")
            .tag("graphql_operation", "searchAcrossEntities")
            .counter()
            .count());
  }
}
