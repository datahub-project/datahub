package com.linkedin.metadata.ratelimit;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class RateLimitEngineTest {

  private HazelcastInstance hazelcastInstance;

  @AfterMethod
  public void tearDown() {
    HazelcastTestSupport.shutdown(hazelcastInstance);
    hazelcastInstance = null;
  }

  @Test
  public void testRestRequestDeniedWhenCapacityExceeded() {
    RateLimitProperties config = capacityOnlyConfig();
    config.getCapacity().getDefaultCapacity().setInitialLimit(1);
    config.getCapacity().getDefaultCapacity().setMaxLimit(1);

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, ObjectMapperContext.defaultMapper);
    RateLimitDecision first = engine.evaluateAndAcquireRest("/entities", "POST");
    RateLimitDecision second = engine.evaluateAndAcquireRest("/entities", "POST");

    assertEquals(first.isAllowed(), true);
    assertFalse(second.isAllowed());
    assertEquals(second.getDenyingRuleId(), CompiledRateLimitRule.DEFAULT_CAPACITY_ID);
    assertNotNull(second.getDenyingType());
    engine.release(engine.toLease(first), true);
  }

  @Test
  public void testEndpointBucketDeniesAfterCapacityConsumed() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    RateLimitProperties config = endpointEnabledConfig();
    config.setRetryAfterJitterPercent(0);
    config
        .getEndpoint()
        .setRules(
            List.of(
                RateLimitProperties.Rule.builder()
                    .id("auth-signup")
                    .pathPattern("/auth/signUp")
                    .methods(List.of("POST"))
                    .capacity(1)
                    .refillTokens(1)
                    .refillPeriodSeconds(60)
                    .build()));

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, hazelcastInstance, ObjectMapperContext.defaultMapper);
    RateLimitDecision first = engine.evaluateAndAcquireRest("/auth/signUp", "POST");
    RateLimitDecision second = engine.evaluateAndAcquireRest("/auth/signUp", "POST");

    assertEquals(first.isAllowed(), true);
    assertFalse(second.isAllowed());
    assertEquals(second.getDenyingRuleId(), "auth-signup");
    assertNotNull(second.getRetryAfterSeconds());
    assertTrue(second.getRetryAfterSeconds() >= config.getMinRetryAfterSeconds());
    engine.release(engine.toLease(first), true);
  }

  @Test(expectedExceptions = IllegalStateException.class)
  public void testEndpointRateLimitingRequiresHazelcastAtStartup() {
    RateLimitProperties config = endpointEnabledConfig();
    new RateLimitEngine(config, "", null, null, ObjectMapperContext.defaultMapper);
  }

  @Test
  public void testEnabledWithNoAdaptiveCapacityRulesUsesEndpointOnly() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    RateLimitProperties config = endpointEnabledConfig();
    config.getCapacity().getDefaultCapacity().setEnabled(false);

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, hazelcastInstance, ObjectMapperContext.defaultMapper);
    assertTrue(engine.isEnabled());
    assertTrue(((Map<?, ?>) engine.statusSnapshot().get("adaptive")).isEmpty());

    RateLimitDecision decision = engine.evaluateAndAcquireRest("/entities", "POST");
    assertTrue(decision.isAllowed());
    assertEquals(decision.getCapacityRuleId(), null);
  }

  @Test
  public void testDisabledWhenNoLimiterTypeEnabled() {
    RateLimitProperties config = new RateLimitProperties();
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, ObjectMapperContext.defaultMapper);
    assertFalse(engine.isEnabled());
    assertTrue(engine.evaluateAndAcquireRest("/entities", "POST").isAllowed());
  }

  @Test
  public void testGraphQLCapacityDenialUsesGraphQLGateSource() {
    RateLimitProperties config = capacityOnlyConfig();
    config.getCapacity().getDefaultCapacity().setInitialLimit(1);
    config.getCapacity().getDefaultCapacity().setMaxLimit(1);
    config.getCapacity().getGraphql().setEnabled(true);
    config.getCapacity().getGraphql().setInitialLimit(1);
    config.getCapacity().getGraphql().setMaxLimit(1);

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, ObjectMapperContext.defaultMapper);
    RateLimitDecision first = engine.evaluateAndAcquireGraphQL("/api/graphql", "POST", "getMe");
    RateLimitDecision second = engine.evaluateAndAcquireGraphQL("/api/graphql", "POST", "getMe");

    assertTrue(first.isAllowed());
    assertFalse(second.isAllowed());
    assertEquals(second.getSource(), RateLimitSource.GRAPHQL_GATE);
    assertEquals(second.getGraphqlOperation(), "getMe");
    engine.release(engine.toLease(first), true);
  }

  @Test
  public void testExcludedPathsBypassRateLimiting() {
    RateLimitProperties config = capacityOnlyConfig();
    config.setExcludedPaths("/health,/actuator/prometheus/**");
    config.getCapacity().getDefaultCapacity().setInitialLimit(1);
    config.getCapacity().getDefaultCapacity().setMaxLimit(1);

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, ObjectMapperContext.defaultMapper);
    assertTrue(engine.isExcluded("/health"));
    assertTrue(engine.isExcluded("/actuator/prometheus/metrics"));
    assertFalse(engine.isExcluded("/actuator/prometheus-extra"));
    assertFalse(engine.isExcluded("/entities"));
  }

  @Test
  public void testIsGraphQLPostMatchesConfiguredPath() {
    RateLimitProperties config = capacityOnlyConfig();
    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, ObjectMapperContext.defaultMapper);

    assertTrue(engine.isGraphQLPost("/api/graphql", "POST"));
    assertFalse(engine.isGraphQLPost("/api/graphql", "GET"));
    assertFalse(engine.isGraphQLPost("/entities", "POST"));
  }

  @Test
  public void testApplyHeadersAndWriteDeniedResponse() throws Exception {
    RateLimitProperties config = capacityOnlyConfig();
    config.getCapacity().getDefaultCapacity().setInitialLimit(1);
    config.getCapacity().getDefaultCapacity().setMaxLimit(1);

    RateLimitEngine engine =
        new RateLimitEngine(config, "", null, ObjectMapperContext.defaultMapper);
    RateLimitDecision allowed = engine.evaluateAndAcquireRest("/entities", "POST");
    RateLimitDecision denied = engine.evaluateAndAcquireRest("/entities", "POST");

    HttpServletResponse response = mock(HttpServletResponse.class);
    engine.applyHeaders(response, allowed);
    verify(response)
        .setHeader(ThrottleResponseHeaders.RULE, CompiledRateLimitRule.DEFAULT_CAPACITY_ID);

    ByteArrayOutputStream body = new ByteArrayOutputStream();
    ServletOutputStream outputStream =
        new ServletOutputStream() {
          @Override
          public void write(int b) {
            body.write(b);
          }

          @Override
          public boolean isReady() {
            return true;
          }

          @Override
          public void setWriteListener(jakarta.servlet.WriteListener writeListener) {}
        };
    when(response.getOutputStream()).thenReturn(outputStream);

    engine.writeDeniedResponse(response, denied);
    verify(response).setStatus(429);
    assertTrue(body.toString().contains("Rate limit exceeded"));
    engine.release(engine.toLease(allowed), true);
  }

  @Test
  public void testStatusSnapshotIncludesAdaptiveAndEndpointState() {
    hazelcastInstance = HazelcastTestSupport.createIsolatedInstance();
    RateLimitProperties config = endpointEnabledConfig();
    config.getMetrics().setDetailed(true);
    config
        .getEndpoint()
        .setRules(
            List.of(
                RateLimitProperties.Rule.builder()
                    .id("auth-signup")
                    .pathPattern("/auth/signUp")
                    .methods(List.of("POST"))
                    .capacity(2)
                    .refillTokens(2)
                    .refillPeriodSeconds(60)
                    .build()));

    RateLimitEngine engine =
        new RateLimitEngine(
            config,
            "",
            new SimpleMeterRegistry(),
            hazelcastInstance,
            ObjectMapperContext.defaultMapper);
    Map<String, Object> snapshot = engine.statusSnapshot();

    assertTrue((Boolean) snapshot.get("capacityEnabled"));
    assertTrue((Boolean) snapshot.get("endpointEnabled"));
    assertTrue(
        ((Map<?, ?>) snapshot.get("adaptive"))
            .containsKey(CompiledRateLimitRule.DEFAULT_CAPACITY_ID));
    assertTrue(((Map<?, ?>) snapshot.get("endpoint")).containsKey("auth-signup"));
  }

  private RateLimitProperties capacityOnlyConfig() {
    RateLimitProperties config = new RateLimitProperties();
    config.setFailOpen(false);
    config.getCapacity().setEnabled(true);
    config.getCapacity().getGraphql().setPathPattern("/api/graphql");
    config.getCapacity().getGraphql().setEnabled(false);
    config.getCapacity().setDefaultCapacity(new CapacityLimitConfig());
    config.getCapacity().getDefaultCapacity().setInitialLimit(50);
    config.getCapacity().getDefaultCapacity().setMaxLimit(50);
    return config;
  }

  private RateLimitProperties endpointEnabledConfig() {
    RateLimitProperties config = capacityOnlyConfig();
    config.getEndpoint().setEnabled(true);
    return config;
  }
}
