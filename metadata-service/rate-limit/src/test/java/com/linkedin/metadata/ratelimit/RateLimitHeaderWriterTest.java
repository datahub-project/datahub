package com.linkedin.metadata.ratelimit;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import java.util.Map;
import org.testng.annotations.Test;

public class RateLimitHeaderWriterTest {

  @Test
  public void testAllowedDecisionIncludesCapacityAndEndpointRules() {
    RateLimitDecision decision =
        RateLimitDecision.builder()
            .allowed(true)
            .source(RateLimitSource.SERVLET_FILTER)
            .capacityRuleId("_default_capacity")
            .endpointRuleId("auth-signup")
            .build();

    Map<String, String> headers = RateLimitHeaderWriter.createHeaders(decision);

    assertEquals(headers.get(ThrottleResponseHeaders.SOURCE), "servlet-filter");
    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "_default_capacity");
    assertEquals(headers.get(ThrottleResponseHeaders.TYPE), "capacity");
    assertEquals(headers.get(ThrottleResponseHeaders.ENDPOINT_RULE), "auth-signup");
    assertFalse(headers.containsKey(ThrottleResponseHeaders.RETRY_AFTER));
  }

  @Test
  public void testEndpointDenialIncludesRetryAfter() {
    RateLimitDecision decision =
        RateLimitDecision.builder()
            .allowed(false)
            .source(RateLimitSource.GRAPHQL_GATE)
            .denyingRuleId("auth-signup")
            .denyingType(RateLimitRuleType.endpoint)
            .retryAfterSeconds(90)
            .build();

    Map<String, String> headers = RateLimitHeaderWriter.createHeaders(decision);

    assertEquals(headers.get(ThrottleResponseHeaders.RULE), "auth-signup");
    assertEquals(headers.get(ThrottleResponseHeaders.TYPE), "endpoint");
    assertEquals(headers.get(ThrottleResponseHeaders.SOURCE), "graphql-gate");
    assertEquals(headers.get(ThrottleResponseHeaders.RETRY_AFTER), "90");
  }
}
