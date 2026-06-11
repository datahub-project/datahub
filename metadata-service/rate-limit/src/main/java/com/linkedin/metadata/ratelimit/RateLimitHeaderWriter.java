package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import com.linkedin.metadata.throttle.ThrottleMechanismType;
import com.linkedin.metadata.throttle.ThrottleResponseHeaderWriter;
import com.linkedin.metadata.throttle.ThrottleResponseHeaders;
import com.linkedin.metadata.throttle.ThrottleResponseSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nonnull;

public final class RateLimitHeaderWriter {

  private RateLimitHeaderWriter() {}

  @Nonnull
  public static Map<String, String> createHeaders(@Nonnull RateLimitDecision decision) {
    LinkedHashMap<String, String> headers = new LinkedHashMap<>();
    applyHeaders(headers, decision);
    return headers;
  }

  public static void applyHeaders(
      @Nonnull Map<String, String> headers, @Nonnull RateLimitDecision decision) {
    if (decision.isAllowed()) {
      headers.put(
          ThrottleResponseHeaders.SOURCE, toThrottleSource(decision.getSource()).headerValue());
      if (decision.getCapacityRuleId() != null) {
        headers.put(ThrottleResponseHeaders.RULE, decision.getCapacityRuleId());
        headers.put(ThrottleResponseHeaders.TYPE, ThrottleMechanismType.CAPACITY.headerValue());
      }
      if (decision.getEndpointRuleId() != null) {
        headers.put(ThrottleResponseHeaders.ENDPOINT_RULE, decision.getEndpointRuleId());
      }
      return;
    }

    ThrottleResponseHeaderWriter.applyDenial(
        headers,
        decision.getDenyingRuleId(),
        toMechanismType(decision.getDenyingType()),
        toThrottleSource(decision.getSource()),
        decision.getRetryAfterSeconds() != null
            ? TimeUnit.SECONDS.toMillis(decision.getRetryAfterSeconds())
            : -1);
  }

  private static ThrottleMechanismType toMechanismType(RateLimitRuleType denyingType) {
    if (denyingType == null) {
      return null;
    }
    return switch (denyingType) {
      case capacity -> ThrottleMechanismType.CAPACITY;
      case endpoint -> ThrottleMechanismType.ENDPOINT;
    };
  }

  private static ThrottleResponseSource toThrottleSource(RateLimitSource source) {
    return switch (source) {
      case SERVLET_FILTER -> ThrottleResponseSource.SERVLET_FILTER;
      case GRAPHQL_GATE -> ThrottleResponseSource.GRAPHQL_GATE;
    };
  }
}
