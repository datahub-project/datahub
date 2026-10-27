package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.util.StringUtils;

final class RateLimitMetrics {
  private static final String REQUESTS = "gms.rate_limit.requests";
  private static final String ADAPTIVE_LIMIT = "gms.rate_limit.adaptive.limit";
  private static final String ADAPTIVE_INFLIGHT = "gms.rate_limit.adaptive.inflight";
  private static final String ENDPOINT_REMAINING = "gms.rate_limit.endpoint.remaining";
  private static final String FAIL_OPEN = "gms.rate_limit.fail_open";

  private final MeterRegistry meterRegistry;
  private final boolean detailed;

  RateLimitMetrics(@Nullable MeterRegistry meterRegistry, boolean detailed) {
    this.meterRegistry = meterRegistry;
    this.detailed = detailed;
  }

  void recordDecision(RateLimitDecision decision, @Nonnull String graphqlOperationTag) {
    if (meterRegistry == null || !shouldSample(decision)) {
      return;
    }
    Tags tags =
        Tags.of(
            "rule_id",
            decision.getDenyingRuleId() != null
                ? decision.getDenyingRuleId()
                : decision.getCapacityRuleId() != null ? decision.getCapacityRuleId() : "none",
            "type",
            decision.getDenyingType() != null
                ? decision.getDenyingType().name()
                : decision.getCapacityRuleId() != null ? "capacity" : "none",
            "outcome",
            decision.isAllowed() ? "allow" : "deny",
            "graphql_operation",
            graphqlOperationTag);
    meterRegistry.counter(REQUESTS, tags).increment();
  }

  /**
   * Records a fail-open event — rate-limit evaluation threw and the request was allowed through
   * because {@code failOpen=true}. A spike here means limiting is effectively disabled (e.g. a
   * Hazelcast disruption, possibly induced), so it is worth alerting on. {@code stage} tags which
   * limiter failed (front gate vs heavy resolver).
   */
  void recordFailOpen(@Nonnull String stage) {
    if (meterRegistry == null) {
      return;
    }
    meterRegistry.counter(FAIL_OPEN, Tags.of("stage", stage)).increment();
  }

  /**
   * Limits {@code graphql_operation} metric cardinality to operations covered by an
   * operation-scoped rate limit rule.
   */
  @Nonnull
  static String graphqlOperationTag(
      @Nullable String operationName, CompiledRateLimitRule... matchedRules) {
    if (!StringUtils.hasText(operationName)) {
      return "none";
    }
    for (CompiledRateLimitRule rule : matchedRules) {
      if (rule != null && rule.isOperationScoped()) {
        return operationName;
      }
    }
    return "none";
  }

  /**
   * Tag for heavy-resolver denials. The resolver name is a configured {@code heavyResolvers} key —
   * a small bounded set, server-parsed from the query (not a spoofable client operation name) — so
   * it is safe to emit directly for the {@code graphql_operation} dimension without exploding
   * cardinality.
   */
  @Nonnull
  static String graphqlOperationTagForResolver(@Nullable String resolverName) {
    return StringUtils.hasText(resolverName) ? resolverName : "none";
  }

  void registerAdaptiveGauges(String ruleId, AdaptiveCapacityLimiter limiter) {
    if (meterRegistry == null) {
      return;
    }
    meterRegistry.gauge(
        ADAPTIVE_LIMIT, Tags.of("rule_id", ruleId), limiter, l -> l.getLimit(ruleId));
    meterRegistry.gauge(
        ADAPTIVE_INFLIGHT, Tags.of("rule_id", ruleId), limiter, l -> l.getInflight(ruleId));
  }

  void registerEndpointGauge(String ruleId, EndpointRateLimitStore store) {
    if (meterRegistry == null) {
      return;
    }
    meterRegistry.gauge(
        ENDPOINT_REMAINING, Tags.of("rule_id", ruleId), store, s -> s.remaining(ruleId));
  }

  private boolean shouldSample(RateLimitDecision decision) {
    if (!decision.isAllowed()) {
      return true;
    }
    if (detailed) {
      return true;
    }
    return ThreadLocalRandom.current().nextInt(100) == 0;
  }
}
