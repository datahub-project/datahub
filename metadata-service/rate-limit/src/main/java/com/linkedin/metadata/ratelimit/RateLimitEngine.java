package com.linkedin.metadata.ratelimit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import com.linkedin.metadata.ratelimit.model.RateLimitDecision;
import com.linkedin.metadata.ratelimit.model.RateLimitLease;
import com.linkedin.metadata.ratelimit.model.RateLimitSource;
import com.linkedin.metadata.utils.BasePathUtils;
import com.netflix.concurrency.limits.Limiter;
import io.github.bucket4j.ConsumptionProbe;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.StringUtils;

@Slf4j
public class RateLimitEngine {

  @Getter private final RateLimitProperties config;
  private final RuleSelector ruleSelector;
  private final AdaptiveCapacityLimiter adaptiveCapacityLimiter;
  @Nullable private final EndpointRateLimitStore endpointStore;
  private final RateLimitMetrics metrics;
  private final AntPathMatcher pathMatcher = new AntPathMatcher();
  private final List<String> excludedPaths;
  private final String resolvedBasePath;
  private final ObjectMapper objectMapper;

  public RateLimitEngine(
      @Nonnull RateLimitProperties config,
      @Nonnull String resolvedBasePath,
      @Nullable MeterRegistry meterRegistry,
      @Nonnull ObjectMapper objectMapper) {
    this(config, resolvedBasePath, meterRegistry, null, objectMapper);
  }

  public RateLimitEngine(
      @Nonnull RateLimitProperties config,
      @Nonnull String resolvedBasePath,
      @Nullable MeterRegistry meterRegistry,
      @Nullable HazelcastInstance hazelcastInstance,
      @Nonnull ObjectMapper objectMapper) {
    this.config = config;
    this.objectMapper = objectMapper;
    this.resolvedBasePath = resolvedBasePath;
    this.ruleSelector = new RuleSelector(config);
    validateEndpointRateLimiting(hazelcastInstance);
    this.endpointStore =
        isEndpointRateLimitingActive()
            ? new EndpointRateLimitStore(config.getEndpoint(), hazelcastInstance)
            : null;
    this.adaptiveCapacityLimiter =
        new AdaptiveCapacityLimiter(ruleSelector.getCapacityLimiterConfigs());
    this.metrics =
        new RateLimitMetrics(
            meterRegistry, config.getMetrics() != null && config.getMetrics().isDetailed());
    this.excludedPaths = parseExcludedPaths(config.getExcludedPaths());
    registerEndpointBuckets(config);
    registerMetrics();
    warnIfMisconfiguredPaths(config);
    warnIfMisconfiguredLimiters();
    validatePerActorRules(config);
  }

  private void validatePerActorRules(RateLimitProperties config) {
    if (config.getCapacity() != null && config.getCapacity().getRules() != null) {
      config.getCapacity().getRules().stream()
          .filter(RateLimitProperties.Rule::isEnabled)
          .filter(RateLimitProperties.Rule::isPerActor)
          .findFirst()
          .ifPresent(
              rule -> {
                throw new IllegalStateException(
                    "perActor is not supported on capacity (adaptive concurrency) rules; "
                        + "offending rule="
                        + rule.getId());
              });
    }
    String graphqlPathPattern = ruleSelector.getGraphqlPathPattern();
    if (config.getEndpoint() != null && config.getEndpoint().getRules() != null) {
      config.getEndpoint().getRules().stream()
          .filter(RateLimitProperties.Rule::isEnabled)
          .filter(RateLimitProperties.Rule::isPerActor)
          .filter(rule -> !pathMatcher.match(rule.getPathPattern(), graphqlPathPattern))
          .forEach(
              rule ->
                  log.warn(
                      "Rate limit rule {} sets perActor=true on non-GraphQL path {}; per-actor "
                          + "enforcement only applies to the GraphQL POST path ({}) and will be "
                          + "skipped (no actor available) elsewhere",
                      rule.getId(),
                      rule.getPathPattern(),
                      graphqlPathPattern));
    }
  }

  private void warnIfMisconfiguredLimiters() {
    boolean capacityActive = config.getCapacity() != null && config.getCapacity().isEnabled();
    boolean endpointActive = config.getEndpoint() != null && config.getEndpoint().isEnabled();
    if (!capacityActive && !endpointActive) {
      return;
    }
    if (capacityActive && ruleSelector.getCapacityLimiterConfigs().isEmpty()) {
      log.warn(
          "GMS rate limiting has capacity.enabled=true but no adaptive capacity rules are active "
              + "(capacity.default.enabled, capacity.graphql.enabled, and capacity.rules are all off or unset).");
    } else if (!capacityActive && endpointActive) {
      log.info(
          "GMS rate limiting: capacity.enabled=false — only endpoint token-bucket rules will enforce limits.");
    }
  }

  /** True when at least one limiter type (capacity or endpoint) is enabled. */
  public boolean isEnabled() {
    return (config.getCapacity() != null && config.getCapacity().isEnabled())
        || (config.getEndpoint() != null && config.getEndpoint().isEnabled());
  }

  public boolean isFailOpen() {
    return config.isFailOpen();
  }

  public boolean isExcluded(@Nonnull String requestUri) {
    String path = stripPath(requestUri);
    return excludedPaths.stream().anyMatch(pattern -> pathMatcher.match(pattern, path));
  }

  public boolean isGraphQLPost(@Nonnull String requestUri, @Nonnull String method) {
    return ruleSelector.isGraphQLPost(stripPath(requestUri), method.toUpperCase(Locale.ROOT));
  }

  @Nonnull
  public RateLimitDecision evaluateAndAcquireRest(
      @Nonnull String requestUri, @Nonnull String method) {
    return evaluateAndAcquire(
        new RateLimitContext(
            stripPath(requestUri),
            method.toUpperCase(Locale.ROOT),
            null,
            null,
            RateLimitSource.SERVLET_FILTER));
  }

  @Nonnull
  public RateLimitDecision evaluateAndAcquireGraphQL(
      @Nonnull String requestUri,
      @Nonnull String method,
      @Nonnull String operationName,
      @Nullable String actorUrn) {
    return evaluateAndAcquire(
        new RateLimitContext(
            stripPath(requestUri),
            method.toUpperCase(Locale.ROOT),
            operationName,
            actorUrn,
            RateLimitSource.GRAPHQL_GATE));
  }

  public void release(@Nonnull RateLimitLease lease, boolean success) {
    adaptiveCapacityLimiter.release(lease.getCapacityListener(), success);
  }

  public void applyHeaders(
      @Nonnull HttpServletResponse response, @Nonnull RateLimitDecision decision) {
    RateLimitHeaderWriter.createHeaders(decision)
        .forEach((name, value) -> response.setHeader(name, value));
  }

  public void writeDeniedResponse(
      @Nonnull HttpServletResponse response, @Nonnull RateLimitDecision decision)
      throws IOException {
    applyHeaders(response, decision);
    response.setStatus(429);
    response.setContentType("application/json;charset=utf-8");
    byte[] body =
        objectMapper
            .writeValueAsString(Map.of("error", "Rate limit exceeded"))
            .getBytes(StandardCharsets.UTF_8);
    response.getOutputStream().write(body);
  }

  @Nonnull
  public Map<String, Object> statusSnapshot() {
    Map<String, Object> adaptive = new HashMap<>();
    ruleSelector
        .getCapacityLimiterConfigs()
        .keySet()
        .forEach(
            ruleId -> {
              Map<String, Object> ruleStatus = new HashMap<>();
              ruleStatus.put("limit", adaptiveCapacityLimiter.getLimit(ruleId));
              ruleStatus.put("inflight", adaptiveCapacityLimiter.getInflight(ruleId));
              adaptive.put(ruleId, ruleStatus);
            });

    Map<String, Object> endpoint = new HashMap<>();
    if (endpointStore != null) {
      endpointStore
          .snapshotRemaining()
          .forEach(
              (ruleId, remaining) -> {
                Map<String, Object> ruleStatus = new HashMap<>();
                ruleStatus.put("remaining", remaining);
                ruleStatus.put("capacity", endpointStore.capacity(ruleId));
                endpoint.put(ruleId, ruleStatus);
              });
    }

    return Map.of(
        "capacityEnabled",
        config.getCapacity() != null && config.getCapacity().isEnabled(),
        "endpointEnabled",
        config.getEndpoint() != null && config.getEndpoint().isEnabled(),
        "adaptive",
        adaptive,
        "endpoint",
        endpoint);
  }

  @Nonnull
  public RateLimitLease toLease(@Nonnull RateLimitDecision decision) {
    return new RateLimitLease(
        decision.getCapacityListener(),
        decision.getCapacityRuleId(),
        decision.getEndpointRuleId(),
        System.nanoTime());
  }

  private RateLimitDecision evaluateAndAcquire(RateLimitContext ctx) {
    if (!isEnabled()) {
      return RateLimitDecision.disabled(ctx.source());
    }

    try {
      CompiledRateLimitRule capacityRule =
          ruleSelector.selectCapacityRule(ctx.path(), ctx.method(), ctx.operationName());
      CompiledRateLimitRule endpointRule =
          ruleSelector.selectEndpointRule(ctx.path(), ctx.method(), ctx.operationName());

      Limiter.Listener capacityListener = null;
      if (capacityRule != null) {
        Optional<Limiter.Listener> acquired =
            adaptiveCapacityLimiter.tryAcquire(capacityRule.getId());
        if (acquired.isEmpty()) {
          RateLimitDecision decision =
              RateLimitDecision.builder()
                  .allowed(false)
                  .denyingRuleId(capacityRule.getId())
                  .denyingType(RateLimitRuleType.capacity)
                  .source(ctx.source())
                  .graphqlOperation(ctx.operationName())
                  .retryAfterSeconds(config.getMinRetryAfterSeconds())
                  .build();
          logDeny(decision);
          metrics.recordDecision(
              decision, RateLimitMetrics.graphqlOperationTag(ctx.operationName(), capacityRule));
          return decision;
        }
        capacityListener = acquired.get();
      }

      if (endpointRule != null
          && endpointStore != null
          && !skipPerActorConsume(endpointRule, ctx.actorUrn())) {
        ConsumptionProbe probe = consumeEndpoint(endpointRule, ctx.actorUrn());
        if (probe == null || !probe.isConsumed()) {
          if (capacityListener != null) {
            adaptiveCapacityLimiter.release(capacityListener, false);
          }
          RateLimitDecision decision =
              RateLimitDecision.builder()
                  .allowed(false)
                  .denyingRuleId(endpointRule.getId())
                  .denyingType(RateLimitRuleType.endpoint)
                  .source(ctx.source())
                  .graphqlOperation(ctx.operationName())
                  .retryAfterSeconds(endpointRetryAfterSeconds(probe))
                  .build();
          logDeny(decision);
          metrics.recordDecision(
              decision,
              RateLimitMetrics.graphqlOperationTag(
                  ctx.operationName(), capacityRule, endpointRule));
          return decision;
        }
      }

      RateLimitDecision decision =
          RateLimitDecision.builder()
              .allowed(true)
              .capacityRuleId(capacityRule != null ? capacityRule.getId() : null)
              .endpointRuleId(endpointRule != null ? endpointRule.getId() : null)
              .capacityListener(capacityListener)
              .source(ctx.source())
              .graphqlOperation(ctx.operationName())
              .build();
      metrics.recordDecision(
          decision,
          RateLimitMetrics.graphqlOperationTag(ctx.operationName(), capacityRule, endpointRule));
      return decision;
    } catch (RuntimeException e) {
      if (config.isFailOpen()) {
        log.warn(
            "Rate limit evaluation failed; fail-open allowing request for {} {}",
            ctx.method(),
            ctx.path(),
            e);
        return RateLimitDecision.disabled(ctx.source());
      }
      throw e;
    }
  }

  /**
   * A {@code perActor} rule with no actor (REST path, or non-USER/null GraphQL actor) skips the
   * per-actor token-bucket consume — the request proceeds. Safe because the GraphQL POST path is
   * auth-gated and the global adaptive capacity limiter still applies.
   */
  private boolean skipPerActorConsume(
      @Nonnull CompiledRateLimitRule endpointRule, @Nullable String actorUrn) {
    return endpointRule.isPerActor() && actorUrn == null;
  }

  @Nullable
  private ConsumptionProbe consumeEndpoint(
      @Nonnull CompiledRateLimitRule endpointRule, @Nullable String actorUrn) {
    if (endpointRule.isPerActor() && actorUrn != null) {
      return endpointStore.tryConsumeForActor(endpointRule.getId(), actorUrn);
    }
    return endpointStore.tryConsumeAndReturnRemaining(endpointRule.getId());
  }

  private int endpointRetryAfterSeconds(@Nullable ConsumptionProbe probe) {
    if (probe == null) {
      return config.getMinRetryAfterSeconds();
    }
    return EndpointRetryAfterCalculator.computeSeconds(
        config.getMinRetryAfterSeconds(),
        probe.getNanosToWaitForRefill(),
        config.getRetryAfterJitterPercent(),
        ThreadLocalRandom.current());
  }

  private void logDeny(RateLimitDecision decision) {
    log.warn(
        "Rate limit denied: rule={} type={} source={} operation={}",
        decision.getDenyingRuleId(),
        decision.getDenyingType(),
        decision.getSource().headerValue(),
        decision.getGraphqlOperation());
  }

  private String stripPath(String requestUri) {
    return BasePathUtils.stripBasePath(requestUri, resolvedBasePath);
  }

  private List<String> parseExcludedPaths(String excludedPaths) {
    if (!StringUtils.hasText(excludedPaths)) {
      return List.of();
    }
    return Arrays.stream(excludedPaths.split(","))
        .map(String::trim)
        .filter(StringUtils::hasText)
        .collect(Collectors.toList());
  }

  private void registerEndpointBuckets(RateLimitProperties config) {
    if (config.getEndpoint() == null || !config.getEndpoint().isEnabled()) {
      return;
    }
    if (config.getEndpoint().getRules() == null) {
      return;
    }
    if (endpointStore != null) {
      // perActor rules register too — the store needs the BucketConfiguration to build
      // per-actor proxies via tryConsumeForActor. The ruleId-keyed bucket itself stays idle
      // (one Hazelcast slot per rule); gauge registration in registerMetrics suppresses its
      // misleading always-full reading.
      config.getEndpoint().getRules().stream()
          .filter(RateLimitProperties.Rule::isEnabled)
          .forEach(endpointStore::registerEndpointRule);
    }
  }

  private void validateEndpointRateLimiting(@Nullable HazelcastInstance hazelcastInstance) {
    if (!isEndpointRateLimitingActive()) {
      return;
    }
    if (hazelcastInstance == null) {
      throw new IllegalStateException(
          "Endpoint rate limiting requires Hazelcast "
              + "(set RATE_LIMITS_ENDPOINT_ENABLED=true to provision a cluster client)");
    }
  }

  private boolean isEndpointRateLimitingActive() {
    return config.getEndpoint() != null && config.getEndpoint().isEnabled();
  }

  private void registerMetrics() {
    ruleSelector
        .getCapacityLimiterConfigs()
        .keySet()
        .forEach(ruleId -> metrics.registerAdaptiveGauges(ruleId, adaptiveCapacityLimiter));
    if (config.getEndpoint() != null
        && config.getEndpoint().isEnabled()
        && config.getEndpoint().getRules() != null) {
      if (endpointStore != null) {
        config.getEndpoint().getRules().stream()
            .filter(RateLimitProperties.Rule::isEnabled)
            // perActor rules never consume the ruleId-keyed bucket, so its remaining gauge
            // would always read full and mislead operators — suppress it.
            .filter(rule -> !rule.isPerActor())
            .forEach(rule -> metrics.registerEndpointGauge(rule.getId(), endpointStore));
      }
    }
  }

  private void warnIfMisconfiguredPaths(RateLimitProperties config) {
    if (!StringUtils.hasText(resolvedBasePath)) {
      return;
    }
    Stream<RateLimitProperties.Rule> allRules =
        Stream.concat(
            config.getCapacity() != null && config.getCapacity().getRules() != null
                ? config.getCapacity().getRules().stream()
                : Stream.empty(),
            config.getEndpoint() != null && config.getEndpoint().getRules() != null
                ? config.getEndpoint().getRules().stream()
                : Stream.empty());
    allRules.forEach(
        rule -> {
          if (rule.getPathPattern() != null && rule.getPathPattern().startsWith(resolvedBasePath)) {
            log.warn(
                "Rate limit rule {} pathPattern appears to include GMS base path {}; use logical paths",
                rule.getId(),
                resolvedBasePath);
          }
        });
  }
}
