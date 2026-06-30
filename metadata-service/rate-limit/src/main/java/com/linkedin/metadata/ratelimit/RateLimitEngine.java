package com.linkedin.metadata.ratelimit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hazelcast.core.HazelcastInstance;
import com.linkedin.metadata.config.ratelimit.RateLimitConfigValidator;
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
import java.util.ArrayList;
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
        needsDistributedStore()
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
    // Scoped-chain sizing validation is owned by RateLimitConfigValidator (single source of truth);
    // delegate here so it runs even when the engine is constructed outside the factory path.
    RateLimitConfigValidator.validateScoped(config);
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

  /**
   * True when the master switch is on AND at least one limiter type (capacity, endpoint, or the
   * scoped chain) is enabled. The master switch ({@code rateLimits.enabled}) short-circuits
   * everything when off.
   */
  public boolean isEnabled() {
    return config.isEnabled()
        && ((config.getCapacity() != null && config.getCapacity().isEnabled())
            || (config.getEndpoint() != null && config.getEndpoint().isEnabled())
            || isScopedActive());
  }

  private boolean isScopedActive() {
    return config.getScoped() != null && config.getScoped().isEnabled();
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
    return evaluateAndAcquireRest(requestUri, method, null);
  }

  @Nonnull
  public RateLimitDecision evaluateAndAcquireRest(
      @Nonnull String requestUri, @Nonnull String method, @Nullable ClientClass clientClass) {
    return evaluateAndAcquire(
        new RateLimitContext(
            stripPath(requestUri),
            method.toUpperCase(Locale.ROOT),
            null,
            null,
            clientClass,
            RateLimitSource.SERVLET_FILTER));
  }

  @Nonnull
  public RateLimitDecision evaluateAndAcquireGraphQL(
      @Nonnull String requestUri,
      @Nonnull String method,
      @Nonnull String operationName,
      @Nullable String actorUrn) {
    return evaluateAndAcquireGraphQL(requestUri, method, operationName, actorUrn, null);
  }

  @Nonnull
  public RateLimitDecision evaluateAndAcquireGraphQL(
      @Nonnull String requestUri,
      @Nonnull String method,
      @Nonnull String operationName,
      @Nullable String actorUrn,
      @Nullable ClientClass clientClass) {
    return evaluateAndAcquire(
        new RateLimitContext(
            stripPath(requestUri),
            method.toUpperCase(Locale.ROOT),
            operationName,
            actorUrn,
            clientClass,
            RateLimitSource.GRAPHQL_GATE));
  }

  /**
   * Resolves the client class actually used for rule selection. When client-class selection is
   * disabled (the default), returns {@code null} so {@link
   * CompiledRateLimitRule#matchesClientClass} treats every rule as a match — i.e. the class
   * dimension is ignored and behavior is unchanged.
   */
  @Nullable
  private ClientClass effectiveClientClass(@Nullable ClientClass clientClass) {
    return config.isClientClassEnabled() ? clientClass : null;
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
    // Flush so the 429 body is committed even if nothing downstream closes the stream (the filter
    // returns immediately after this without continuing the chain).
    response.getOutputStream().flush();
  }

  /**
   * Config view for the read-only ops endpoint with internal isolation identifiers redacted: {@code
   * tenantId} and {@code endpoint.hazelcastMapName} name the shared-infra namespace/keys, so even
   * behind the system-operations privilege there's no reason to expose them (and in a shared
   * multi-tenant GMS it would let a privileged tenant admin learn another tenant's key prefix).
   */
  @Nonnull
  public JsonNode getRedactedConfig() {
    ObjectNode node = objectMapper.valueToTree(config);
    node.remove("tenantId");
    JsonNode endpoint = node.get("endpoint");
    if (endpoint instanceof ObjectNode) {
      ((ObjectNode) endpoint).remove("hazelcastMapName");
    }
    return node;
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
      ClientClass ruleClass = effectiveClientClass(ctx.clientClass());
      CompiledRateLimitRule capacityRule =
          ruleSelector.selectCapacityRule(ctx.path(), ctx.method(), ctx.operationName(), ruleClass);
      CompiledRateLimitRule endpointRule =
          ruleSelector.selectEndpointRule(ctx.path(), ctx.method(), ctx.operationName(), ruleClass);

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

      RateLimitDecision scopedDeny = consumeScopedChain(ctx);
      if (scopedDeny != null) {
        if (capacityListener != null) {
          adaptiveCapacityLimiter.release(capacityListener, false);
        }
        logDeny(scopedDeny);
        metrics.recordDecision(
            scopedDeny,
            RateLimitMetrics.graphqlOperationTag(ctx.operationName(), capacityRule, endpointRule));
        return scopedDeny;
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
        metrics.recordFailOpen("front_gate");
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

  /**
   * The front-gate scoped chain — the request-level multi-dimensional limiter consumed narrow →
   * broad after the per-pod capacity limiter: per-actor → client class (browser/sdk) → fleet
   * global. Heavy-resolver buckets are deliberately excluded here; they are a separate gate the
   * GraphQL controller applies per top-level resolver (see {@link #consumeHeavyResolver}). Returns
   * a deny decision for the first stage that rejects (after refunding the buckets already consumed,
   * unless refund is disabled), or {@code null} when the chain is inactive or every applicable
   * stage allows.
   */
  @Nullable
  private RateLimitDecision consumeScopedChain(@Nonnull RateLimitContext ctx) {
    RateLimitProperties.ScopedLimits scoped = config.getScoped();
    if (endpointStore == null || scoped == null || !scoped.isEnabled()) {
      return null;
    }

    List<ScopedStep> steps = buildScopedSteps(ctx.actorUrn(), ctx.clientClass(), scoped);
    List<ScopedStep> consumed = new ArrayList<>(steps.size());
    for (ScopedStep step : steps) {
      ConsumptionProbe probe =
          endpointStore.tryConsumeScoped(step.key(), step.limits(), 1, step.global());
      if (probe.isConsumed()) {
        consumed.add(step);
        continue;
      }
      if (!scoped.isRefundDisabled()) {
        consumed.forEach(
            done -> endpointStore.refundScoped(done.key(), done.limits(), 1, done.global()));
      }
      return RateLimitDecision.builder()
          .allowed(false)
          .denyingRuleId(step.label())
          .denyingType(RateLimitRuleType.endpoint)
          .source(ctx.source())
          .graphqlOperation(ctx.operationName())
          .retryAfterSeconds(endpointRetryAfterSeconds(probe))
          .build();
    }
    return null;
  }

  /**
   * Refunds the scoped-chain tokens a request consumed at the front gate. Called when a later,
   * out-of-band gate (the heavy-resolver gate, applied by the GraphQL controller after the chain
   * has already admitted the request) rejects it — so a rejected request doesn't permanently burn
   * the actor/class/global quota, mirroring the chain's own refund-on-deny. No-op when the chain is
   * inactive or refunds are disabled. The steps are rebuilt from the request's actor/class (the
   * same deterministic inputs {@link #buildScopedSteps} used at consume time); {@code refundScoped}
   * caps at capacity, so refunding a token can never overflow a bucket.
   */
  public void refundScopedChain(@Nullable String actorUrn, @Nullable ClientClass clientClass) {
    RateLimitProperties.ScopedLimits scoped = config.getScoped();
    if (endpointStore == null
        || scoped == null
        || !scoped.isEnabled()
        || scoped.isRefundDisabled()) {
      return;
    }
    for (ScopedStep step : buildScopedSteps(actorUrn, clientClass, scoped)) {
      endpointStore.refundScoped(step.key(), step.limits(), 1, step.global());
    }
  }

  /**
   * Builds the ordered narrow → broad steps for this request. Disabled limiters and absent
   * dimensions (no actor on REST, unclassified client, non-heavy resolver) are skipped.
   * Tenant-scoped keys are prefixed with the tenant id; only {@code global} is un-prefixed and
   * lives in the shared fleet map.
   */
  @Nonnull
  private List<ScopedStep> buildScopedSteps(
      @Nullable String actorUrn,
      @Nullable ClientClass rawClientClass,
      @Nonnull RateLimitProperties.ScopedLimits scoped) {
    String tenant = config.getTenantId();
    List<ScopedStep> steps = new ArrayList<>(4);

    if (actorUrn != null && !scoped.getActor().isDisabled()) {
      steps.add(
          new ScopedStep(
              tenantKey(tenant, "actor:" + actorUrn), scoped.getActor(), false, "scoped:actor"));
    }

    // Gate the class step on clientClassEnabled, exactly like rule selection
    // (effectiveClientClass).
    // The client class is derived from a request header that is only trusted on the
    // frontend-proxied
    // hop; when class discrimination is disabled we must NOT split scoped traffic by it either,
    // otherwise a direct-to-GMS caller could claim the more generous bucket by spoofing the header.
    ClientClass clientClass = effectiveClientClass(rawClientClass);
    if (clientClass != null) {
      boolean browser = clientClass == ClientClass.BROWSER;
      RateLimitProperties.BucketLimits classLimits =
          browser ? scoped.getBrowser() : scoped.getSdk();
      if (!classLimits.isDisabled()) {
        String suffix = browser ? "browser" : "sdk";
        steps.add(
            new ScopedStep(tenantKey(tenant, suffix), classLimits, false, "scoped:" + suffix));
      }
    }

    // Heavy-resolver buckets are NOT part of this front-gate chain. The GraphQL controller applies
    // them separately, per top-level resolver, after this chain admits the request (see
    // consumeHeavyResolver) — a standalone, independent protection.

    if (!scoped.getGlobal().isDisabled()) {
      steps.add(new ScopedStep("global", scoped.getGlobal(), true, "scoped:global"));
    }
    return steps;
  }

  /** Prefixes a tenant-scoped bucket key so shared infra (ES/MySQL/Hazelcast) stays isolated. */
  @Nonnull
  private static String tenantKey(@Nullable String tenantId, @Nonnull String suffix) {
    return StringUtils.hasText(tenantId) ? tenantId + ":" + suffix : suffix;
  }

  /**
   * Heavy-resolver gate (Part B), distinct from the front-gate chain. The GraphQL controller calls
   * this after the front gate admits the request, once per top-level resolver field name (reusing
   * the query it already parsed); it consumes that resolver's tenant-scoped bucket ({@code
   * {tenantId}:op:{resolver}}). Returns a deny decision when the bucket is exhausted, or {@code
   * null} when the scoped chain is inactive, the resolver isn't configured as heavy, or the bucket
   * allows. This method only consumes the resolver bucket; it does not itself touch the scoped
   * chain. When it denies, the GraphQL controller unwinds the front gate it already passed —
   * releasing the capacity slot and calling {@link #refundScopedChain} — so a request rejected here
   * doesn't permanently burn the actor/class/global quota.
   *
   * <p>{@code systemActor} lets the internal system principal bypass resolvers configured with
   * {@code exemptSystemActor=true}, so high-volume internal traffic isn't throttled where that's
   * intended.
   */
  @Nullable
  public RateLimitDecision consumeHeavyResolver(@Nonnull String resolverName, boolean systemActor) {
    RateLimitProperties.ScopedLimits scoped = config.getScoped();
    if (endpointStore == null || scoped == null || !scoped.isEnabled()) {
      return null;
    }
    RateLimitProperties.BucketLimits limits = scoped.getHeavyResolvers().get(resolverName);
    if (limits == null || limits.isDisabled() || (systemActor && limits.isExemptSystemActor())) {
      return null;
    }
    try {
      ConsumptionProbe probe =
          endpointStore.tryConsumeScoped(
              tenantKey(config.getTenantId(), "op:" + resolverName), limits, 1, false);
      if (probe.isConsumed()) {
        return null;
      }
      RateLimitDecision decision =
          RateLimitDecision.builder()
              .allowed(false)
              .denyingRuleId("scoped:op:" + resolverName)
              .denyingType(RateLimitRuleType.endpoint)
              .source(RateLimitSource.GRAPHQL_GATE)
              .graphqlOperation(resolverName)
              .retryAfterSeconds(endpointRetryAfterSeconds(probe))
              .build();
      logDeny(decision);
      metrics.recordDecision(decision, RateLimitMetrics.graphqlOperationTag(resolverName));
      return decision;
    } catch (RuntimeException e) {
      if (config.isFailOpen()) {
        metrics.recordFailOpen("heavy_resolver");
        log.warn(
            "Heavy-resolver rate limit evaluation failed; fail-open allowing resolver {}",
            resolverName,
            e);
        return null;
      }
      throw e;
    }
  }

  /** One ordered step in the scoped chain: its bucket key, sizing, target map, and deny label. */
  private record ScopedStep(
      String key, RateLimitProperties.BucketLimits limits, boolean global, String label) {}

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
    if (!needsDistributedStore()) {
      return;
    }
    if (hazelcastInstance == null) {
      throw new IllegalStateException(
          "Distributed rate limiting requires Hazelcast (set RATE_LIMITS_ENDPOINT_ENABLED=true or "
              + "RATE_LIMITS_SCOPED_ENABLED=true to provision a cluster client)");
    }
  }

  /**
   * The shared Bucket4j/Hazelcast store backs both the endpoint rules and the scoped chain, so it
   * is provisioned when either is enabled.
   */
  private boolean needsDistributedStore() {
    return config.isEnabled()
        && ((config.getEndpoint() != null && config.getEndpoint().isEnabled()) || isScopedActive());
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
