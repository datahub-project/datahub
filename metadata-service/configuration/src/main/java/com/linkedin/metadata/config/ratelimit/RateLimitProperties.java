package com.linkedin.metadata.config.ratelimit;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitProperties {
  /**
   * Master kill switch for ALL GMS rate limiting. When false, every limiter (capacity, endpoint,
   * and the scoped chain) is bypassed regardless of its individual flag, and no distributed store
   * is provisioned — the single "turn it all off" control if something goes wrong in production.
   * Default true; the individual layers remain opt-in via their own {@code enabled} flags. Env:
   * {@code RATE_LIMITS_ENABLED}.
   */
  @Builder.Default private boolean enabled = true;

  @Builder.Default private boolean failOpen = true;
  @Builder.Default private int minRetryAfterSeconds = 60;
  @Builder.Default private int retryAfterJitterPercent = 10;

  /**
   * Master switch for client-class (browser vs non-browser) rule selection. Default off for safe
   * rollout — when off, the client-class predicate is bypassed and all rules match all classes
   * (current behavior). Env: {@code RATE_LIMITS_CLIENT_CLASS_ENABLED}.
   */
  @Builder.Default private boolean clientClassEnabled = false;

  /**
   * Tenant identifier used to namespace every tenant-scoped bucket key (e.g. {@code
   * {tenantId}:actor:{urn}}). Sourced from {@code RATE_LIMITS_TENANT_ID} (the deployment's {@code
   * global.id}) today; supplied per-request from a trusted header if GMS becomes shared
   * multi-tenant. Empty = single un-namespaced scope (OSS / non-isolated).
   */
  @Builder.Default private String tenantId = "";

  /** The ordered per-tenant + global bucket chain (the primary rate-limit model). */
  @Builder.Default private ScopedLimits scoped = new ScopedLimits();

  @Builder.Default
  private String excludedPaths =
      "/health,/health/live,/actuator/prometheus,/openapi/v1/rate-limits/**";

  @Builder.Default private Capacity capacity = new Capacity();
  @Builder.Default private Endpoint endpoint = new Endpoint();
  @Builder.Default private Metrics metrics = new Metrics();

  /** Shared rule shape for {@link Capacity#rules} and {@link Endpoint#rules}. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Rule {
    private String id;
    @Builder.Default private boolean enabled = true;
    private String pathPattern;
    @Builder.Default private List<String> methods = new ArrayList<>();
    @Builder.Default private List<String> graphqlOperationNames = new ArrayList<>();

    // capacity rule fields
    private Integer initialLimit;
    private Integer minLimit;
    private Integer maxLimit;

    // endpoint rule fields
    private Integer capacity;
    private Integer refillTokens;
    private Integer refillPeriodSeconds;

    @Builder.Default private boolean perActor = false;

    /**
     * Client classes this rule applies to (e.g. {@code [browser]}, {@code [non_browser]}). Empty =
     * applies to all classes (class-agnostic). Only enforced when {@code clientClassEnabled=true}.
     * Coarse for v1; forward-compatible with finer-grained types.
     */
    @Builder.Default private List<String> clientTypes = new ArrayList<>();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Capacity {
    /** When false, no adaptive in-flight (Gradient2) limits are enforced. */
    @Builder.Default private boolean enabled = false;

    @JsonProperty("default")
    @Builder.Default
    private CapacityLimitConfig defaultCapacity = new CapacityLimitConfig();

    @Builder.Default private RateLimitGraphQLConfig graphql = new RateLimitGraphQLConfig();

    @Builder.Default private List<Rule> rules = new ArrayList<>();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Endpoint {
    public static final String DEFAULT_HAZELCAST_MAP_NAME = "gmsRateLimitEndpointBuckets";

    /**
     * Hazelcast map holding the single cross-tenant {@code global} scoped bucket (fleet-wide
     * ceiling). Fixed (not configurable) and separate from the per-tenant endpoint map. Referenced
     * by EndpointRateLimitStore and by CacheConfig's MapConfig bean.
     */
    public static final String GLOBAL_HAZELCAST_MAP_NAME = "gmsRateLimitGlobalBuckets";

    /** When true, enforce cluster-wide token-bucket (Bucket4j) endpoint limits via Hazelcast. */
    @Builder.Default private boolean enabled = false;

    @Builder.Default private String hazelcastMapName = DEFAULT_HAZELCAST_MAP_NAME;

    /** Idle eviction window (seconds) for endpoint bucket entries in Hazelcast. */
    @Builder.Default private int bucketMaxIdleSeconds = 300;

    /** LRU per-node size cap for endpoint bucket entries in Hazelcast. */
    @Builder.Default private int bucketMaxSize = 100_000;

    @Builder.Default private List<Rule> rules = new ArrayList<>();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Metrics {
    private boolean detailed;
  }

  /**
   * The scoped bucket chain consumed in order per request (narrow → broad). All buckets except
   * {@link #global} are tenant-scoped (keyed {@code {tenantId}:…}); {@code global} is the single
   * fleet-wide (cross-tenant) ceiling, evaluated last.
   */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ScopedLimits {
    /** Master switch for the scoped chain. Default off for safe rollout. */
    @Builder.Default private boolean enabled = false;

    /**
     * When a later stage of the chain denies, already-consumed upstream buckets are refunded
     * (Bucket4j {@code addTokens}, capped at capacity) so a rejected request doesn't permanently
     * burn tenant/actor tokens. Set {@code true} to turn the refund off (consume stays committed)
     * if it ever misbehaves. Default {@code false} (refund active); modeled as {@code disabled} so
     * the "on" default survives YAML/Jackson binding.
     */
    private boolean refundDisabled;

    // Bucket fields are initialized to EMPTY buckets (capacity 0), NOT hardcoded sizes: the actual
    // limits come from yaml only (application.yaml's scoped block / RATE_LIMITS_SCOPED_* env, or
    // the
    // rate-limit config file), which is the single source of the values. Plain initializers (not
    // @Builder.Default) are used so no-arg/Jackson construction still yields non-null buckets when
    // a
    // `scoped` block is partially specified — avoiding NPEs in the chain — while leaving the
    // numbers
    // to config. An enabled-but-unconfigured bucket (capacity 0) fails startup validation, so a
    // missing value is caught loudly rather than silently defaulted.

    /** Per-actor (tenant-scoped) — `{tenantId}:actor:{urn}`. Limits configured in yaml. */
    private BucketLimits actor = new BucketLimits();

    /** Browser class (tenant-scoped) — `{tenantId}:browser`. Limits configured in yaml. */
    private BucketLimits browser = new BucketLimits();

    /** SDK class (tenant-scoped) — `{tenantId}:sdk`. Limits configured in yaml. */
    private BucketLimits sdk = new BucketLimits();

    /** Fleet-wide ceiling (cross-tenant, shared map) — `global`. Limits configured in yaml. */
    private BucketLimits global = new BucketLimits();

    /**
     * Heavy-resolver buckets, tenant-scoped — `{tenantId}:op:{resolver}`. Keyed by top-level
     * GraphQL field name. A request consumes its resolver's bucket only if the resolver is listed
     * here. Add entries reactively (e.g. when a dashboard shows a tenant hammering a resolver).
     * Empty by default — no resolver is specially limited.
     */
    private Map<String, BucketLimits> heavyResolvers = new HashMap<>();
  }

  /** Token-bucket sizing shared by every scoped bucket. */
  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class BucketLimits {
    /**
     * Per-limiter kill switch. Set {@code true} to skip this single bucket entirely (the request is
     * not charged against it) while every other limiter keeps running — lets you disable one stage
     * via yaml if its numbers are wrong, without turning off the whole chain. Default {@code false}
     * (active). Modeled as {@code disabled} rather than {@code enabled=true} so the default
     * survives both builder and YAML/Jackson construction (a {@code true} default would be dropped
     * to {@code false} by Lombok {@code @Builder.Default} under no-arg/Jackson binding).
     */
    private boolean disabled;

    /**
     * When true, the internal system principal bypasses this bucket (its calls are not charged).
     * Currently honored for heavy-resolver buckets (see {@code
     * RateLimitEngine#consumeHeavyResolver}) so high-volume internal/background traffic can skip a
     * hot resolver's tenant budget when that's desired. Default false — system calls are charged
     * like any other. Default-false keeps the value stable under YAML/Jackson binding.
     */
    private boolean exemptSystemActor;

    /**
     * Whether this bucket is keyed per actor. Only meaningful on the client-class buckets ({@code
     * browser}/{@code sdk}): {@code false} (default) = one bucket per class per tenant ({@code
     * {tenantId}:sdk}) shared by all actors of that class; {@code true} = keyed per actor ({@code
     * {tenantId}:sdk:{urn}}) so each actor gets its own class-sized budget and one noisy actor
     * can't drain the whole class. Set independently per class (e.g. per-actor for {@code sdk} but
     * shared for {@code browser}). Ignored on the {@code actor} bucket (inherently per-actor) and
     * {@code global} (inherently fleet-wide); falls back to the shared key when there is no actor
     * (system principal / unauthenticated).
     */
    private boolean perActor;

    private int capacity;
    private int refillTokens;

    // Plain initializer (not @Builder.Default) so a bucket created by no-arg/Jackson binding — e.g.
    // a
    // heavyResolvers entry that omits the period — still defaults to 60 instead of 0.
    private int refillPeriodSeconds = 60;
  }
}
