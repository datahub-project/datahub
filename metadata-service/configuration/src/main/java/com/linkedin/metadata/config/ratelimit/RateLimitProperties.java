package com.linkedin.metadata.config.ratelimit;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class RateLimitProperties {
  @Builder.Default private boolean failOpen = true;
  @Builder.Default private int minRetryAfterSeconds = 60;
  @Builder.Default private int retryAfterJitterPercent = 10;

  @Builder.Default
  private String excludedPaths =
      "/health,/health/live,/actuator/prometheus,/openapi/v1/rate-limits/**";

  @Builder.Default private ConfigFile configFile = new ConfigFile();
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
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class ConfigFile {
    private boolean enabled;
    private String path;
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

    /** When true, enforce cluster-wide token-bucket (Bucket4j) endpoint limits via Hazelcast. */
    @Builder.Default private boolean enabled = false;

    @Builder.Default private String hazelcastMapName = DEFAULT_HAZELCAST_MAP_NAME;

    @Builder.Default private List<Rule> rules = new ArrayList<>();
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class Metrics {
    private boolean detailed;
  }
}
