package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;
import org.springframework.util.AntPathMatcher;

@Getter
final class CompiledRateLimitRule {
  static final String DEFAULT_CAPACITY_ID = "_default_capacity";
  static final String GRAPHQL_CAPACITY_ID = "_graphql_capacity";
  static final String GLOBAL_PATH_PATTERN = "/**";

  private final String id;
  private final RateLimitRuleType type;
  private final String pathPattern;
  private final Set<String> methods;
  private final List<String> graphqlOperationNames;
  private final int specificityRank;
  private final int patternSpecificity;
  private final CapacityLimitConfig capacityConfig;
  private final Integer endpointCapacity;
  private final Integer refillTokens;
  private final Integer refillPeriodSeconds;

  private CompiledRateLimitRule(
      String id,
      RateLimitRuleType type,
      String pathPattern,
      Set<String> methods,
      List<String> graphqlOperationNames,
      int specificityRank,
      CapacityLimitConfig capacityConfig,
      Integer endpointCapacity,
      Integer refillTokens,
      Integer refillPeriodSeconds) {
    this.id = id;
    this.type = type;
    this.pathPattern = pathPattern;
    this.methods = methods;
    this.graphqlOperationNames =
        graphqlOperationNames == null ? List.of() : List.copyOf(graphqlOperationNames);
    this.specificityRank = specificityRank;
    this.patternSpecificity = computePatternSpecificity(pathPattern);
    this.capacityConfig = capacityConfig;
    this.endpointCapacity = endpointCapacity;
    this.refillTokens = refillTokens;
    this.refillPeriodSeconds = refillPeriodSeconds;
  }

  static CompiledRateLimitRule fromCapacityRuleConfig(
      @Nonnull RateLimitProperties.Rule config, @Nonnull String graphqlPathPattern) {
    return fromConfig(config, RateLimitRuleType.capacity, graphqlPathPattern);
  }

  static CompiledRateLimitRule fromEndpointRuleConfig(
      @Nonnull RateLimitProperties.Rule config, @Nonnull String graphqlPathPattern) {
    return fromConfig(config, RateLimitRuleType.endpoint, graphqlPathPattern);
  }

  private static CompiledRateLimitRule fromConfig(
      @Nonnull RateLimitProperties.Rule config,
      @Nonnull RateLimitRuleType type,
      @Nonnull String graphqlPathPattern) {
    int rank = computeRank(config, graphqlPathPattern);
    CapacityLimitConfig capacityConfig = null;
    if (type == RateLimitRuleType.capacity) {
      capacityConfig = new CapacityLimitConfig();
      if (config.getInitialLimit() != null) {
        capacityConfig.setInitialLimit(config.getInitialLimit());
      }
      if (config.getMinLimit() != null) {
        capacityConfig.setMinLimit(config.getMinLimit());
      }
      if (config.getMaxLimit() != null) {
        capacityConfig.setMaxLimit(config.getMaxLimit());
      }
      capacityConfig.setEnabled(true);
    }
    return new CompiledRateLimitRule(
        config.getId(),
        type,
        config.getPathPattern(),
        normalizeMethods(config.getMethods()),
        config.getGraphqlOperationNames(),
        rank,
        capacityConfig,
        config.getCapacity(),
        config.getRefillTokens(),
        config.getRefillPeriodSeconds());
  }

  static CompiledRateLimitRule materializedCapacityRule(
      String id, String pathPattern, int rank, CapacityLimitConfig capacityConfig) {
    return new CompiledRateLimitRule(
        id,
        RateLimitRuleType.capacity,
        pathPattern,
        Set.of("POST", "GET", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"),
        List.of(),
        rank,
        capacityConfig,
        null,
        null,
        null);
  }

  boolean matchesPath(@Nonnull AntPathMatcher matcher, @Nonnull String path) {
    return matcher.match(pathPattern, path);
  }

  boolean matchesMethod(@Nonnull String method) {
    return methods.contains(method.toUpperCase(Locale.ROOT));
  }

  boolean matchesOperation(@Nullable String operationName) {
    if (graphqlOperationNames.isEmpty()) {
      return true;
    }
    return operationName != null && graphqlOperationNames.contains(operationName);
  }

  boolean isOperationScoped() {
    return !graphqlOperationNames.isEmpty();
  }

  private static int computeRank(RateLimitProperties.Rule config, String graphqlPathPattern) {
    if (config.getGraphqlOperationNames() != null && !config.getGraphqlOperationNames().isEmpty()) {
      return 4;
    }
    if (graphqlPathPattern.equals(config.getPathPattern())) {
      return 3;
    }
    if (!GLOBAL_PATH_PATTERN.equals(config.getPathPattern())) {
      return 2;
    }
    return 1;
  }

  private static int computePatternSpecificity(String pathPattern) {
    return pathPattern == null ? 0 : pathPattern.replace("/**", "").length();
  }

  private static Set<String> normalizeMethods(List<String> methods) {
    if (methods == null || methods.isEmpty()) {
      return Set.of("POST");
    }
    return methods.stream()
        .map(method -> method.toUpperCase(Locale.ROOT))
        .collect(Collectors.toUnmodifiableSet());
  }

  static int compareSpecificity(CompiledRateLimitRule left, CompiledRateLimitRule right) {
    int rankCompare = Integer.compare(left.specificityRank, right.specificityRank);
    if (rankCompare != 0) {
      return rankCompare;
    }
    int patternCompare = Integer.compare(right.patternSpecificity, left.patternSpecificity);
    if (patternCompare != 0) {
      return patternCompare;
    }
    return left.id.compareTo(right.id);
  }
}
