package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Builder;
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
  private final boolean perActor;
  private final Set<ClientClass> clientClasses;

  @Builder
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
      Integer refillPeriodSeconds,
      boolean perActor,
      Set<ClientClass> clientClasses) {
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
    this.perActor = perActor;
    this.clientClasses = clientClasses == null ? Set.of() : Set.copyOf(clientClasses);
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
    return CompiledRateLimitRule.builder()
        .id(config.getId())
        .type(type)
        .pathPattern(config.getPathPattern())
        .methods(normalizeMethods(config.getMethods()))
        .graphqlOperationNames(config.getGraphqlOperationNames())
        .specificityRank(rank)
        .capacityConfig(capacityConfig)
        .endpointCapacity(config.getCapacity())
        .refillTokens(config.getRefillTokens())
        .refillPeriodSeconds(config.getRefillPeriodSeconds())
        .perActor(config.isPerActor())
        .clientClasses(normalizeClientClasses(config.getClientTypes()))
        .build();
  }

  static CompiledRateLimitRule materializedCapacityRule(
      String id, String pathPattern, int rank, CapacityLimitConfig capacityConfig) {
    return CompiledRateLimitRule.builder()
        .id(id)
        .type(RateLimitRuleType.capacity)
        .pathPattern(pathPattern)
        .methods(Set.of("POST", "GET", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"))
        .graphqlOperationNames(List.of())
        .specificityRank(rank)
        .capacityConfig(capacityConfig)
        .endpointCapacity(null)
        .refillTokens(null)
        .refillPeriodSeconds(null)
        .perActor(false)
        .clientClasses(Set.of())
        .build();
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

  /**
   * A class-agnostic rule (empty {@code clientClasses}) matches any client. A {@code null} client
   * class is a wildcard — it matches every rule — used when client-class selection is disabled so
   * class-scoped rules behave as if class-agnostic (current behavior).
   */
  boolean matchesClientClass(@Nullable ClientClass clientClass) {
    if (clientClasses.isEmpty() || clientClass == null) {
      return true;
    }
    return clientClasses.contains(clientClass);
  }

  boolean isClientClassScoped() {
    return !clientClasses.isEmpty();
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

  /**
   * Parses configured {@code clientTypes} (case- and hyphen-insensitive) into {@link ClientClass}.
   * Empty/null = class-agnostic. Unrecognized values are ignored rather than failing startup —
   * client class is advisory, and tolerating unknown values keeps the config forward-compatible
   * with finer-grained types a newer config may declare.
   */
  private static Set<ClientClass> normalizeClientClasses(List<String> clientTypes) {
    if (clientTypes == null || clientTypes.isEmpty()) {
      return Set.of();
    }
    Set<ClientClass> classes = EnumSet.noneOf(ClientClass.class);
    for (String value : clientTypes) {
      if (value == null) {
        continue;
      }
      String normalized = value.trim().toUpperCase(Locale.ROOT).replace('-', '_');
      for (ClientClass candidate : ClientClass.values()) {
        if (candidate.name().equals(normalized)) {
          classes.add(candidate);
        }
      }
    }
    return classes.isEmpty() ? Set.of() : Set.copyOf(classes);
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
    // At equal path specificity, a class-scoped rule outranks a class-agnostic one. Deliberate:
    // even when client-class selection is disabled (null class is a wildcard, so both rules match),
    // the more specific class-scoped rule wins — i.e. a stricter class rule still applies rather
    // than the looser catch-all. Asserted by RuleSelector/CompiledRateLimitRule tests.
    int classCompare = Boolean.compare(left.isClientClassScoped(), right.isClientClassScoped());
    if (classCompare != 0) {
      return classCompare;
    }
    return left.id.compareTo(right.id);
  }
}
