package com.linkedin.metadata.ratelimit;

import com.linkedin.metadata.config.ratelimit.CapacityLimitConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitGraphQLConfig;
import com.linkedin.metadata.config.ratelimit.RateLimitProperties;
import com.linkedin.metadata.config.ratelimit.RateLimitRuleType;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.util.AntPathMatcher;
import org.springframework.util.StringUtils;

final class RuleSelector {

  private final AntPathMatcher pathMatcher = new AntPathMatcher();
  private final List<CompiledRateLimitRule> capacityRules;
  private final List<CompiledRateLimitRule> endpointRules;
  private final String graphqlPathPattern;

  RuleSelector(@Nonnull RateLimitProperties config) {
    RateLimitGraphQLConfig graphql = requireGraphqlConfig(config);
    this.graphqlPathPattern = requireGraphqlPathPattern(graphql);
    this.capacityRules = compileCapacityRules(config, graphql);
    this.endpointRules = compileEndpointRules(config, graphql);
  }

  @Nonnull
  private static RateLimitGraphQLConfig requireGraphqlConfig(@Nonnull RateLimitProperties config) {
    if (config.getCapacity() == null || config.getCapacity().getGraphql() == null) {
      throw new IllegalStateException("capacity.graphql is required");
    }
    return config.getCapacity().getGraphql();
  }

  @Nonnull
  private static String requireGraphqlPathPattern(@Nonnull RateLimitGraphQLConfig graphql) {
    if (!StringUtils.hasText(graphql.getPathPattern())) {
      throw new IllegalStateException("capacity.graphql.pathPattern is required");
    }
    return graphql.getPathPattern();
  }

  @Nullable
  CompiledRateLimitRule selectCapacityRule(
      @Nonnull String path, @Nonnull String method, @Nullable String operationName) {
    return capacityRules.stream()
        .filter(rule -> rule.matchesPath(pathMatcher, path))
        .filter(rule -> rule.matchesMethod(method))
        .filter(rule -> rule.matchesOperation(operationName))
        .max((left, right) -> CompiledRateLimitRule.compareSpecificity(left, right))
        .orElse(null);
  }

  @Nullable
  CompiledRateLimitRule selectEndpointRule(
      @Nonnull String path, @Nonnull String method, @Nullable String operationName) {
    return endpointRules.stream()
        .filter(rule -> rule.matchesPath(pathMatcher, path))
        .filter(rule -> rule.matchesMethod(method))
        .filter(rule -> rule.matchesOperation(operationName))
        .max((left, right) -> CompiledRateLimitRule.compareSpecificity(left, right))
        .orElse(null);
  }

  boolean isGraphQLPost(@Nonnull String path, @Nonnull String method) {
    return "POST".equalsIgnoreCase(method) && pathMatcher.match(graphqlPathPattern, path);
  }

  @Nonnull
  String getGraphqlPathPattern() {
    return graphqlPathPattern;
  }

  /** Capacity limiter ids and configs — must match {@link #selectCapacityRule} outcomes. */
  Map<String, CapacityLimitConfig> getCapacityLimiterConfigs() {
    Map<String, CapacityLimitConfig> configs = new HashMap<>();
    for (CompiledRateLimitRule rule : capacityRules) {
      if (rule.getType() == RateLimitRuleType.capacity && rule.getCapacityConfig() != null) {
        configs.put(rule.getId(), rule.getCapacityConfig());
      }
    }
    return Map.copyOf(configs);
  }

  private List<CompiledRateLimitRule> compileCapacityRules(
      RateLimitProperties config, @Nonnull RateLimitGraphQLConfig graphql) {
    List<CompiledRateLimitRule> rules = new ArrayList<>();

    if (config.getCapacity() == null || !config.getCapacity().isEnabled()) {
      return List.of();
    }

    CapacityLimitConfig defaultCapacity = config.getCapacity().getDefaultCapacity();
    if (defaultCapacity != null && defaultCapacity.isEnabled()) {
      rules.add(
          CompiledRateLimitRule.materializedCapacityRule(
              CompiledRateLimitRule.DEFAULT_CAPACITY_ID,
              CompiledRateLimitRule.GLOBAL_PATH_PATTERN,
              1,
              defaultCapacity));
    }

    if (graphql.isEnabled()) {
      rules.add(
          CompiledRateLimitRule.materializedCapacityRule(
              CompiledRateLimitRule.GRAPHQL_CAPACITY_ID, graphqlPathPattern, 3, graphql));
    }

    if (config.getCapacity().getRules() != null) {
      for (RateLimitProperties.Rule ruleConfig : config.getCapacity().getRules()) {
        if (!graphql.isOperationRulesEnabled()
            && ruleConfig.getGraphqlOperationNames() != null
            && !ruleConfig.getGraphqlOperationNames().isEmpty()) {
          continue;
        }
        rules.add(CompiledRateLimitRule.fromCapacityRuleConfig(ruleConfig, graphqlPathPattern));
      }
    }

    return List.copyOf(rules);
  }

  private List<CompiledRateLimitRule> compileEndpointRules(
      RateLimitProperties config, @Nonnull RateLimitGraphQLConfig graphql) {
    if (config.getEndpoint() == null || !config.getEndpoint().isEnabled()) {
      return List.of();
    }
    List<CompiledRateLimitRule> rules = new ArrayList<>();
    if (config.getEndpoint().getRules() != null) {
      for (RateLimitProperties.Rule ruleConfig : config.getEndpoint().getRules()) {
        if (!graphql.isOperationRulesEnabled()
            && ruleConfig.getGraphqlOperationNames() != null
            && !ruleConfig.getGraphqlOperationNames().isEmpty()) {
          continue;
        }
        rules.add(CompiledRateLimitRule.fromEndpointRuleConfig(ruleConfig, graphqlPathPattern));
      }
    }
    return List.copyOf(rules);
  }
}
