package com.linkedin.metadata.config.ratelimit;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import org.springframework.util.StringUtils;

/**
 * Startup validation for the bound {@link RateLimitProperties}. Spring now binds the whole
 * rate-limit config (application.yaml toggles + rate-limit-config.yaml policy, with env
 * placeholders resolved), so there is no custom loader to validate during; the engine factory calls
 * {@link #validate} on the bound bean before constructing the engine, failing fast on a
 * misconfiguration rather than silently mis-limiting traffic. Scoped-chain sizing is validated
 * separately by the engine at construction.
 */
public final class RateLimitConfigValidator {

  private RateLimitConfigValidator() {}

  public static void validate(RateLimitProperties config) {
    ensureNestedDefaults(config);
    String graphqlPath = config.getCapacity().getGraphql().getPathPattern();
    List<String> validationErrors = new ArrayList<>();

    if (!StringUtils.hasText(graphqlPath)) {
      validationErrors.add("capacity.graphql.pathPattern is required");
    }

    validateCapacityLimitConfig(
        "capacity.default", config.getCapacity().getDefaultCapacity(), validationErrors);
    validateCapacityLimitConfig(
        "capacity.graphql", config.getCapacity().getGraphql(), validationErrors);
    validateCapacityRules(config.getCapacity().getRules(), graphqlPath, validationErrors);
    validateEndpointRules(config.getEndpoint().getRules(), graphqlPath, validationErrors);
    validateRetryAfterConfig(config, validationErrors);

    if (!validationErrors.isEmpty()) {
      throw new IllegalStateException(
          "Invalid rate limit configuration: " + String.join("; ", validationErrors));
    }
  }

  private static void ensureNestedDefaults(RateLimitProperties target) {
    if (target.getCapacity() == null) {
      target.setCapacity(new RateLimitProperties.Capacity());
    }
    if (target.getCapacity().getDefaultCapacity() == null) {
      target.getCapacity().setDefaultCapacity(new CapacityLimitConfig());
    }
    if (target.getCapacity().getGraphql() == null) {
      target.getCapacity().setGraphql(new RateLimitGraphQLConfig());
    }
    if (target.getCapacity().getRules() == null) {
      target.getCapacity().setRules(new ArrayList<>());
    }
    if (target.getEndpoint() == null) {
      target.setEndpoint(new RateLimitProperties.Endpoint());
    }
    if (target.getEndpoint().getRules() == null) {
      target.getEndpoint().setRules(new ArrayList<>());
    }
    if (target.getMetrics() == null) {
      target.setMetrics(new RateLimitProperties.Metrics());
    }
    if (target.getScoped() == null) {
      target.setScoped(new RateLimitProperties.ScopedLimits());
    }
  }

  private static void validateCapacityRules(
      List<RateLimitProperties.Rule> rules, String graphqlPath, List<String> validationErrors) {
    CapacityLimitConfig ruleDefaults = new CapacityLimitConfig();
    for (RateLimitProperties.Rule rule : rules) {
      validateCommonRuleFields(rule, graphqlPath, validationErrors, "capacity");
      if (rule.getInitialLimit() == null && rule.getMaxLimit() == null) {
        validationErrors.add("Capacity rate limit rule " + rule.getId() + " requires limit fields");
      }
      validateCapacityLimits(
          "Capacity rate limit rule " + rule.getId(),
          effectiveLimit(rule.getMinLimit(), ruleDefaults.getMinLimit()),
          effectiveLimit(rule.getInitialLimit(), ruleDefaults.getInitialLimit()),
          effectiveLimit(rule.getMaxLimit(), ruleDefaults.getMaxLimit()),
          validationErrors);
    }
  }

  private static void validateCapacityLimitConfig(
      String configPath, CapacityLimitConfig config, List<String> validationErrors) {
    validateCapacityLimits(
        configPath,
        config.getMinLimit(),
        config.getInitialLimit(),
        config.getMaxLimit(),
        validationErrors);
  }

  private static int effectiveLimit(Integer value, int defaultValue) {
    return value != null ? value : defaultValue;
  }

  private static void validateCapacityLimits(
      String configPath,
      int minLimit,
      int initialLimit,
      int maxLimit,
      List<String> validationErrors) {
    if (minLimit <= 0) {
      validationErrors.add(configPath + " minLimit must be > 0");
    }
    if (initialLimit <= 0) {
      validationErrors.add(configPath + " initialLimit must be > 0");
    }
    if (maxLimit <= 0) {
      validationErrors.add(configPath + " maxLimit must be > 0");
    }
    if (minLimit > initialLimit) {
      validationErrors.add(configPath + " minLimit must be <= initialLimit");
    }
    if (initialLimit > maxLimit) {
      validationErrors.add(configPath + " initialLimit must be <= maxLimit");
    }
  }

  private static void validateEndpointRules(
      List<RateLimitProperties.Rule> rules, String graphqlPath, List<String> validationErrors) {
    for (RateLimitProperties.Rule rule : rules) {
      validateCommonRuleFields(rule, graphqlPath, validationErrors, "endpoint");
      if (rule.getCapacity() == null
          || rule.getRefillTokens() == null
          || rule.getRefillPeriodSeconds() == null) {
        validationErrors.add(
            "Endpoint rate limit rule " + rule.getId() + " requires capacity/refill fields");
      }
    }
  }

  private static void validateRetryAfterConfig(
      RateLimitProperties config, List<String> validationErrors) {
    if (config.getMinRetryAfterSeconds() < 0) {
      validationErrors.add("minRetryAfterSeconds must be >= 0");
    }
    if (config.getRetryAfterJitterPercent() < 0 || config.getRetryAfterJitterPercent() > 100) {
      validationErrors.add("retryAfterJitterPercent must be between 0 and 100");
    }
  }

  private static void validateCommonRuleFields(
      RateLimitProperties.Rule rule,
      String graphqlPath,
      List<String> validationErrors,
      String listLabel) {
    if (!StringUtils.hasText(rule.getId())) {
      validationErrors.add(listLabel + " rate limit rule missing id");
    }
    if (!StringUtils.hasText(rule.getPathPattern())) {
      validationErrors.add(listLabel + " rate limit rule " + rule.getId() + " missing pathPattern");
    }
    if (rule.getMethods() == null || rule.getMethods().isEmpty()) {
      validationErrors.add(listLabel + " rate limit rule " + rule.getId() + " missing methods");
    }
    if (rule.getGraphqlOperationNames() != null
        && !rule.getGraphqlOperationNames().isEmpty()
        && !Objects.equals(graphqlPath, rule.getPathPattern())) {
      validationErrors.add(
          listLabel
              + " rate limit rule "
              + rule.getId()
              + " with graphqlOperationNames must use capacity.graphql.pathPattern");
    }
  }
}
