package com.linkedin.metadata.config.ratelimit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.util.StringUtils;

@Slf4j
public class RateLimitConfigLoader {

  public static final String RATE_LIMITS_CONFIG_JSON_ENV = "RATE_LIMITS_CONFIG_JSON";

  private final ObjectMapper jsonMapper;
  private final ObjectMapper yamlMapper;

  public RateLimitConfigLoader(ObjectMapper jsonMapper, ObjectMapper yamlMapper) {
    this.jsonMapper = jsonMapper;
    this.yamlMapper = yamlMapper;
  }

  /**
   * Load effective rate limit configuration: bundled defaults + env scalars, optional external
   * file, optional JSON env overlay.
   */
  public RateLimitProperties loadEffective(RateLimitProperties fromSpring) {
    RateLimitProperties effective = deepCopy(fromSpring);
    ensureNestedDefaults(effective);

    if (effective.getConfigFile() != null
        && effective.getConfigFile().isEnabled()
        && StringUtils.hasText(effective.getConfigFile().getPath())) {
      applyFileOverlay(effective.getConfigFile().getPath(), effective);
    }

    String jsonOverlay = System.getenv(RATE_LIMITS_CONFIG_JSON_ENV);
    if (StringUtils.hasText(jsonOverlay)) {
      applyJsonOverlay(jsonOverlay, effective, RATE_LIMITS_CONFIG_JSON_ENV);
    }

    validate(effective);
    return effective;
  }

  /** Package-visible for tests — applies a JSON overlay without env vars. */
  void applyJsonOverlay(String json, RateLimitProperties target, String sourceLabel) {
    try {
      JsonNode root = jsonMapper.readTree(json);
      JsonNode rateLimitsNode = root.has("rateLimits") ? root.get("rateLimits") : root;
      mergeOverlayNode(rateLimitsNode, target, sourceLabel);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to parse rate limit overlay from " + sourceLabel, e);
    }
  }

  private void applyFileOverlay(String path, RateLimitProperties target) {
    try (InputStream stream = openConfigStream(path)) {
      JsonNode root = yamlMapper.readTree(stream);
      JsonNode rateLimitsNode = root.has("rateLimits") ? root.get("rateLimits") : root;
      mergeOverlayNode(rateLimitsNode, target, path);
    } catch (FileNotFoundException e) {
      log.warn(
          "Rate limit configuration file was NOT found at {} — continuing with bundled defaults. "
              + "If RATE_LIMITS_CONFIG_FILE_ENABLED=true, verify the mount path.",
          path);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to load rate limit configuration from: " + path, e);
    }
  }

  private InputStream openConfigStream(String path) throws IOException {
    ClassPathResource classpathResource = new ClassPathResource(path);
    if (classpathResource.exists()) {
      log.info("Rate limit configuration found in classpath: {}", path);
      return classpathResource.getInputStream();
    }
    log.debug("Rate limit configuration was NOT found in classpath: {}", path);
    FileSystemResource filesystemResource = new FileSystemResource(path);
    if (!filesystemResource.exists()) {
      throw new FileNotFoundException(path);
    }
    log.info("Rate limit configuration found in filesystem: {}", path);
    return filesystemResource.getInputStream();
  }

  private void mergeOverlayNode(JsonNode overlay, RateLimitProperties target, String sourceLabel)
      throws IOException {
    if (overlay == null || overlay.isNull() || overlay.isMissingNode()) {
      return;
    }
    ensureNestedDefaults(target);
    jsonMapper.readerForUpdating(target).readValue(overlay);
    int ruleCount = countRulesInOverlay(overlay);
    if (ruleCount > 0) {
      log.info("Applied {} rate limit rules from {}", ruleCount, sourceLabel);
    }
  }

  private int countRulesInOverlay(JsonNode overlay) {
    int count = 0;
    if (overlay.has("capacity")
        && overlay.get("capacity").has("rules")
        && overlay.get("capacity").get("rules").isArray()) {
      count += overlay.get("capacity").get("rules").size();
    }
    if (overlay.has("endpoint")
        && overlay.get("endpoint").has("rules")
        && overlay.get("endpoint").get("rules").isArray()) {
      count += overlay.get("endpoint").get("rules").size();
    }
    return count;
  }

  private void ensureNestedDefaults(RateLimitProperties target) {
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
    if (target.getConfigFile() == null) {
      target.setConfigFile(new RateLimitProperties.ConfigFile());
    }
  }

  private RateLimitProperties deepCopy(RateLimitProperties source) {
    return jsonMapper.convertValue(source, RateLimitProperties.class);
  }

  public void validate(RateLimitProperties config) {
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
    validateEndpointConfig(config.getEndpoint(), graphqlPath, validationErrors);
    validateRetryAfterConfig(config, validationErrors);

    if (!validationErrors.isEmpty()) {
      throw new IllegalStateException(
          "Invalid rate limit configuration: " + String.join("; ", validationErrors));
    }
  }

  private void validateCapacityRules(
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

  private void validateCapacityLimitConfig(
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

  private void validateEndpointConfig(
      RateLimitProperties.Endpoint endpoint, String graphqlPath, List<String> validationErrors) {
    validateEndpointRules(endpoint.getRules(), graphqlPath, validationErrors);
  }

  private void validateEndpointRules(
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

  private void validateRetryAfterConfig(RateLimitProperties config, List<String> validationErrors) {
    if (config.getMinRetryAfterSeconds() < 0) {
      validationErrors.add("minRetryAfterSeconds must be >= 0");
    }
    if (config.getRetryAfterJitterPercent() < 0 || config.getRetryAfterJitterPercent() > 100) {
      validationErrors.add("retryAfterJitterPercent must be between 0 and 100");
    }
  }

  private void validateCommonRuleFields(
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

  /** Wrapper for external YAML files — top-level {@code rateLimits:} fragment. */
  public static class RateLimitPropertiesWrapper {
    private RateLimitProperties rateLimits;

    public RateLimitProperties getRateLimits() {
      return rateLimits;
    }

    public void setRateLimits(RateLimitProperties rateLimits) {
      this.rateLimits = rateLimits;
    }
  }
}
