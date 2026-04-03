package com.linkedin.gms.factory.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.config.requestcontext.ContextHeaderConfiguration;
import com.linkedin.metadata.config.requestcontext.RequestContextConfiguration;
import io.datahubproject.metadata.context.ObjectMapperContext;
import io.datahubproject.metadata.context.request.DataHubContextKeyRule;
import io.datahubproject.metadata.context.request.DataHubContextParsePolicy;
import io.datahubproject.metadata.context.request.DataHubContextParser;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public final class DataHubContextPolicyFactory {

  private static final int MAX_VALUE_LENGTH_CAP = 256;

  private DataHubContextPolicyFactory() {}

  /**
   * Same as {@link #from(RequestContextConfiguration, ObjectMapper)} using {@link
   * ObjectMapperContext#DEFAULT}'s JSON mapper (matches stock {@code OperationContext}).
   */
  public static DataHubContextParsePolicy from(@Nonnull RequestContextConfiguration cfg) {
    return from(cfg, ObjectMapperContext.DEFAULT.getObjectMapper());
  }

  public static DataHubContextParsePolicy from(
      @Nonnull RequestContextConfiguration cfg, @Nonnull ObjectMapper objectMapper) {
    Objects.requireNonNull(cfg, "requestContext configuration must be set");
    Objects.requireNonNull(objectMapper, "ObjectMapper must be set");
    ContextHeaderConfiguration ch =
        Objects.requireNonNull(cfg.getContextHeader(), "requestContext.contextHeader must be set");
    int maxLen = ch.getMaxValueLength();
    if (maxLen < 1) {
      maxLen = DataHubContextParser.DEFAULT_MAX_VALUE_LENGTH;
    }
    maxLen = Math.min(maxLen, MAX_VALUE_LENGTH_CAP);

    String unspecified =
        DataHubContextParser.sanitizeValue(
            ch.getUnspecifiedLabel(), maxLen, DataHubContextParser.UNSPECIFIED);
    String other =
        DataHubContextParser.sanitizeValue(
            ch.getOtherLabel(), maxLen, DataHubContextParsePolicy.DEFAULT_OTHER_LABEL);

    String allowJson = Objects.requireNonNullElse(ch.getValueAllowlistsJson(), "");
    Map<String, Set<String>> allowByKey =
        parseValueAllowlistsJson(allowJson, maxLen, unspecified, objectMapper);
    if (allowByKey.isEmpty()) {
      return DataHubContextParsePolicy.defaults();
    }

    List<String> sortedKeys = new ArrayList<>(allowByKey.keySet());
    Collections.sort(sortedKeys);

    List<DataHubContextKeyRule> rules = new ArrayList<>();
    for (String headerKey : sortedKeys) {
      String metricTag = "agent_" + headerKey.replace('-', '_');
      Set<String> allowed = allowByKey.get(headerKey);
      Set<String> ruleAllowed = allowed.isEmpty() ? Set.of() : Set.copyOf(allowed);
      rules.add(new DataHubContextKeyRule(headerKey, metricTag, ruleAllowed));
    }

    return new DataHubContextParsePolicy(unspecified, other, maxLen, List.copyOf(rules));
  }

  /**
   * Parses a JSON array of {@code {"key":"...","values":["..."]}} objects (or a single such
   * object). Missing {@code values}, null, or empty array means unrestricted for that key.
   * Duplicate keys merge allowed values.
   */
  static Map<String, Set<String>> parseValueAllowlistsJson(
      @Nonnull String json, int maxLen, String unspecifiedToken) {
    return parseValueAllowlistsJson(
        json, maxLen, unspecifiedToken, ObjectMapperContext.DEFAULT.getObjectMapper());
  }

  static Map<String, Set<String>> parseValueAllowlistsJson(
      @Nonnull String json,
      int maxLen,
      String unspecifiedToken,
      @Nonnull ObjectMapper objectMapper) {
    Map<String, Set<String>> allowByKey = new HashMap<>();
    if (json.isBlank()) {
      return allowByKey;
    }
    JsonNode root;
    try {
      root = objectMapper.readTree(json.trim());
    } catch (JsonProcessingException e) {
      log.warn("Invalid valueAllowlistsJson", e);
      return allowByKey;
    }
    if (root.isObject()) {
      mergeAllowlistObject(root, allowByKey, maxLen, unspecifiedToken);
    } else if (root.isArray()) {
      for (JsonNode el : root) {
        if (el.isObject()) {
          mergeAllowlistObject(el, allowByKey, maxLen, unspecifiedToken);
        }
      }
    } else {
      log.warn("valueAllowlistsJson must be a JSON array or object, got: {}", root.getNodeType());
    }
    return allowByKey;
  }

  private static void mergeAllowlistObject(
      JsonNode obj, Map<String, Set<String>> allowByKey, int maxLen, String unspecifiedToken) {
    JsonNode keyNode = obj.get("key");
    if (keyNode == null || !keyNode.isTextual()) {
      log.warn("Skipping allowlist entry (missing string \"key\"): {}", obj);
      return;
    }
    String keyPart = keyNode.asText().trim().toLowerCase(Locale.ROOT);
    if (keyPart.isEmpty()) {
      return;
    }
    JsonNode valuesNode = obj.get("values");
    if (valuesNode == null
        || valuesNode.isNull()
        || (valuesNode.isArray() && valuesNode.isEmpty())) {
      allowByKey.put(keyPart, new HashSet<>());
      return;
    }
    if (!valuesNode.isArray()) {
      log.warn("Skipping allowlist entry (\"values\" must be an array): {}", obj);
      return;
    }
    Set<String> bucket = allowByKey.computeIfAbsent(keyPart, k -> new HashSet<>());
    for (JsonNode el : valuesNode) {
      if (el.isTextual()) {
        String t = DataHubContextParser.sanitizeValue(el.asText(), maxLen, unspecifiedToken);
        if (!unspecifiedToken.equals(t)) {
          bucket.add(t);
        }
      }
    }
  }
}
