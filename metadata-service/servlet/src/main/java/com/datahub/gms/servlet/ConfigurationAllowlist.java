package com.datahub.gms.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds allowed configuration from ConfigurationProvider using secure allowlist rules.
 *
 * <p>This class: - Applies ConfigSectionRule instances to filter configuration sections - Uses
 * secure-by-default approach (only explicitly allowed fields are exposed) - Supports field-level
 * filtering within sections - Handles section renaming for backward compatibility
 */
@Slf4j
public class ConfigurationAllowlist {

  private final List<ConfigSectionRule> rules;
  private final ObjectMapper objectMapper;

  public ConfigurationAllowlist(List<ConfigSectionRule> rules, ObjectMapper objectMapper) {
    this.rules = rules;
    this.objectMapper = objectMapper;
  }

  /**
   * Builds the allowed configuration from the ConfigurationProvider.
   *
   * @param configProvider The configuration provider to extract data from
   * @return Map containing only the allowed configuration sections and fields
   */
  @Nonnull
  public Map<String, Object> buildAllowedConfiguration(ConfigurationProvider configProvider) {
    Map<String, Object> result = new LinkedHashMap<>();

    for (ConfigSectionRule rule : rules) {
      if (!rule.isIncludeSection()) {
        continue;
      }

      try {
        Object sectionData = extractConfigurationSection(configProvider, rule.getSectionPath());
        if (sectionData != null) {
          Object filteredData = applyFieldFiltering(sectionData, rule);
          if (filteredData != null) {
            result.put(rule.getOutputPath(), filteredData);
          }
        }
      } catch (Exception e) {
        log.warn("Failed to extract configuration section: {}", rule.getSectionPath(), e);
        // Continue with other rules - don't fail the entire request
      }
    }

    return result;
  }

  /** Extracts a configuration section from the ConfigurationProvider using reflection. */
  private Object extractConfigurationSection(
      ConfigurationProvider configProvider, String sectionPath) throws Exception {
    // Convert sectionPath to getter method name (e.g., "datahub" -> "getDatahub")
    String methodName = "get" + capitalize(sectionPath);

    try {
      Method method = configProvider.getClass().getMethod(methodName);
      return method.invoke(configProvider);
    } catch (NoSuchMethodException e) {
      log.debug(
          "No getter method found for section: {} (tried method: {})", sectionPath, methodName);
      return null;
    }
  }

  /**
   * Applies field filtering to a configuration section according to the rule. Supports both simple
   * field names and nested dot notation paths.
   */
  private Object applyFieldFiltering(Object sectionData, ConfigSectionRule rule) {
    if (rule.isAllFieldsAllowed()) {
      // No field filtering needed - return entire section converted to Map
      return objectMapper.convertValue(sectionData, Map.class);
    }

    // Convert to Map for field-level filtering
    Map<String, Object> sectionMap = objectMapper.convertValue(sectionData, Map.class);
    Map<String, Object> filteredMap = new LinkedHashMap<>();

    // Process top-level fields
    Set<String> topLevelFields = rule.getTopLevelFields();
    for (String fieldName : topLevelFields) {
      if (sectionMap.containsKey(fieldName)) {
        filteredMap.put(fieldName, sectionMap.get(fieldName));
      } else {
        log.warn(
            "Configured field '{}' not found in section '{}' - rule may be stale",
            fieldName,
            rule.getSectionPath());
      }
    }

    // Process nested paths
    for (Map.Entry<String, Object> entry : sectionMap.entrySet()) {
      String fieldName = entry.getKey();

      // Skip if already processed as top-level field
      if (topLevelFields.contains(fieldName)) {
        continue;
      }

      // Check if this field has nested paths configured
      if (rule.hasNestedPathsForField(fieldName)) {
        Object nestedValue = entry.getValue();
        Object filteredNestedValue = applyNestedPathFiltering(nestedValue, fieldName, rule);

        if (filteredNestedValue != null) {
          filteredMap.put(fieldName, filteredNestedValue);
        }
      } else {
        log.debug(
            "Field '{}' filtered out from section '{}' by allowlist rule",
            fieldName,
            rule.getSectionPath());
      }
    }

    return filteredMap.isEmpty() ? null : filteredMap;
  }

  /** Applies filtering to nested objects based on dot notation paths. */
  private Object applyNestedPathFiltering(
      Object nestedData, String parentPath, ConfigSectionRule rule) {
    if (nestedData == null) {
      return null;
    }

    // If it's an array, include the entire array (per design decision)
    if (nestedData instanceof java.util.List || nestedData.getClass().isArray()) {
      return nestedData;
    }

    // For objects, filter based on nested paths
    Map<String, Object> nestedMap = objectMapper.convertValue(nestedData, Map.class);
    Map<String, Object> filteredNestedMap = new LinkedHashMap<>();

    Set<String> allowedNestedPaths = rule.getAllowedPathsWithPrefix(parentPath);

    for (String fullPath : allowedNestedPaths) {
      String[] pathParts = fullPath.split("\\.");
      if (pathParts.length < 2) {
        continue; // Should not happen, but safety check
      }

      // Extract the field name after the parent path
      String nestedFieldName =
          pathParts[1]; // e.g., "signingAlgorithm" from "tokenService.signingAlgorithm"

      if (nestedMap.containsKey(nestedFieldName)) {
        // Handle deeper nesting if needed
        if (pathParts.length > 2) {
          // Reconstruct remaining path for recursive processing
          String remainingPath =
              String.join(".", java.util.Arrays.copyOfRange(pathParts, 1, pathParts.length));
          String newParentPath =
              String.join(".", java.util.Arrays.copyOfRange(pathParts, 0, pathParts.length - 1));

          Object deepValue = nestedMap.get(nestedFieldName);
          Object filteredDeepValue = applyNestedPathFiltering(deepValue, newParentPath, rule);

          if (filteredDeepValue != null) {
            filteredNestedMap.put(nestedFieldName, filteredDeepValue);
          }
        } else {
          // Direct field - include it
          filteredNestedMap.put(nestedFieldName, nestedMap.get(nestedFieldName));
        }
      } else {
        log.warn(
            "Configured nested path '{}' not found in section '{}' - rule may be stale",
            fullPath,
            rule.getSectionPath());
      }
    }

    return filteredNestedMap.isEmpty() ? null : filteredNestedMap;
  }

  /** Capitalizes the first letter of a string. */
  private String capitalize(String str) {
    if (str == null || str.isEmpty()) {
      return str;
    }
    return str.substring(0, 1).toUpperCase() + str.substring(1);
  }

  /**
   * Creates a default allowlist with safe configuration sections.
   *
   * @param objectMapper ObjectMapper for JSON conversion
   * @return ConfigurationAllowlist with predefined safe rules
   */
  public static ConfigurationAllowlist createDefault(ObjectMapper objectMapper) {
    List<ConfigSectionRule> defaultRules =
        Arrays.asList(
            // Authentication - expose basic settings but not credentials, using nested paths for
            // tokenService
            ConfigSectionRule.include(
                "authentication",
                Set.of(
                    "enabled",
                    "defaultProvider",
                    "systemClientEnabled",
                    "sessionTokenDurationHours",
                    // TokenService nested paths - include safe fields only
                    "tokenService.signingAlgorithm", // ✅ Safe: algorithm type
                    "tokenService.issuer", // ✅ Safe: issuer name
                    "tokenService.audience", // ✅ Safe: audience name
                    "tokenService.accessTokenTtlSeconds", // ✅ Safe: TTL setting
                    "tokenService.refreshTokenTtlSeconds" // ✅ Safe: TTL setting
                    // ❌ Excluded: tokenService.signingKey, tokenService.refreshSigningKey
                    // (sensitive!)
                    // ❌ Excluded: systemClientId, systemClientSecret (sensitive!)
                    )),

            // Kafka - expose basic connection info but not security credentials
            ConfigSectionRule.include(
                "kafka",
                Set.of(
                    "bootstrapServers",
                    "compressionType",
                    "maxRequestSize",
                    "retries",
                    "deliveryTimeout",
                    // Connection settings
                    "producer.bootstrapServers", // ✅ Safe: connection info
                    "producer.compressionType", // ✅ Safe: performance setting
                    "consumer.bootstrapServers" // ✅ Safe: connection info
                    // ❌ Excluded: security.protocol, sasl.*, ssl.* (sensitive!)
                    )),

            // Spring/Actuator - expose basic management info
            ConfigSectionRule.include(
                "springActuator",
                Set.of(
                    "enabled",
                    "endpoints.web.exposure.include", // ✅ Safe: exposed endpoints list
                    "endpoints.web.base-path" // ✅ Safe: base path
                    // ❌ Excluded: security.*, management.security.* (sensitive!)
                    )),

            // Cache - expose basic cache configuration
            ConfigSectionRule.include(
                "cache",
                Set.of(
                    "client",
                    "ttlSeconds",
                    "maxSize",
                    "redis.host", // ✅ Safe: connection info (hostname)
                    "redis.port", // ✅ Safe: connection info (port)
                    "redis.database" // ✅ Safe: database number
                    // ❌ Excluded: redis.password, redis.username, redis.ssl.* (sensitive!)
                    )),

            // Metadata service - expose basic settings
            ConfigSectionRule.include(
                "metadataService",
                Set.of(
                    "host",
                    "port",
                    "useSSL",
                    "restli.server.host", // ✅ Safe: server host
                    "restli.server.port" // ✅ Safe: server port
                    // ❌ Excluded: auth.*, ssl.keystore.*, certificates.* (sensitive!)
                    )));

    return new ConfigurationAllowlist(defaultRules, objectMapper);
  }

  /**
   * Creates an allowlist with custom rules for specific use cases.
   *
   * @param customRules List of custom ConfigSectionRule instances
   * @param objectMapper ObjectMapper for JSON conversion
   * @return ConfigurationAllowlist with custom rules
   */
  public static ConfigurationAllowlist createCustom(
      List<ConfigSectionRule> customRules, ObjectMapper objectMapper) {
    return new ConfigurationAllowlist(customRules, objectMapper);
  }
}
