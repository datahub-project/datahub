package com.datahub.gms.servlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Builds allowed configuration from ConfigurationProvider using secure allowlist rules.
 *
 * <p>This class implements a security-critical configuration filtering system that exposes only
 * explicitly allowlisted configuration data. It operates on a "secure by default" principle where
 * all configuration data is filtered out unless explicitly included via allowlist rules.
 *
 * <h2>Core Security Model</h2>
 *
 * <ul>
 *   <li><strong>Allowlist Only</strong>: Only explicitly specified fields are included in output
 *   <li><strong>Leaf Node Matching</strong>: Only leaf values (primitives, arrays, empty maps) can
 *       be included via top-level rules
 *   <li><strong>Nested Path Support</strong>: Complex objects must be accessed via explicit
 *       dot-notation paths
 *   <li><strong>No Wildcards</strong>: No pattern matching or wildcards - every exposed field must
 *       be explicitly specified
 * </ul>
 *
 * <h2>Rule Types and Behavior</h2>
 *
 * <h3>Section Rules</h3>
 *
 * <ul>
 *   <li><strong>Include Sections</strong>: {@code ConfigSectionRule.include("sectionName",
 *       allowedFields)}
 *   <li><strong>Exclude Sections</strong>: {@code ConfigSectionRule.exclude("sectionName")} -
 *       completely omits section
 *   <li><strong>Section Renaming</strong>: {@code ConfigSectionRule.include("original", "renamed",
 *       fields)} - maps section names
 * </ul>
 *
 * <h3>Field Matching Rules</h3>
 *
 * <ul>
 *   <li><strong>All Fields</strong>: {@code Set.of()} or {@code null} - includes entire section
 *       (use sparingly)
 *   <li><strong>Top-Level Fields</strong>: {@code "fieldName"} - includes field only if it's a leaf
 *       value
 *   <li><strong>Nested Paths</strong>: {@code "parent.child.field"} - includes specific nested
 *       field using dot notation
 * </ul>
 *
 * <h2>Leaf Node Detection</h2>
 *
 * <p>The system differentiates between "leaf" and "non-leaf" values to prevent accidental exposure
 * of complex objects:
 *
 * <h3>Leaf Values (Can be included via top-level rules)</h3>
 *
 * <ul>
 *   <li><strong>Primitives</strong>: String, Number, Boolean
 *   <li><strong>Arrays/Lists</strong>: Included entirely (design decision for simplicity)
 *   <li><strong>Empty Maps</strong>: Maps with no entries
 * </ul>
 *
 * <h3>Non-Leaf Values (Require nested path access)</h3>
 *
 * <ul>
 *   <li><strong>null values</strong>: Could represent uninstantiated complex objects
 *   <li><strong>Non-empty Maps</strong>: Have child fields that should be explicitly controlled
 *   <li><strong>Complex Objects</strong>: Configuration POJOs with multiple fields
 * </ul>
 *
 * <h2>Nested Path Processing</h2>
 *
 * <p>Nested paths use dot notation to access fields within complex objects:
 *
 * <ul>
 *   <li><strong>Syntax</strong>: {@code "parent.child.grandchild"}
 *   <li><strong>Depth</strong>: Unlimited nesting depth supported
 *   <li><strong>Creation</strong>: Automatically creates intermediate objects as needed
 *   <li><strong>Arrays</strong>: When encountered in nested paths, entire array is included
 *   <li><strong>Missing Paths</strong>: Gracefully handled with warning logs
 * </ul>
 *
 * <h2>Processing Order and Interaction</h2>
 *
 * <ol>
 *   <li><strong>Top-Level Processing</strong>: Process fields that don't contain dots
 *       <ul>
 *         <li>Include field only if it exists AND is a leaf value
 *         <li>Log debug message if field is non-leaf (explaining why it's skipped)
 *         <li>Log warning if field doesn't exist
 *       </ul>
 *   <li><strong>Nested Path Processing</strong>: Process fields with dots, BUT skip fields already
 *       processed successfully as top-level
 *       <ul>
 *         <li>Parse dot notation to traverse object hierarchy
 *         <li>Create intermediate objects as needed
 *         <li>Include arrays entirely when encountered
 *       </ul>
 * </ol>
 *
 * <h2>Usage Examples</h2>
 *
 * <h3>Basic Field Filtering</h3>
 *
 * <pre>{@code
 * // Include only safe authentication fields
 * ConfigSectionRule.include("authentication", Set.of(
 *     "enabled",           // ✅ boolean primitive
 *     "sessionTimeout"     // ✅ number primitive
 *     // "systemClientSecret" excluded - sensitive!
 * ))
 * }</pre>
 *
 * <h3>Nested Path Access</h3>
 *
 * <pre>{@code
 * // Access specific fields within complex objects
 * ConfigSectionRule.include("authentication", Set.of(
 *     "enabled",                          // ✅ top-level primitive
 *     "tokenService.signingAlgorithm",    // ✅ nested safe field
 *     "tokenService.issuer",              // ✅ nested safe field
 *     // "tokenService.signingKey" excluded - sensitive!
 *     // "tokenService" as top-level excluded - non-leaf object
 * ))
 * }</pre>
 *
 * <h3>Array Handling</h3>
 *
 * <pre>{@code
 * // Arrays are included entirely when referenced
 * ConfigSectionRule.include("authentication", Set.of(
 *     "authenticators"    // ✅ entire List<AuthenticatorConfiguration> included
 * ))
 * }</pre>
 *
 * <h2>Security Considerations</h2>
 *
 * <ul>
 *   <li><strong>Default Deny</strong>: All fields are filtered unless explicitly allowed
 *   <li><strong>Explicit Paths</strong>: Nested objects require explicit path specification
 *   <li><strong>No Pattern Matching</strong>: Prevents accidental inclusion via wildcards
 *   <li><strong>Sensitive Field Protection</strong>: Keys, passwords, secrets must be explicitly
 *       excluded from rules
 *   <li><strong>Audit Trail</strong>: Comprehensive logging for debugging and security auditing
 * </ul>
 *
 * <h2>Error Handling</h2>
 *
 * <ul>
 *   <li><strong>Missing Sections</strong>: Gracefully omitted from output
 *   <li><strong>Missing Fields</strong>: Warning logged, field omitted
 *   <li><strong>Invalid Paths</strong>: Warning logged, path omitted
 *   <li><strong>Processing Errors</strong>: Catch exceptions, log warnings, continue processing
 * </ul>
 *
 * <h2>Thread Safety</h2>
 *
 * <p>This class is <strong>thread-safe</strong> for read operations. The configuration rules are
 * immutable after construction, and filtering operations don't modify shared state.
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
   * Builds filtered configuration data from ConfigurationProvider using allowlist rules.
   *
   * <p>This is the main entry point for the allowlist filtering system. It processes all configured
   * rules and returns a filtered Map containing only explicitly allowed configuration data.
   *
   * <p><strong>Processing Flow:</strong>
   *
   * <ol>
   *   <li>Iterate through all ConfigSectionRule instances
   *   <li>For each INCLUDE rule: extract section data and apply field filtering
   *   <li>For each EXCLUDE rule: skip section entirely
   *   <li>Handle section renaming if specified
   *   <li>Return combined filtered configuration
   * </ol>
   *
   * <p><strong>Error Handling:</strong> If processing any individual section fails, that section is
   * omitted but processing continues for other sections. Errors are logged at WARN level for
   * debugging.
   *
   * @param configProvider The source ConfigurationProvider containing all configuration data
   * @return Map&lt;String, Object&gt; containing only allowed configuration sections and fields.
   *     Keys are section names (possibly renamed), values are filtered section data. Returns empty
   *     Map if no rules match or all sections are filtered out.
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
   * Applies field filtering to a configuration section according to the rule.
   *
   * <p>This method implements the core filtering logic that differentiates between leaf and
   * non-leaf values, processes both top-level and nested fields, and enforces the secure-by-default
   * model.
   *
   * <p><strong>Processing Strategy:</strong>
   *
   * <ul>
   *   <li><strong>All Fields Allowed</strong>: If rule allows all fields, return entire section as
   *       Map
   *   <li><strong>Selective Filtering</strong>: Apply two-phase filtering for granular control
   * </ul>
   *
   * <p><strong>Two-Phase Filtering Process:</strong>
   *
   * <ol>
   *   <li><strong>Phase 1 - Top-Level Fields</strong>: Process field names without dots
   *       <ul>
   *         <li>Include only if field exists AND is a leaf value (primitives, arrays, empty maps)
   *         <li>Skip non-leaf values (null, complex objects, non-empty maps)
   *         <li>Track which fields were successfully processed to avoid duplication
   *       </ul>
   *   <li><strong>Phase 2 - Nested Paths</strong>: Process field names with dots
   *       <ul>
   *         <li>Skip fields already included in Phase 1
   *         <li>Parse dot notation to traverse object hierarchy
   *         <li>Create intermediate Map structures as needed
   *         <li>Include arrays entirely when encountered in traversal
   *       </ul>
   * </ol>
   *
   * <p><strong>Examples of Field Processing:</strong>
   *
   * <pre>{@code
   * Rule: Set.of("enabled", "primary", "primary.ttlSeconds", "authenticators")
   *
   * Phase 1 (Top-Level):
   * - "enabled": true (boolean) -> ✅ INCLUDED (leaf value)
   * - "primary": {ttlSeconds: 3600, maxSize: 1000} -> ❌ SKIPPED (non-leaf object)
   * - "authenticators": [{type: "..."}, {...}] -> ✅ INCLUDED (array treated as leaf)
   *
   * Phase 2 (Nested):
   * - "primary.ttlSeconds": Creates {primary: {ttlSeconds: 3600}} -> ✅ INCLUDED
   * }</pre>
   *
   * @param sectionData The raw configuration section data (POJO or Map)
   * @param rule The ConfigSectionRule containing field filtering rules
   * @return Filtered section data as Map&lt;String, Object&gt;, or null if no fields match
   */
  private Object applyFieldFiltering(Object sectionData, ConfigSectionRule rule) {
    if (rule.isAllFieldsAllowed()) {
      // No field filtering needed - return entire section converted to Map
      return objectMapper.convertValue(sectionData, Map.class);
    }

    // Convert to Map for field-level filtering
    Map<String, Object> sectionMap = objectMapper.convertValue(sectionData, Map.class);
    Map<String, Object> filteredMap = new LinkedHashMap<>();

    // Process top-level fields (only include leaf values)
    Set<String> topLevelFields = rule.getTopLevelFields();
    Set<String> processedTopLevelFields = new HashSet<>();

    for (String fieldName : topLevelFields) {
      if (sectionMap.containsKey(fieldName)) {
        Object fieldValue = sectionMap.get(fieldName);
        if (isLeafValue(fieldValue)) {
          filteredMap.put(fieldName, fieldValue);
          processedTopLevelFields.add(fieldName);
        } else {
          log.debug(
              "Field '{}' in section '{}' is not a leaf value (has children) - skipping top-level inclusion",
              fieldName,
              rule.getSectionPath());
        }
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

      // Skip if already successfully processed as top-level field
      if (processedTopLevelFields.contains(fieldName)) {
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
    // FIXME: this is incorrect - we should still apply the filter to all array elements. a.b.c when
    // a is an array should be applied to all elements of a (ie a[0].b.c, a[1].b.c, etc)
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
   * Determines if a value is a leaf node that can be safely included via top-level field rules.
   *
   * <p>This method implements a critical security decision: only "leaf" values can be included
   * through top-level field specifications. Complex objects must be accessed via explicit nested
   * path notation to prevent accidental exposure of sensitive data.
   *
   * <p><strong>Design Rationale:</strong>
   *
   * <ul>
   *   <li><strong>Security</strong>: Prevents accidental inclusion of entire complex objects
   *   <li><strong>Explicit Access</strong>: Forces developers to explicitly specify nested paths
   *   <li><strong>Null Safety</strong>: Treats null as potentially complex to be conservative
   *   <li><strong>Array Simplicity</strong>: Includes arrays entirely to avoid complex element
   *       filtering
   * </ul>
   *
   * <p><strong>Leaf Values (✅ Can be included via top-level rules):</strong>
   *
   * <ul>
   *   <li><strong>String</strong>: "datahub", "localhost", etc.
   *   <li><strong>Number</strong>: 3600, 9092, 1.5, etc.
   *   <li><strong>Boolean</strong>: true, false
   *   <li><strong>Arrays/Lists</strong>: [1,2,3], ["a","b"], List&lt;AuthConfig&gt;
   *   <li><strong>Empty Maps</strong>: {}, new HashMap&lt;&gt;()
   * </ul>
   *
   * <p><strong>Non-Leaf Values (❌ Require nested path access):</strong>
   *
   * <ul>
   *   <li><strong>null</strong>: Could represent uninstantiated TokenServiceConfiguration
   *   <li><strong>Non-empty Maps</strong>: {"host": "localhost", "port": 9092}
   *   <li><strong>POJOs</strong>: AuthenticationConfiguration, CacheConfiguration
   * </ul>
   *
   * <p><strong>Access Examples:</strong>
   *
   * <pre>{@code
   * // ✅ VALID: Top-level access to leaf values
   * "enabled"              -> boolean (leaf)
   * "sessionTimeout"       -> number (leaf)
   * "authenticators"       -> List<AuthConfig> (leaf - array)
   *
   * // ❌ INVALID: Top-level access to complex objects
   * "tokenService"         -> TokenServiceConfiguration (non-leaf)
   * "cacheConfig"          -> CacheConfiguration (non-leaf)
   *
   * // ✅ VALID: Nested path access to complex objects
   * "tokenService.issuer"  -> accesses issuer field within TokenServiceConfiguration
   * "cache.primary.ttl"    -> accesses ttl within primary within cache
   * }</pre>
   *
   * @param value The configuration value to evaluate
   * @return true if value is a leaf that can be included via top-level rules, false if value
   *     requires nested path access
   */
  private boolean isLeafValue(Object value) {
    // null is considered non-leaf because it could represent a complex object that isn't
    // instantiated
    // If you want to include null fields, use nested paths like "field.subfield"
    if (value == null) {
      return false;
    }

    // Primitive types are leaf values
    if (value instanceof String || value instanceof Number || value instanceof Boolean) {
      return true;
    }

    // Arrays/Lists are treated as leaf values (include entirely)
    // FIXME: this is not accurate - we should not include them entirely
    if (value instanceof java.util.List || value.getClass().isArray()) {
      return true;
    }

    // Maps: only empty maps are considered leaf values
    if (value instanceof Map<?, ?> mapValue) {
      return mapValue.isEmpty();
    }

    // All other complex objects are considered non-leaf
    return false;
  }

  /**
   * Creates a default allowlist with predefined safe configuration sections for production use.
   *
   * <p>This factory method provides a security-vetted set of configuration fields that are safe to
   * expose via the /config endpoint. The default rules follow the principle of least privilege and
   * have been carefully chosen to exclude sensitive information.
   *
   * <p><strong>Included Sections and Fields:</strong>
   *
   * <ul>
   *   <li><strong>authentication</strong>: Basic settings (enabled, defaultProvider, TTL values) +
   *       safe tokenService fields
   *   <li><strong>kafka</strong>: Connection info (bootstrapServers, compression) but NOT security
   *       credentials
   *   <li><strong>springActuator</strong>: Management endpoint configuration
   *   <li><strong>cache</strong>: Basic cache settings + safe Redis connection info (host/port, NOT
   *       password)
   *   <li><strong>metadataService</strong>: Service connectivity (host/port) but NOT auth/SSL
   *       details
   * </ul>
   *
   * <p><strong>Security Exclusions (NOT included):</strong>
   *
   * <ul>
   *   <li>🔒 <strong>Authentication secrets</strong>: systemClientSecret, signingKey,
   *       refreshSigningKey
   *   <li>🔒 <strong>Database passwords</strong>: All database connection passwords
   *   <li>🔒 <strong>Kafka security</strong>: security.protocol, sasl.*, ssl.* configurations
   *   <li>🔒 <strong>Redis credentials</strong>: password, username, SSL certificates
   *   <li>🔒 <strong>SSL/TLS config</strong>: keystore paths, certificates, private keys
   *   <li>🔒 <strong>Management secrets</strong>: actuator security tokens, admin credentials
   * </ul>
   *
   * <p><strong>Nested Path Examples:</strong>
   *
   * <pre>{@code
   * // ✅ SAFE: Exposed via nested paths
   * authentication.tokenService.signingAlgorithm   -> "RS256"
   * authentication.tokenService.issuer             -> "datahub"
   * cache.redis.host                               -> "localhost"
   * cache.redis.port                               -> 6379
   *
   * // 🔒 SENSITIVE: Excluded from all rules
   * authentication.tokenService.signingKey         -> EXCLUDED
   * authentication.systemClientSecret              -> EXCLUDED
   * cache.redis.password                           -> EXCLUDED
   * }</pre>
   *
   * <p><strong>Usage Recommendations:</strong>
   *
   * <ul>
   *   <li><strong>Production Use</strong>: Use this default for production deployments
   *   <li><strong>Custom Rules</strong>: Use {@link #createCustom} for development/testing with
   *       custom rules
   *   <li><strong>Regular Review</strong>: Periodically audit these defaults as new config fields
   *       are added
   *   <li><strong>Security Testing</strong>: Verify output doesn't contain credentials before
   *       exposing
   * </ul>
   *
   * @param objectMapper ObjectMapper for JSON serialization and conversion
   * @return ConfigurationAllowlist with security-vetted default rules for production use
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
   * <p>This factory method allows complete customization of the filtering rules, useful for:
   *
   * <ul>
   *   <li><strong>Development/Testing</strong>: Custom rules for debugging and testing
   *   <li><strong>Special Deployments</strong>: Environments with different security requirements
   *   <li><strong>Limited Exposure</strong>: Highly restricted configurations for specific clients
   *   <li><strong>Extended Access</strong>: Additional safe fields not in default rules
   * </ul>
   *
   * <p><strong>⚠️ Security Warning:</strong> When creating custom rules, carefully review each
   * exposed field to ensure no sensitive information (passwords, keys, secrets) is included. Custom
   * rules bypass the security-vetted defaults.
   *
   * <p><strong>Custom Rule Examples:</strong>
   *
   * <pre>{@code
   * // Development: Include additional debugging fields
   * List<ConfigSectionRule> devRules = Arrays.asList(
   *     ConfigSectionRule.include("authentication", Set.of("enabled", "sessionTimeout")),
   *     ConfigSectionRule.include("debug", Set.of("logLevel", "metricsEnabled"))
   * );
   *
   * // Restricted: Only basic connectivity info
   * List<ConfigSectionRule> restrictedRules = Arrays.asList(
   *     ConfigSectionRule.include("metadataService", Set.of("host", "port"))
   * );
   *
   * // Extended: Include additional safe fields
   * List<ConfigSectionRule> extendedRules = Arrays.asList(
   *     ConfigSectionRule.include("authentication", Set.of(
   *         "enabled", "sessionTimeout", "tokenService.algorithm")),
   *     ConfigSectionRule.include("cache", Set.of("ttl", "maxSize"))
   * );
   * }</pre>
   *
   * <p><strong>Best Practices:</strong>
   *
   * <ul>
   *   <li><strong>Security Review</strong>: Have security team review custom rules before
   *       production
   *   <li><strong>Principle of Least Privilege</strong>: Only include fields that are absolutely
   *       needed
   *   <li><strong>Documentation</strong>: Document why each custom field is needed and safe
   *   <li><strong>Testing</strong>: Test custom rules with realistic configuration data
   *   <li><strong>Monitoring</strong>: Monitor logs for any unexpected sensitive data exposure
   * </ul>
   *
   * @param customRules List of custom ConfigSectionRule instances defining allowed fields
   * @param objectMapper ObjectMapper for JSON serialization and conversion
   * @return ConfigurationAllowlist with the specified custom rules
   */
  public static ConfigurationAllowlist createCustom(
      List<ConfigSectionRule> customRules, ObjectMapper objectMapper) {
    return new ConfigurationAllowlist(customRules, objectMapper);
  }
}
