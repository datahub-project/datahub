package com.linkedin.metadata.system_info.collectors;

import com.linkedin.metadata.system_info.PropertyInfo;
import com.linkedin.metadata.system_info.PropertySourceInfo;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.EnumerablePropertySource;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class PropertiesCollector {

  private final Environment springEnvironment;

  // Common patterns for sensitive properties
  private static final Set<String> SENSITIVE_PATTERNS =
      Set.of(
          "password",
          "passwd",
          "pwd",
          "pass",
          "secret",
          "key",
          "salt",
          "token",
          "credential",
          "private",
          "auth",
          "api-key",
          "apikey",
          "access-key",
          "accesskey",
          "private-key",
          "privatekey");

  private static final Set<Pattern> ALLOWED_PATTERNS =
      compilePatterns(
          Set.of(
              "cache\\.client\\..*", // Allow all cache.client.* properties
              ".*\\.(delay|interval|timeout|duration|initial|max|wait).*ms$", // Allow specific ms
              // properties
              ".*\\.limit$", // Allow properties ending with .limit
              ".*\\.max$", // Allow properties ending with .max
              ".*\\.\\w*size$" // Allow properties ending with .pageSize .batchSize
              ));

  /**
   * Compile string patterns into Pattern objects for efficient regex matching
   *
   * @param patterns Set of regex pattern strings
   * @return Set of compiled Pattern objects
   */
  private static Set<Pattern> compilePatterns(Set<String> patterns) {
    return patterns.stream().map(Pattern::compile).collect(Collectors.toSet());
  }

  /**
   * Collect all system properties with metadata
   *
   * @return SystemPropertiesInfo containing properties with metadata and source information
   */
  public SystemPropertiesInfo collect() {
    Map<String, PropertyInfo> properties = dumpPropertiesWithMetadata();

    return SystemPropertiesInfo.builder()
        .properties(properties)
        .propertySources(getPropertySources())
        .totalProperties(properties.size())
        .redactedProperties(
            (int)
                properties.values().stream()
                    .filter(p -> p.getValue().equals("***REDACTED***"))
                    .count())
        .build();
  }

  /**
   * Get properties as a simple map (for backward compatibility)
   *
   * @return Map of property keys to resolved values
   */
  public Map<String, Object> getPropertiesAsMap() {
    return dumpPropertiesWithMetadata().entrySet().stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                e ->
                    e.getValue().getResolvedValue() != null
                        ? e.getValue().getResolvedValue()
                        : e.getValue().getValue(),
                (oldValue, newValue) -> oldValue,
                () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));
  }

  private Map<String, PropertyInfo> dumpPropertiesWithMetadata() {
    // Use TreeMap to keep properties sorted by key
    Map<String, PropertyInfo> sortedProperties = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    if (springEnvironment instanceof ConfigurableEnvironment) {
      ConfigurableEnvironment configEnv = (ConfigurableEnvironment) springEnvironment;

      for (PropertySource<?> propertySource : configEnv.getPropertySources()) {
        if (propertySource instanceof EnumerablePropertySource) {
          EnumerablePropertySource<?> enumerable = (EnumerablePropertySource<?>) propertySource;

          for (String key : enumerable.getPropertyNames()) {
            // TreeMap will keep the first occurrence (highest priority source)
            sortedProperties.computeIfAbsent(
                key,
                k -> {
                  Object rawValue = enumerable.getProperty(k);
                  String resolvedValue = springEnvironment.getProperty(k);

                  // Check if this is an allowed property
                  if (isAllowedProperty(k, resolvedValue)) {
                    Object sanitizedRaw = scrubUrlCredentials(rawValue);
                    String sanitizedResolved =
                        resolvedValue == null ? null : (String) scrubUrlCredentials(resolvedValue);
                    return PropertyInfo.builder()
                        .key(k)
                        .value(sanitizedRaw)
                        .source(propertySource.getName())
                        .sourceType(propertySource.getClass().getSimpleName())
                        .resolvedValue(sanitizedResolved)
                        .build();
                  } else {
                    return PropertyInfo.builder()
                        .key(k)
                        .value("***REDACTED***")
                        .source(propertySource.getName())
                        .sourceType(propertySource.getClass().getSimpleName())
                        .resolvedValue("***REDACTED***")
                        .build();
                  }
                });
          }
        }
      }
    }

    return sortedProperties;
  }

  private List<PropertySourceInfo> getPropertySources() {
    List<PropertySourceInfo> sources = new ArrayList<>();

    if (springEnvironment instanceof ConfigurableEnvironment) {
      ConfigurableEnvironment configEnv = (ConfigurableEnvironment) springEnvironment;

      for (PropertySource<?> propertySource : configEnv.getPropertySources()) {
        int propertyCount = 0;
        if (propertySource instanceof EnumerablePropertySource) {
          propertyCount = ((EnumerablePropertySource<?>) propertySource).getPropertyNames().length;
        }

        sources.add(
            PropertySourceInfo.builder()
                .name(propertySource.getName())
                .type(propertySource.getClass().getSimpleName())
                .propertyCount(propertyCount)
                .build());
      }
    }

    return sources;
  }

  private boolean isAllowedProperty(String key, String value) {
    String lowerKey = key.toLowerCase();
    // Check if value is a boolean
    boolean isBooleanValue =
        value != null && (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false"));

    return isBooleanValue
        || ALLOWED_PATTERNS.stream().anyMatch(pattern -> pattern.matcher(lowerKey).find())
        || SENSITIVE_PATTERNS.stream().noneMatch(lowerKey::endsWith);
  }

  /**
   * Strip embedded credentials from JDBC/HTTP URLs while keeping host/path visible in system-info.
   * Non-URL values are returned unchanged.
   */
  @Nullable
  static Object scrubUrlCredentials(@Nullable Object value) {
    if (!(value instanceof String original) || original.isBlank()) {
      return value;
    }
    String working = original.trim();
    String jdbcPrefix = "";
    if (working.regionMatches(true, 0, "jdbc:", 0, 5)) {
      jdbcPrefix = working.substring(0, 5);
      working = working.substring(5);
    }
    if (!working.contains("://")) {
      return original;
    }
    try {
      URI uri = new URI(working);
      String query = uri.getRawQuery();
      String sanitizedQuery = scrubCredentialQueryParams(query);
      URI rebuilt =
          new URI(
              uri.getScheme(),
              null, // drop userInfo
              uri.getHost(),
              uri.getPort(),
              uri.getPath(),
              sanitizedQuery,
              uri.getFragment());
      return jdbcPrefix + rebuilt;
    } catch (URISyntaxException e) {
      // Best-effort fallback for non-RFC JDBC forms.
      String stripped = working.replaceFirst("://[^/@\\s]+@", "://");
      stripped = scrubCredentialQueryParamsInPlace(stripped);
      return jdbcPrefix + stripped;
    }
  }

  @Nullable
  private static String scrubCredentialQueryParams(@Nullable String query) {
    if (query == null || query.isBlank()) {
      return query;
    }
    List<String> kept = new ArrayList<>();
    for (String part : query.split("&")) {
      int eq = part.indexOf('=');
      String name = eq >= 0 ? part.substring(0, eq) : part;
      String lower = name.toLowerCase(Locale.ROOT);
      if (lower.equals("user")
          || lower.equals("username")
          || lower.equals("password")
          || lower.equals("pwd")
          || lower.equals("pass")) {
        continue;
      }
      kept.add(part);
    }
    return kept.isEmpty() ? null : String.join("&", kept);
  }

  private static String scrubCredentialQueryParamsInPlace(String url) {
    int q = url.indexOf('?');
    if (q < 0) {
      return url;
    }
    String base = url.substring(0, q);
    String sanitized = scrubCredentialQueryParams(url.substring(q + 1));
    return sanitized == null ? base : base + "?" + sanitized;
  }
}
