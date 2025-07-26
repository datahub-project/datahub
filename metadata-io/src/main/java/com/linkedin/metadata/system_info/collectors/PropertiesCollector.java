package com.linkedin.metadata.system_info.collectors;

import com.linkedin.metadata.system_info.PropertyInfo;
import com.linkedin.metadata.system_info.PropertySourceInfo;
import com.linkedin.metadata.system_info.SystemPropertiesInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
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

  private static final Set<String> ALLOWED_PREFIX = Set.of("cache.client.");

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

  /**
   * Filter sensitive configuration properties This method applies filtering logic to protect
   * sensitive data
   *
   * @param properties Raw configuration properties
   * @return Filtered configuration properties
   */
  private Map<String, PropertyInfo> filter(Map<String, PropertyInfo> properties) {
    // Filtering is now applied during collection in dumpPropertiesWithMetadata()
    // This method is kept for potential future filtering enhancements
    log.debug("Filtering configuration properties");
    return properties;
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

                  // Check if this is a sensitive property
                  if (isSensitiveProperty(k)) {
                    return PropertyInfo.builder()
                        .key(k)
                        .value("***REDACTED***")
                        .source(propertySource.getName())
                        .sourceType(propertySource.getClass().getSimpleName())
                        .resolvedValue("***REDACTED***")
                        .build();
                  } else {
                    return PropertyInfo.builder()
                        .key(k)
                        .value(rawValue)
                        .source(propertySource.getName())
                        .sourceType(propertySource.getClass().getSimpleName())
                        .resolvedValue(resolvedValue)
                        .build();
                  }
                });
          }
        }
      }
    }

    return filter(sortedProperties);
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

  private boolean isSensitiveProperty(String key) {
    String lowerKey = key.toLowerCase();
    return ALLOWED_PREFIX.stream().noneMatch(lowerKey::startsWith)
        && SENSITIVE_PATTERNS.stream().anyMatch(lowerKey::endsWith);
  }
}
