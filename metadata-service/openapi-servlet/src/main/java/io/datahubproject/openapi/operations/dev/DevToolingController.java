package io.datahubproject.openapi.operations.dev;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Development tooling endpoints for agent-driven workflows. Provides runtime access to feature
 * flags (warm toggles) and configuration inspection.
 *
 * <p>Gated by devTooling.enabled=true (set via DEV_TOOLING_ENABLED env var). Enabled by default in
 * all debug/quickstartDebug profiles.
 */
@RestController
@RequestMapping("/dev")
@ConditionalOnProperty(name = "devTooling.enabled", havingValue = "true")
@Slf4j
@Tag(
    name = "DevTooling",
    description = "Development tooling for agent-driven workflows (feature flags, config)")
public class DevToolingController {

  private final ConfigurationProvider configProvider;
  private final FeatureFlags featureFlags;

  public DevToolingController(ConfigurationProvider configProvider) {
    this.configProvider = configProvider;
    this.featureFlags = configProvider.getFeatureFlags();
  }

  @GetMapping(path = "/featureFlags", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get all feature flags and their current values")
  public ResponseEntity<Map<String, Object>> getFeatureFlags() {
    Map<String, Object> flags = serializeFeatureFlags();
    return ResponseEntity.ok(flags);
  }

  @GetMapping(path = "/featureFlags/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get a specific feature flag value")
  public ResponseEntity<Map<String, Object>> getFeatureFlag(@PathVariable("name") String name) {
    Map<String, Object> flags = serializeFeatureFlags();
    if (!flags.containsKey(name)) {
      return ResponseEntity.badRequest()
          .body(
              Map.of(
                  "error",
                  "Unknown flag: " + name,
                  "available",
                  String.join(", ", flags.keySet())));
    }
    return ResponseEntity.ok(Map.of(name, flags.get(name)));
  }

  @PutMapping(
      path = "/featureFlags/{name}",
      consumes = MediaType.APPLICATION_JSON_VALUE,
      produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(
      summary = "Set a feature flag at runtime (warm toggle, no restart needed)",
      description =
          "Mutates the in-memory FeatureFlags singleton. Change is transient and will be lost on"
              + " restart.")
  public ResponseEntity<Map<String, Object>> setFeatureFlag(
      @PathVariable("name") String name, @RequestBody Map<String, Object> body) {

    Object value = body.get("value");
    if (value == null) {
      return ResponseEntity.badRequest().body(Map.of("error", "Request body must contain 'value'"));
    }

    try {
      Field field = FeatureFlags.class.getDeclaredField(name);
      field.setAccessible(true);

      Class<?> fieldType = field.getType();
      Object typedValue = coerceValue(value, fieldType);

      // Use the Lombok-generated setter
      String setterName = "set" + Character.toUpperCase(name.charAt(0)) + name.substring(1);
      Method setter = FeatureFlags.class.getMethod(setterName, fieldType);
      setter.invoke(featureFlags, typedValue);

      log.warn(
          "Dev tooling: flag '{}' set to {} (transient override, lost on restart)",
          name,
          typedValue);

      return ResponseEntity.ok(Map.of(name, typedValue, "transient", true));

    } catch (NoSuchFieldException e) {
      Map<String, Object> flags = serializeFeatureFlags();
      return ResponseEntity.badRequest()
          .body(
              Map.of(
                  "error",
                  "Unknown flag: " + name,
                  "available",
                  String.join(", ", flags.keySet())));
    } catch (Exception e) {
      log.error("Failed to set feature flag '{}'", name, e);
      return ResponseEntity.internalServerError()
          .body(Map.of("error", "Failed to set flag: " + e.getMessage()));
    }
  }

  private Map<String, Object> serializeFeatureFlags() {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Field field : FeatureFlags.class.getDeclaredFields()) {
      field.setAccessible(true);
      try {
        result.put(field.getName(), field.get(featureFlags));
      } catch (IllegalAccessException e) {
        result.put(field.getName(), "ERROR: " + e.getMessage());
      }
    }
    return result;
  }

  private Object coerceValue(Object value, Class<?> targetType) {
    if (targetType == boolean.class || targetType == Boolean.class) {
      if (value instanceof Boolean) {
        return value;
      }
      String strVal = value.toString().toLowerCase();
      return "true".equals(strVal) || "1".equals(strVal) || "yes".equals(strVal);
    }
    if (targetType == String.class) {
      return value.toString();
    }
    if (targetType == int.class || targetType == Integer.class) {
      return Integer.parseInt(value.toString());
    }
    if (targetType == long.class || targetType == Long.class) {
      return Long.parseLong(value.toString());
    }
    return value;
  }
}
