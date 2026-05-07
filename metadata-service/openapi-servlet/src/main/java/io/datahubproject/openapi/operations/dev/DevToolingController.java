package io.datahubproject.openapi.operations.dev;

import com.linkedin.datahub.graphql.featureflags.FeatureFlags;
import com.linkedin.gms.factory.config.ConfigurationProvider;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import java.lang.reflect.Field;
import java.lang.reflect.InaccessibleObjectException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Development tooling endpoints for agent-driven workflows. Provides read-only inspection of
 * feature flags and configuration.
 *
 * <p>Gated by devTooling.enabled=true (set via DEV_TOOLING_ENABLED env var). Enabled by default in
 * all debug/quickstartDebug profiles.
 */
@RestController
@RequestMapping("/openapi/operations/dev")
@ConditionalOnProperty(name = "devTooling.enabled", havingValue = "true")
@Tag(
    name = "DevTooling",
    description = "Development tooling for agent-driven workflows (feature flag inspection)")
public class DevToolingController {

  private static final Logger log = LoggerFactory.getLogger(DevToolingController.class);

  private final FeatureFlags featureFlags;

  public DevToolingController(ConfigurationProvider configProvider) {
    FeatureFlags flags = configProvider.getFeatureFlags();
    if (flags == null) {
      throw new IllegalStateException(
          "ConfigurationProvider.getFeatureFlags() returned null — check Spring configuration");
    }
    this.featureFlags = flags;
  }

  @GetMapping(path = "/featureFlags", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get all feature flags and their current runtime values")
  public ResponseEntity<Map<String, Object>> getFeatureFlags() {
    return ResponseEntity.ok(serializeFeatureFlags());
  }

  @GetMapping(path = "/featureFlags/{name}", produces = MediaType.APPLICATION_JSON_VALUE)
  @Operation(summary = "Get a specific feature flag value")
  public ResponseEntity<Map<String, Object>> getFeatureFlag(@PathVariable("name") String name) {
    Map<String, Object> flags = serializeFeatureFlags();
    if (!flags.containsKey(name)) {
      return ResponseEntity.status(HttpStatus.NOT_FOUND)
          .body(
              Map.of(
                  "error",
                  "Unknown flag: " + name,
                  "available",
                  String.join(", ", flags.keySet())));
    }
    return ResponseEntity.ok(Map.of(name, flags.get(name)));
  }

  private Map<String, Object> serializeFeatureFlags() {
    Map<String, Object> result = new LinkedHashMap<>();
    for (Field field : FeatureFlags.class.getDeclaredFields()) {
      if (field.getType() != boolean.class) continue;
      try {
        field.setAccessible(true);
        result.put(field.getName(), field.get(featureFlags));
      } catch (IllegalAccessException e) {
        log.error("Cannot read feature flag field: {}", field.getName(), e);
        result.put(field.getName(), "ERROR: " + e.getMessage());
      } catch (InaccessibleObjectException e) {
        log.error("Module access blocked for field: {}", field.getName(), e);
        result.put(field.getName(), "ERROR: module access blocked");
      }
    }
    return result;
  }
}
