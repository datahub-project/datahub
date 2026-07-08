package com.linkedin.metadata.usage.registry.operations;

import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import com.linkedin.metadata.config.usage.overlay.UsageConfigurationOverlay;
import com.linkedin.metadata.config.usage.overlay.UsageOperationCostOverride;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nonnull;
import lombok.Getter;

@Getter
public class UsageOperationsRegistry {

  private final Map<String, UsageOperationEntry> operations;

  public UsageOperationsRegistry(
      @Nonnull UsageOperationsManifest usageManifest, @Nonnull UsageConfigurationOverlay overlay) {
    Map<String, UsageOperationEntry> built = new HashMap<>();
    Map<String, UsageOperationsManifest.UsageOperationDefinition> usageOperations =
        usageManifest.getUsageOperations();
    usageOperations.forEach(
        (key, def) -> {
          int costUnits = def.getDefaultCostUnits();
          if (overlay.getUsageOperationOverrides() != null
              && overlay.getUsageOperationOverrides().containsKey(key)) {
            UsageOperationCostOverride override = overlay.getUsageOperationOverrides().get(key);
            if (override.getDefaultCostUnits() != null) {
              costUnits = override.getDefaultCostUnits();
            }
          }
          Set<RequestContext.RequestAPI> apis =
              def.getRequestApis() == null
                  ? Set.of()
                  : def.getRequestApis().stream()
                      .map(UsageOperationsRegistry::parseRequestApi)
                      .collect(Collectors.toSet());
          built.put(
              key,
              new UsageOperationEntry(
                  UsageOperation.fromKey(key),
                  ActivityClass.fromYaml(def.getActivityClass()),
                  def.isIngestionEndpoint(),
                  costUnits,
                  apis));
        });
    if (overlay.getUsageOperationOverrides() != null) {
      for (String overrideKey : overlay.getUsageOperationOverrides().keySet()) {
        if (!usageOperations.containsKey(overrideKey)) {
          throw new IllegalStateException(
              "usage_operation_overrides key not in usage_operations.yaml: " + overrideKey);
        }
      }
    }
    this.operations = Collections.unmodifiableMap(built);
  }

  @Nonnull
  public static UsageOperationsRegistry loadBundled(
      @Nonnull UsageOperationsLoader usageLoader, @Nonnull UsageConfigurationOverlay overlay) {
    return new UsageOperationsRegistry(usageLoader.loadBundled(), overlay);
  }

  @Nonnull
  public static UsageOperationsRegistry loadOssOnly(@Nonnull UsageOperationsLoader usageLoader) {
    return new UsageOperationsRegistry(
        usageLoader.loadBundled(), UsageConfigurationOverlay.empty());
  }

  @Nonnull
  public UsageOperationEntry require(@Nonnull String key) {
    UsageOperationEntry entry = operations.get(key);
    if (entry == null) {
      throw new IllegalArgumentException("Unknown usage_operation: " + key);
    }
    return entry;
  }

  @Nonnull
  public UsageOperationEntry require(@Nonnull UsageOperation operation) {
    return require(operation.key());
  }

  public boolean isAllowedForApi(
      @Nonnull UsageOperationEntry entry, @Nonnull RequestContext.RequestAPI requestApi) {
    if (entry.requestApis().isEmpty()) {
      return true;
    }
    return entry.requestApis().contains(requestApi);
  }

  private static RequestContext.RequestAPI parseRequestApi(String raw) {
    return switch (raw.toLowerCase()) {
      case "graphql" -> RequestContext.RequestAPI.GRAPHQL;
      case "openapi" -> RequestContext.RequestAPI.OPENAPI;
      case "restli" -> RequestContext.RequestAPI.RESTLI;
      case "messaging" -> RequestContext.RequestAPI.MESSAGING;
      default -> throw new IllegalArgumentException("Unknown request_api in manifest: " + raw);
    };
  }

  public record UsageOperationEntry(
      UsageOperation operation,
      ActivityClass activityClass,
      boolean ingestionEndpoint,
      int defaultCostUnits,
      Set<RequestContext.RequestAPI> requestApis) {

    /**
     * Whether this operation should add the actor to {@code active_writers}. Only catalog writes
     * with {@code default_cost_units > 0} count; zero-cost {@code other_write} remains in {@code
     * active_users} only.
     */
    public boolean contributesToActiveWriters() {
      return activityClass == ActivityClass.WRITE && defaultCostUnits > 0;
    }
  }
}
