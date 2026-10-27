package com.linkedin.metadata.config.usage.cigate;

import com.linkedin.metadata.config.usage.UsageYamlMapper;
import com.linkedin.metadata.config.usage.loader.UsageMetricRegistryLoader;
import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricIncrementResolver;
import com.linkedin.metadata.usage.registry.metrics.UsageMetricRegistry;
import com.linkedin.metadata.usage.registry.operations.ActivityClass;
import com.linkedin.metadata.usage.registry.operations.UsageOperationsRegistry;
import io.datahubproject.metadata.context.RequestContext;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

public final class UsageMetricEmitPathValidator {

  private final UsageOperationsRegistry operationsRegistry;
  private final UsageMetricRegistry metricRegistry;

  UsageMetricEmitPathValidator(
      @Nonnull UsageOperationsRegistry operationsRegistry,
      @Nonnull UsageMetricRegistry metricRegistry) {
    this.operationsRegistry = operationsRegistry;
    this.metricRegistry = metricRegistry;
  }

  @Nonnull
  public static UsageMetricEmitPathValidator fromBundled(@Nonnull UsageOperationsLoader opsLoader) {
    UsageMetricRegistryLoader metricLoader =
        new UsageMetricRegistryLoader(UsageYamlMapper.create());
    return new UsageMetricEmitPathValidator(
        UsageOperationsRegistry.loadOssOnly(opsLoader),
        UsageMetricRegistry.loadBundled(metricLoader, List.of()));
  }

  @Nonnull
  public List<String> validateGraphqlClassifiedOperations(
      @Nonnull UsageOperationsManifest manifest) {
    List<String> failures = new ArrayList<>();
    manifest
        .getUsageOperations()
        .forEach(
            (key, def) -> {
              if (def.getGraphql() == null) {
                return;
              }
              failures.addAll(
                  validateOperationForApi(
                      UsageOperation.fromKey(key), RequestContext.RequestAPI.GRAPHQL));
            });
    return failures;
  }

  @Nonnull
  private List<String> validateOperationForApi(
      @Nonnull UsageOperation operation, @Nonnull RequestContext.RequestAPI requestApi) {
    List<String> failures = new ArrayList<>();
    UsageOperationsRegistry.UsageOperationEntry entry = operationsRegistry.require(operation);
    if (!operationsRegistry.isAllowedForApi(entry, requestApi)) {
      failures.add(
          operation.key()
              + " is not allowed for request_api "
              + requestApi
              + " (check usage_operations.yaml request_apis)");
    }
    if (!hasSupportedMetrics(entry.activityClass())) {
      failures.add(
          operation.key()
              + " activity_class "
              + entry.activityClass()
              + " has no supported metrics in usage_metric_registry.yaml");
    }
    return failures;
  }

  private boolean hasSupportedMetrics(@Nonnull ActivityClass activityClass) {
    boolean hasAlways =
        metricRegistry.apiUsageMetrics().values().stream()
            .anyMatch(UsageMetricIncrementResolver::isSupported);
    if (!hasAlways) {
      return false;
    }
    return switch (activityClass) {
      case READ, WRITE, OPERATION -> true;
    };
  }
}
