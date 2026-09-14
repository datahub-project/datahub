package com.linkedin.metadata.config.usage.graphql;

import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import com.linkedin.metadata.config.usage.overlay.UsageConfigurationOverlay;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

/** Extracts GraphQL classification entries from usage manifests (configuration-only). */
public final class UsageGraphqlManifestCollector {

  private UsageGraphqlManifestCollector() {}

  @Nonnull
  public static Set<String> names(@Nonnull UsageOperationsManifest manifest) {
    Set<String> names = new HashSet<>();
    if (manifest.getUsageOperations() == null) {
      return names;
    }
    for (UsageOperationsManifest.UsageOperationDefinition definition :
        manifest.getUsageOperations().values()) {
      addAll(names, GraphqlClassificationEntries.resolvedNames(definition.getGraphql()));
    }
    return Collections.unmodifiableSet(names);
  }

  @Nonnull
  public static Set<String> overlayGraphqlNames(@Nonnull UsageConfigurationOverlay overlay) {
    Set<String> names = new HashSet<>();
    Map<String, UsageOperationsManifest.GraphqlClassification> overrides =
        overlay.getGraphqlClassificationOverrides();
    if (overrides == null) {
      return Collections.unmodifiableSet(names);
    }
    for (UsageOperationsManifest.GraphqlClassification graphql : overrides.values()) {
      if (graphql == null) {
        continue;
      }
      addAll(names, GraphqlClassificationEntries.resolvedNames(graphql));
    }
    return Collections.unmodifiableSet(names);
  }

  /**
   * Ensures GraphQL names in {@code usage_operations.yaml} do not also appear in a deployment
   * overlay's {@code graphql_classification_overrides}.
   */
  public static void validateGraphqlNamesDisjoint(
      @Nonnull UsageOperationsManifest usageManifest, @Nonnull UsageConfigurationOverlay overlay) {
    Set<String> overlap = new HashSet<>(names(usageManifest));
    overlap.retainAll(overlayGraphqlNames(overlay));
    if (!overlap.isEmpty()) {
      throw new IllegalStateException(
          "GraphQL names must not appear in both usage_operations.yaml and"
              + " graphql_classification_overrides: "
              + overlap);
    }
  }

  private static void addAll(@Nonnull Set<String> target, List<String> values) {
    if (values != null) {
      target.addAll(values);
    }
  }
}
