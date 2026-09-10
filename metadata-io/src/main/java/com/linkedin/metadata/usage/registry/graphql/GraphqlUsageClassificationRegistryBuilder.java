package com.linkedin.metadata.usage.registry.graphql;

import com.linkedin.metadata.config.usage.graphql.GraphqlClassificationEntries;
import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import com.linkedin.metadata.config.usage.overlay.UsageConfigurationOverlay;
import io.datahubproject.metadata.context.graphql.GraphqlPatternRule;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nonnull;

public final class GraphqlUsageClassificationRegistryBuilder {

  private GraphqlUsageClassificationRegistryBuilder() {}

  @Nonnull
  public static GraphqlUsageClassificationRegistry fromManifest(
      @Nonnull UsageOperationsManifest manifest) {
    return fromManifest(manifest, UsageConfigurationOverlay.empty());
  }

  @Nonnull
  public static GraphqlUsageClassificationRegistry fromManifest(
      @Nonnull UsageOperationsManifest manifest, @Nonnull UsageConfigurationOverlay overlay) {
    Map<String, UsageOperation> exactNames = new HashMap<>();
    List<GraphqlPatternRule> patternRules = new ArrayList<>();

    for (Map.Entry<String, UsageOperationsManifest.UsageOperationDefinition> entry :
        manifest.getUsageOperations().entrySet()) {
      registerGraphqlClassification(
          entry.getKey(), entry.getValue().getGraphql(), exactNames, patternRules);
    }

    applyGraphqlClassificationOverrides(
        manifest, overlay.getGraphqlClassificationOverrides(), exactNames, patternRules);

    return new GraphqlUsageClassificationRegistry(
        Map.copyOf(exactNames), List.copyOf(patternRules));
  }

  private static void applyGraphqlClassificationOverrides(
      @Nonnull UsageOperationsManifest manifest,
      @Nonnull Map<String, UsageOperationsManifest.GraphqlClassification> overrides,
      @Nonnull Map<String, UsageOperation> exactNames,
      @Nonnull List<GraphqlPatternRule> patternRules) {
    if (overrides == null || overrides.isEmpty()) {
      return;
    }
    for (Map.Entry<String, UsageOperationsManifest.GraphqlClassification> entry :
        overrides.entrySet()) {
      String usageOperationKey = entry.getKey();
      if (!manifest.getUsageOperations().containsKey(usageOperationKey)) {
        throw new IllegalStateException(
            "graphql_classification_overrides key not in usage_operations.yaml: "
                + usageOperationKey);
      }
      registerGraphqlClassification(usageOperationKey, entry.getValue(), exactNames, patternRules);
    }
  }

  private static void registerGraphqlClassification(
      @Nonnull String usageOperationKey,
      @Nonnull UsageOperationsManifest.GraphqlClassification graphql,
      @Nonnull Map<String, UsageOperation> exactNames,
      @Nonnull List<GraphqlPatternRule> patternRules) {
    if (graphql == null) {
      return;
    }
    UsageOperation usageOperation = UsageOperation.fromKey(usageOperationKey);
    registerExactNames(
        exactNames, usageOperation, GraphqlClassificationEntries.resolvedNames(graphql));
    registerPatterns(
        usageOperationKey,
        usageOperation,
        patternRules,
        GraphqlClassificationEntries.resolvedPatterns(graphql));
  }

  private static void registerExactNames(
      @Nonnull Map<String, UsageOperation> exactNames,
      @Nonnull UsageOperation usageOperation,
      @Nonnull List<String> names) {
    for (String name : names) {
      UsageOperation previous = exactNames.putIfAbsent(name, usageOperation);
      if (previous != null && previous != usageOperation) {
        throw new IllegalStateException(
            "Duplicate GraphQL name "
                + name
                + " mapped to "
                + previous.key()
                + " and "
                + usageOperation.key());
      }
    }
  }

  private static void registerPatterns(
      @Nonnull String usageOperationKey,
      @Nonnull UsageOperation usageOperation,
      @Nonnull List<GraphqlPatternRule> patternRules,
      @Nonnull List<String> patterns) {
    for (String patternString : patterns) {
      try {
        patternRules.add(new GraphqlPatternRule(usageOperation, Pattern.compile(patternString)));
      } catch (PatternSyntaxException e) {
        throw new IllegalStateException(
            "Invalid GraphQL classification pattern for "
                + usageOperationKey
                + ": "
                + patternString,
            e);
      }
    }
  }
}
