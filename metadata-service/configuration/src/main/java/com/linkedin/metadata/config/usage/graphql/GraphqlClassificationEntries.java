package com.linkedin.metadata.config.usage.graphql;

import com.linkedin.metadata.config.usage.manifest.UsageOperationsManifest;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/** Resolves unified {@code names} / {@code patterns} from graphql classification yaml. */
public final class GraphqlClassificationEntries {

  private GraphqlClassificationEntries() {}

  @Nonnull
  public static List<String> resolvedNames(
      @Nullable UsageOperationsManifest.GraphqlClassification graphql) {
    if (graphql == null) {
      return List.of();
    }
    if (graphql.getNames() != null) {
      return List.copyOf(graphql.getNames());
    }
    Set<String> union = new LinkedHashSet<>();
    if (graphql.getOperationNames() != null) {
      union.addAll(graphql.getOperationNames());
    }
    if (graphql.getRootFields() != null) {
      union.addAll(graphql.getRootFields());
    }
    return List.copyOf(union);
  }

  @Nonnull
  public static List<String> resolvedPatterns(
      @Nullable UsageOperationsManifest.GraphqlClassification graphql) {
    if (graphql == null || graphql.getPatterns() == null) {
      return List.of();
    }
    return List.copyOf(graphql.getPatterns());
  }
}
