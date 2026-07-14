package com.linkedin.metadata.config.usage.cigate.graphql;

import com.linkedin.metadata.config.usage.loader.UsageOperationsLoader;
import com.linkedin.metadata.usage.registry.graphql.GraphqlUsageClassificationRegistryBuilder;
import io.datahubproject.metadata.context.graphql.GraphQLOperationKind;
import io.datahubproject.metadata.context.graphql.GraphqlPatternRule;
import io.datahubproject.metadata.context.graphql.GraphqlUsageClassificationRegistry;
import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public final class GraphqlUsageCoverageClassifier {

  private final GraphqlUsageClassificationRegistry registry;

  GraphqlUsageCoverageClassifier(@Nonnull GraphqlUsageClassificationRegistry registry) {
    this.registry = registry;
  }

  /** Builds a classifier from OSS {@code usage_operations.yaml}. */
  @Nonnull
  public static GraphqlUsageCoverageClassifier fromBundledYaml(
      @Nonnull UsageOperationsLoader loader) {
    return new GraphqlUsageCoverageClassifier(
        GraphqlUsageClassificationRegistryBuilder.fromManifest(loader.loadBundled()));
  }

  @Nonnull
  public GraphqlClassificationResult classify(
      @Nonnull GraphqlUsageSurface.GraphqlSurfaceEntry entry) {
    return switch (entry.kind()) {
      case QUERY_ROOT_FIELD -> classifyRootField(entry.name(), GraphQLOperationKind.QUERY);
      case MUTATION_ROOT_FIELD -> classifyRootField(entry.name(), GraphQLOperationKind.MUTATION);
      case CLIENT_OPERATION -> classifyClientOperation(entry.name());
    };
  }

  @Nonnull
  public List<String> formatUnaccountedFailures(
      @Nonnull List<GraphqlUsageSurface.GraphqlSurfaceEntry> entries,
      @Nonnull GraphqlExemptionSnapshot exemptions) {
    List<String> failures = new ArrayList<>();
    for (GraphqlUsageSurface.GraphqlSurfaceEntry entry : entries) {
      GraphqlClassificationResult result = classify(entry);
      if (!result.isAccountedFor() && !exemptions.isExempt(entry)) {
        failures.add(
            entry.kind()
                + " '"
                + entry.name()
                + "' resolves to "
                + result.operation().key()
                + " via "
                + result.source()
                + " — add graphql.names or graphql.patterns under the appropriate"
                + " usage_operations entry, or add an entry to graphql_usage_exemptions.snapshot.yaml");
      }
    }
    return failures;
  }

  @Nonnull
  public List<String> formatUnaccountedFailures(
      @Nonnull List<GraphqlUsageSurface.GraphqlSurfaceEntry> entries) {
    return formatUnaccountedFailures(entries, GraphqlExemptionSnapshot.empty());
  }

  @Nonnull
  private GraphqlClassificationResult classifyRootField(
      @Nonnull String rootField, @Nullable GraphQLOperationKind operationKind) {
    UsageOperation exact = registry.getExactNames().get(rootField);
    if (exact != null) {
      return new GraphqlClassificationResult(exact, GraphqlClassificationSource.EXPLICIT_NAME);
    }
    Optional<GraphqlClassificationResult> fromPattern = matchPatternsWithSource(rootField);
    if (fromPattern.isPresent()) {
      return fromPattern.get();
    }
    return classifyByHeuristicWithSource(rootField, operationKind);
  }

  @Nonnull
  private GraphqlClassificationResult classifyClientOperation(@Nonnull String operationName) {
    GraphQLOperationKind kind = inferOperationKind(operationName);
    if (!registry.isAnonymousOperationName(operationName)) {
      Optional<GraphqlClassificationResult> fromOperationName =
          resolveByOperationNameWithSource(operationName);
      if (fromOperationName.isPresent()) {
        return fromOperationName.get();
      }
    }
    List<String> inferredRootFields = inferRootFields(operationName);
    if (!inferredRootFields.isEmpty()) {
      Optional<GraphqlClassificationResult> fromRootFields =
          resolveByRootFieldsWithSource(inferredRootFields, kind);
      if (fromRootFields.isPresent()) {
        return fromRootFields.get();
      }
    }
    return classifyByHeuristicWithSource(operationName, kind);
  }

  @Nonnull
  private GraphqlClassificationResult resolveWithSource(
      @Nonnull String operationName,
      @Nullable GraphQLOperationKind operationKind,
      @Nonnull List<String> rootFieldNames) {
    if (!registry.isAnonymousOperationName(operationName)) {
      Optional<GraphqlClassificationResult> fromOperationName =
          resolveByOperationNameWithSource(operationName);
      if (fromOperationName.isPresent()) {
        return fromOperationName.get();
      }
    }

    Optional<GraphqlClassificationResult> fromRootFields =
        resolveByRootFieldsWithSource(rootFieldNames, operationKind);
    if (fromRootFields.isPresent()) {
      return fromRootFields.get();
    }

    if (operationKind == GraphQLOperationKind.MUTATION) {
      return new GraphqlClassificationResult(
          UsageOperation.METADATA_WRITE, GraphqlClassificationSource.DEFAULT_WRITE);
    }
    return new GraphqlClassificationResult(
        UsageOperation.METADATA_QUERY, GraphqlClassificationSource.DEFAULT_QUERY);
  }

  @Nonnull
  private Optional<GraphqlClassificationResult> resolveByOperationNameWithSource(
      @Nonnull String operationName) {
    if (registry.isAnonymousOperationName(operationName)) {
      return Optional.empty();
    }
    UsageOperation exact = registry.getExactNames().get(operationName);
    if (exact != null) {
      return Optional.of(
          new GraphqlClassificationResult(exact, GraphqlClassificationSource.EXPLICIT_NAME));
    }
    return matchPatternsWithSource(operationName);
  }

  @Nonnull
  private Optional<GraphqlClassificationResult> resolveByRootFieldsWithSource(
      @Nonnull List<String> rootFieldNames, @Nullable GraphQLOperationKind operationKind) {
    if (rootFieldNames.isEmpty()) {
      return Optional.empty();
    }
    return rootFieldNames.stream()
        .map(field -> classifyRootField(field, operationKind))
        .max(Comparator.comparingInt(result -> expenseRank(result.operation())));
  }

  @Nonnull
  private Optional<GraphqlClassificationResult> matchPatternsWithSource(@Nonnull String name) {
    UsageOperation best = null;
    int bestRank = Integer.MIN_VALUE;
    for (GraphqlPatternRule rule : registry.getPatternRules()) {
      if (rule.pattern().matcher(name).matches()) {
        int rank = expenseRank(rule.operation());
        if (rank > bestRank) {
          bestRank = rank;
          best = rule.operation();
        }
      }
    }
    if (best == null) {
      return Optional.empty();
    }
    return Optional.of(new GraphqlClassificationResult(best, GraphqlClassificationSource.PATTERN));
  }

  @Nonnull
  private static GraphqlClassificationResult classifyByHeuristicWithSource(
      @Nonnull String rootField, @Nullable GraphQLOperationKind operationKind) {
    if (operationKind == GraphQLOperationKind.MUTATION) {
      String lower = rootField.toLowerCase(Locale.ROOT);
      if (lower.startsWith("delete") || lower.startsWith("remove")) {
        return new GraphqlClassificationResult(
            UsageOperation.ENTITY_DELETE, GraphqlClassificationSource.HEURISTIC);
      }
      return new GraphqlClassificationResult(
          UsageOperation.METADATA_WRITE, GraphqlClassificationSource.DEFAULT_WRITE);
    }

    if (matchesLineageHeuristic(rootField)) {
      return new GraphqlClassificationResult(
          UsageOperation.LINEAGE_QUERY, GraphqlClassificationSource.HEURISTIC);
    }
    if (matchesSearchHeuristic(rootField)) {
      return new GraphqlClassificationResult(
          UsageOperation.SEARCH_QUERY, GraphqlClassificationSource.HEURISTIC);
    }
    if (matchesEntityReadDefault(rootField)) {
      return new GraphqlClassificationResult(
          UsageOperation.METADATA_QUERY, GraphqlClassificationSource.ENTITY_READ_DEFAULT);
    }
    return new GraphqlClassificationResult(
        UsageOperation.METADATA_QUERY, GraphqlClassificationSource.DEFAULT_QUERY);
  }

  private static boolean matchesEntityReadDefault(@Nonnull String rootField) {
    return rootField.matches("^[a-z][a-zA-Z0-9]*$");
  }

  private static boolean matchesSearchHeuristic(@Nonnull String rootField) {
    return rootField.startsWith("search")
        || rootField.startsWith("scroll")
        || rootField.startsWith("browse")
        || rootField.startsWith("autoComplete")
        || rootField.startsWith("semanticSearch")
        || rootField.startsWith("aggregateAcross");
  }

  private static boolean matchesLineageHeuristic(@Nonnull String rootField) {
    return rootField.contains("AcrossLineage") || rootField.endsWith("Lineage");
  }

  private static int expenseRank(@Nonnull UsageOperation operation) {
    return switch (operation) {
      case ENTITY_DELETE, ASPECT_DELETE -> 8;
      case METADATA_WRITE -> 7;
      case SEARCH_QUERY -> 6;
      case LINEAGE_QUERY -> 5;
      case OTHER_OPERATIONS -> 3;
      case OTHER_WRITE -> 2;
      case OTHER_READ -> 1;
      case METADATA_QUERY, METADATA_READ, METADATA_INGEST, MCP_QUERY -> 0;
    };
  }

  @Nonnull
  private static GraphQLOperationKind inferOperationKind(@Nonnull String operationName) {
    String lower = operationName.toLowerCase(Locale.ROOT);
    if (lower.startsWith("create")
        || lower.startsWith("update")
        || lower.startsWith("delete")
        || lower.startsWith("remove")
        || lower.startsWith("add")
        || lower.startsWith("set")
        || lower.startsWith("unset")
        || lower.startsWith("link")
        || lower.startsWith("unlink")
        || lower.startsWith("move")
        || lower.startsWith("accept")
        || lower.startsWith("cancel")
        || lower.startsWith("raise")
        || lower.startsWith("rollback")
        || lower.startsWith("import")
        || lower.startsWith("submit")
        || lower.startsWith("verify")
        || lower.startsWith("batch")
        || lower.startsWith("upsert")
        || lower.startsWith("patch")
        || lower.startsWith("revoke")
        || lower.startsWith("dismiss")) {
      return GraphQLOperationKind.MUTATION;
    }
    return GraphQLOperationKind.QUERY;
  }

  @Nonnull
  private static List<String> inferRootFields(@Nonnull String operationName) {
    if (operationName.startsWith("get") && operationName.length() > 3) {
      String candidate =
          Character.toLowerCase(operationName.charAt(3)) + operationName.substring(4);
      return List.of(candidate);
    }
    if (operationName.startsWith("list") && operationName.length() > 4) {
      String candidate =
          Character.toLowerCase(operationName.charAt(4)) + operationName.substring(5);
      return List.of(candidate);
    }
    return List.of();
  }
}
