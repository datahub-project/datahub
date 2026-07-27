package io.datahubproject.metadata.context.graphql;

import io.datahubproject.metadata.context.usage.UsageOperation;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import lombok.Getter;

/**
 * GraphQL HTTP entry-point classification: YAML overrides from {@code usage_operations.yaml} plus
 * code heuristics for common root-field patterns.
 */
public final class GraphqlUsageClassificationRegistry {

  /** Sentinel when the HTTP request has no GraphQL operation name. */
  public static final String ANONYMOUS_OPERATION_NAME =
      GraphqlHttpConstants.ANONYMOUS_OPERATION_NAME;

  @Getter private final Map<String, UsageOperation> exactNames;
  @Getter private final List<GraphqlPatternRule> patternRules;

  public GraphqlUsageClassificationRegistry(
      @Nonnull Map<String, UsageOperation> exactNames,
      @Nonnull List<GraphqlPatternRule> patternRules) {
    this.exactNames = Map.copyOf(exactNames);
    this.patternRules = List.copyOf(patternRules);
  }

  @Nonnull
  public UsageOperation resolve(
      @Nonnull String operationName, @Nullable GraphQLOperationKind operationKind) {
    return resolve(operationName, operationKind, List.of());
  }

  @Nonnull
  public UsageOperation resolve(
      @Nonnull String operationName,
      @Nullable GraphQLOperationKind operationKind,
      @Nonnull List<String> rootFieldNames) {
    if (!isAnonymousOperationName(operationName)) {
      Optional<UsageOperation> fromOperationName = resolveByOperationName(operationName);
      if (fromOperationName.isPresent()) {
        return fromOperationName.get();
      }
    }

    Optional<UsageOperation> fromRootFields = resolveByRootFields(rootFieldNames, operationKind);
    if (fromRootFields.isPresent()) {
      return fromRootFields.get();
    }

    if (operationKind == GraphQLOperationKind.MUTATION) {
      return UsageOperation.METADATA_WRITE;
    }
    return UsageOperation.METADATA_QUERY;
  }

  public boolean isAnonymousOperationName(@Nonnull String operationName) {
    return ANONYMOUS_OPERATION_NAME.equals(operationName);
  }

  /**
   * Returns yaml mapping when present for a named operation (exact or pattern); empty for anonymous
   * or unmapped names.
   */
  @Nonnull
  public Optional<UsageOperation> resolveByOperationName(@Nonnull String operationName) {
    if (isAnonymousOperationName(operationName)) {
      return Optional.empty();
    }
    UsageOperation exact = exactNames.get(operationName);
    if (exact != null) {
      return Optional.of(exact);
    }
    return matchPatterns(operationName);
  }

  /**
   * @deprecated Use {@link #resolveByOperationName(String)}.
   */
  @Deprecated
  @Nullable
  public UsageOperation resolveByOperationNameOnly(@Nonnull String operationName) {
    return resolveByOperationName(operationName).orElse(null);
  }

  @Nonnull
  private Optional<UsageOperation> resolveByRootFields(
      @Nonnull List<String> rootFieldNames, @Nullable GraphQLOperationKind operationKind) {
    if (rootFieldNames.isEmpty()) {
      return Optional.empty();
    }

    return rootFieldNames.stream()
        .map(field -> classifyRootField(field, operationKind))
        .max(Comparator.comparingInt(GraphqlUsageClassificationRegistry::expenseRank));
  }

  @Nonnull
  private UsageOperation classifyRootField(
      @Nonnull String rootField, @Nullable GraphQLOperationKind operationKind) {
    UsageOperation configured = exactNames.get(rootField);
    if (configured != null) {
      return configured;
    }
    Optional<UsageOperation> fromPattern = matchPatterns(rootField);
    if (fromPattern.isPresent()) {
      return fromPattern.get();
    }
    return classifyByHeuristic(rootField, operationKind);
  }

  @Nonnull
  private Optional<UsageOperation> matchPatterns(@Nonnull String name) {
    UsageOperation best = null;
    int bestRank = Integer.MIN_VALUE;
    for (GraphqlPatternRule rule : patternRules) {
      if (rule.pattern().matcher(name).matches()) {
        int rank = expenseRank(rule.operation());
        if (rank > bestRank) {
          bestRank = rank;
          best = rule.operation();
        }
      }
    }
    return Optional.ofNullable(best);
  }

  @Nonnull
  static UsageOperation classifyByHeuristic(
      @Nonnull String rootField, @Nullable GraphQLOperationKind operationKind) {
    if (operationKind == GraphQLOperationKind.MUTATION) {
      String lower = rootField.toLowerCase(Locale.ROOT);
      if (lower.startsWith("delete") || lower.startsWith("remove")) {
        return UsageOperation.ENTITY_DELETE;
      }
      return UsageOperation.METADATA_WRITE;
    }

    if (matchesLineageHeuristic(rootField)) {
      return UsageOperation.LINEAGE_QUERY;
    }
    if (matchesSearchHeuristic(rootField)) {
      return UsageOperation.SEARCH_QUERY;
    }
    return UsageOperation.METADATA_QUERY;
  }

  static boolean matchesSearchHeuristic(@Nonnull String rootField) {
    return rootField.startsWith("search")
        || rootField.startsWith("scroll")
        || rootField.startsWith("browse")
        || rootField.startsWith("autoComplete")
        || rootField.startsWith("semanticSearch")
        || rootField.startsWith("aggregateAcross");
  }

  static boolean matchesLineageHeuristic(@Nonnull String rootField) {
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
}
