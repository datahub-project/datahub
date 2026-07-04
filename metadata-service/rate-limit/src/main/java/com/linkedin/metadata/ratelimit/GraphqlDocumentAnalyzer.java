package com.linkedin.metadata.ratelimit;

import graphql.language.Field;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.parser.InvalidSyntaxException;
import graphql.parser.Parser;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.util.StringUtils;

/** Single entry point for GraphQL document metadata extraction (parse at most once). */
public final class GraphqlDocumentAnalyzer {

  private static final Parser PARSER = new Parser();

  private GraphqlDocumentAnalyzer() {}

  @Nonnull
  public static GraphqlDocumentMetadata analyze(
      @Nullable String httpOperationName, @Nullable String queryDocument) {
    return analyze(httpOperationName, queryDocument, null);
  }

  /**
   * @param skipParseWhenNamed when HTTP operation name is set and this predicate is true, skip AST
   *     parse (tier-2 fast path for known named operations).
   */
  @Nonnull
  public static GraphqlDocumentMetadata analyze(
      @Nullable String httpOperationName,
      @Nullable String queryDocument,
      @Nullable Predicate<String> skipParseWhenNamed) {
    if (StringUtils.hasText(httpOperationName)) {
      if (skipParseWhenNamed != null && skipParseWhenNamed.test(httpOperationName)) {
        return GraphqlDocumentMetadata.unparsed(
            httpOperationName, httpOperationName, queryDocument);
      }
      List<GraphqlOperationMetadata> operations = parseOperations(queryDocument);
      if (operations.isEmpty()) {
        return GraphqlDocumentMetadata.unparsed(
            httpOperationName, httpOperationName, queryDocument);
      }
      return GraphqlDocumentMetadata.parsed(
          httpOperationName, httpOperationName, operations, queryDocument);
    }

    if (!StringUtils.hasText(queryDocument)) {
      return GraphqlDocumentMetadata.anonymous(null, queryDocument);
    }

    List<GraphqlOperationMetadata> operations = parseOperations(queryDocument);
    if (operations.isEmpty()) {
      return GraphqlDocumentMetadata.anonymous(null, queryDocument);
    }

    String resolvedName =
        operations.stream()
            .map(GraphqlOperationMetadata::getName)
            .filter(StringUtils::hasText)
            .findFirst()
            .orElse(GraphqlDocumentMetadata.ANONYMOUS_OPERATION_NAME);

    return GraphqlDocumentMetadata.parsed(null, resolvedName, operations, queryDocument);
  }

  @Nonnull
  static OperationDefinition.Operation detectPrefixOperationKind(@Nullable String queryDocument) {
    if (queryDocument == null || queryDocument.isBlank()) {
      return OperationDefinition.Operation.QUERY;
    }
    String trimmed = queryDocument.stripLeading();
    if (trimmed.regionMatches(true, 0, "mutation", 0, "mutation".length())) {
      return OperationDefinition.Operation.MUTATION;
    }
    if (trimmed.regionMatches(true, 0, "subscription", 0, "subscription".length())) {
      return OperationDefinition.Operation.SUBSCRIPTION;
    }
    return OperationDefinition.Operation.QUERY;
  }

  @Nonnull
  private static List<GraphqlOperationMetadata> parseOperations(@Nullable String queryDocument) {
    if (!StringUtils.hasText(queryDocument)) {
      return List.of();
    }

    try {
      List<OperationDefinition> operationDefinitions =
          PARSER.parseDocument(queryDocument).getDefinitions().stream()
              .filter(def -> def instanceof OperationDefinition)
              .map(def -> (OperationDefinition) def)
              .toList();

      if (operationDefinitions.isEmpty()) {
        return List.of();
      }

      List<GraphqlOperationMetadata> operations = new ArrayList<>();
      for (OperationDefinition operationDefinition : operationDefinitions) {
        OperationDefinition.Operation kind = operationDefinition.getOperation();
        if (kind == null) {
          kind = OperationDefinition.Operation.QUERY;
        }
        SelectionSet selectionSet = operationDefinition.getSelectionSet();
        List<String> rootFields =
            selectionSet == null ? List.of() : collectFieldNames(selectionSet);
        operations.add(
            new GraphqlOperationMetadata(operationDefinition.getName(), kind, rootFields));
      }
      return List.copyOf(operations);
    } catch (InvalidSyntaxException e) {
      return List.of();
    }
  }

  @Nonnull
  private static List<String> collectFieldNames(@Nonnull SelectionSet selectionSet) {
    List<String> fieldNames = new ArrayList<>();
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field field) {
        if (StringUtils.hasText(field.getName())) {
          fieldNames.add(field.getName());
        }
      }
    }
    return List.copyOf(fieldNames);
  }
}
