package com.linkedin.metadata.ratelimit;

import graphql.language.Definition;
import graphql.language.Document;
import graphql.language.Field;
import graphql.language.FragmentDefinition;
import graphql.language.FragmentSpread;
import graphql.language.InlineFragment;
import graphql.language.OperationDefinition;
import graphql.language.Selection;
import graphql.language.SelectionSet;
import graphql.parser.InvalidSyntaxException;
import graphql.parser.Parser;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.util.StringUtils;

/** Single entry point for GraphQL document metadata extraction (parse at most once). */
public final class GraphqlDocumentAnalyzer {

  private static final Parser PARSER = new Parser();
  private static final Map<String, FragmentDefinition> NO_FRAGMENTS = Map.of();

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
      Document document = PARSER.parseDocument(queryDocument);
      Map<String, FragmentDefinition> fragments = fragmentDefinitions(document);
      List<OperationDefinition> operationDefinitions =
          document.getDefinitions().stream()
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
        List<String> rootFields = topLevelFieldList(operationDefinition, fragments);
        operations.add(
            new GraphqlOperationMetadata(operationDefinition.getName(), kind, rootFields));
      }
      return List.copyOf(operations);
    } catch (InvalidSyntaxException e) {
      return List.of();
    }
  }

  @Nonnull
  private static Map<String, FragmentDefinition> fragmentDefinitions(@Nonnull Document document) {
    Map<String, FragmentDefinition> fragments = null;
    for (Definition<?> definition : document.getDefinitions()) {
      if (definition instanceof FragmentDefinition fragmentDefinition) {
        if (fragments == null) {
          fragments = new HashMap<>();
        }
        fragments.put(fragmentDefinition.getName(), fragmentDefinition);
      }
    }
    return fragments != null ? fragments : NO_FRAGMENTS;
  }

  /**
   * Direct top-level field names of an operation; aliases collapsed. Top-level inline fragments and
   * fragment spreads are descended so the fields they contribute at the root are counted as
   * top-level; fragment reference cycles are guarded.
   */
  @Nonnull
  private static List<String> topLevelFieldList(
      @Nonnull OperationDefinition operation, @Nonnull Map<String, FragmentDefinition> fragments) {
    SelectionSet selectionSet = operation.getSelectionSet();
    if (selectionSet == null || selectionSet.getSelections().isEmpty()) {
      return List.of();
    }
    List<String> fieldNames = new ArrayList<>();
    Set<String> visitedFragments = fragments.isEmpty() ? null : new HashSet<>();
    collectTopLevelFields(selectionSet, fragments, visitedFragments, fieldNames);
    return List.copyOf(fieldNames);
  }

  private static void collectTopLevelFields(
      @Nonnull SelectionSet selectionSet,
      @Nonnull Map<String, FragmentDefinition> fragments,
      @Nullable Set<String> visitedFragments,
      @Nonnull List<String> out) {
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field field) {
        if (StringUtils.hasText(field.getName())) {
          out.add(field.getName());
        }
      } else if (selection instanceof InlineFragment inlineFragment) {
        SelectionSet inner = inlineFragment.getSelectionSet();
        if (inner != null) {
          collectTopLevelFields(inner, fragments, visitedFragments, out);
        }
      } else if (selection instanceof FragmentSpread fragmentSpread) {
        if (visitedFragments == null) {
          continue;
        }
        String name = fragmentSpread.getName();
        if (visitedFragments.add(name)) {
          FragmentDefinition definition = fragments.get(name);
          if (definition != null && definition.getSelectionSet() != null) {
            collectTopLevelFields(definition.getSelectionSet(), fragments, visitedFragments, out);
          }
        }
      }
    }
  }
}
