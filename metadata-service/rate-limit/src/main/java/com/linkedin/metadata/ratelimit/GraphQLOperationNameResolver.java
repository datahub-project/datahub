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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.springframework.util.StringUtils;

/**
 * Derives the rate-limit view of a GraphQL request from the query document. The whole rate-limit
 * path needs three things off the same AST — the display/operation name, the rate-limit identity,
 * and the top-level resolver field names — so {@link #analyze} parses the document exactly once and
 * returns all three. Callers must not re-parse for these individually.
 */
public final class GraphQLOperationNameResolver {

  private static final Parser PARSER = new Parser();

  /** Identity used when a query has no operation name and no usable top-level fields. */
  static final String DEFAULT_IDENTITY = "graphql";

  private GraphQLOperationNameResolver() {}

  /**
   * Resolves just the operation name (named op, else {@code "graphql"}). Cheap no-op when {@code
   * jsonOperationName} is already provided/resolved (no parse). Used for the display/query name;
   * the rate-limit path should use {@link #analyze} instead of calling this separately.
   */
  @Nonnull
  public static String resolve(@Nullable String jsonOperationName, @Nullable String queryDocument) {
    if (StringUtils.hasText(jsonOperationName)) {
      return jsonOperationName;
    }
    if (!StringUtils.hasText(queryDocument)) {
      return "graphql";
    }
    try {
      return operations(PARSER.parseDocument(queryDocument)).stream()
          .map(OperationDefinition::getName)
          .filter(StringUtils::hasText)
          .findFirst()
          .orElse("graphql");
    } catch (InvalidSyntaxException e) {
      return "graphql";
    }
  }

  /** The rate-limit view of a request, all produced from a single parse of the query. */
  public record RateLimitQuery(
      @Nonnull String queryName, @Nonnull String identity, @Nonnull List<String> topLevelFields) {}

  /**
   * Parses the query <b>once</b> and returns everything the rate-limit path needs:
   *
   * <ul>
   *   <li>{@code queryName} — the operation name for display/context (named op, else {@code
   *       "graphql"}).
   *   <li>{@code identity} — the rate-limit identity: the operation name for a named op, or for an
   *       unnamed op the sorted top-level field names (e.g. {@code "search,user"}), else {@code
   *       "graphql"}. The operation name is client-controlled (the client writes both the query and
   *       the JSON {@code operationName}), so name-based identity is advisory, not a security
   *       boundary; only the unnamed-op fallback (top-level field names) is schema-derived. See the
   *       threat-model note in docs/deploy/gms-rate-limiting.md.
   *   <li>{@code topLevelFields} — the operation's top-level field names (aliases collapsed,
   *       top-level inline fragments and fragment spreads resolved), used by the heavy-resolver
   *       gate.
   * </ul>
   *
   * Never throws — any parse failure falls back to name/identity {@code "graphql"} and no fields.
   */
  @Nonnull
  public static RateLimitQuery analyze(
      @Nullable String jsonOperationName, @Nullable String queryDocument) {
    String jsonName = StringUtils.hasText(jsonOperationName) ? jsonOperationName : null;
    if (!StringUtils.hasText(queryDocument)) {
      return fallback(jsonName);
    }
    try {
      Document document = PARSER.parseDocument(queryDocument);
      List<OperationDefinition> operations = operations(document);
      if (operations.isEmpty()) {
        return fallback(jsonName);
      }
      String docName =
          operations.stream()
              .map(OperationDefinition::getName)
              .filter(StringUtils::hasText)
              .findFirst()
              .orElse(null);
      String effectiveName = jsonName != null ? jsonName : docName;
      List<String> topLevelFields =
          topLevelFieldList(selectOperation(operations, jsonName), fragmentDefinitions(document));

      String queryName = effectiveName != null ? effectiveName : "graphql";
      String identity = effectiveName != null ? effectiveName : identityFromFields(topLevelFields);
      return new RateLimitQuery(queryName, identity, topLevelFields);
    } catch (InvalidSyntaxException e) {
      return fallback(jsonName);
    }
  }

  @Nonnull
  private static RateLimitQuery fallback(@Nullable String jsonName) {
    String queryName = jsonName != null ? jsonName : "graphql";
    String identity = jsonName != null ? jsonName : DEFAULT_IDENTITY;
    return new RateLimitQuery(queryName, identity, List.of());
  }

  /** Sorted, comma-joined top-level field names (stable regardless of client field order). */
  @Nonnull
  private static String identityFromFields(@Nonnull List<String> topLevelFields) {
    if (topLevelFields.isEmpty()) {
      return DEFAULT_IDENTITY;
    }
    List<String> sorted = new ArrayList<>(topLevelFields);
    sorted.sort(null);
    return String.join(",", sorted);
  }

  @Nonnull
  private static List<OperationDefinition> operations(@Nonnull Document document) {
    return document.getDefinitions().stream()
        .filter(def -> def instanceof OperationDefinition)
        .map(def -> (OperationDefinition) def)
        .toList();
  }

  @Nonnull
  private static Map<String, FragmentDefinition> fragmentDefinitions(@Nonnull Document document) {
    Map<String, FragmentDefinition> fragments = new HashMap<>();
    for (Definition<?> def : document.getDefinitions()) {
      if (def instanceof FragmentDefinition) {
        FragmentDefinition fragment = (FragmentDefinition) def;
        fragments.put(fragment.getName(), fragment);
      }
    }
    return fragments;
  }

  @Nonnull
  private static OperationDefinition selectOperation(
      @Nonnull List<OperationDefinition> operations, @Nullable String operationName) {
    if (StringUtils.hasText(operationName)) {
      for (OperationDefinition op : operations) {
        if (operationName.equals(op.getName())) {
          return op;
        }
      }
    }
    return operations.get(0);
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
    collectTopLevelFields(selectionSet, fragments, new HashSet<>(), fieldNames);
    return fieldNames;
  }

  private static void collectTopLevelFields(
      @Nonnull SelectionSet selectionSet,
      @Nonnull Map<String, FragmentDefinition> fragments,
      @Nonnull Set<String> visitedFragments,
      @Nonnull List<String> out) {
    for (Selection<?> selection : selectionSet.getSelections()) {
      if (selection instanceof Field) {
        out.add(((Field) selection).getName());
      } else if (selection instanceof InlineFragment) {
        SelectionSet inner = ((InlineFragment) selection).getSelectionSet();
        if (inner != null) {
          collectTopLevelFields(inner, fragments, visitedFragments, out);
        }
      } else if (selection instanceof FragmentSpread) {
        String name = ((FragmentSpread) selection).getName();
        // add() returns false when already present → breaks fragment reference cycles.
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
