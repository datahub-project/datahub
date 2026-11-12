package com.linkedin.metadata.search.elasticsearch.query.template;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;

/**
 * Drops a rendered clause that targets only a field the template removed. A multi-field clause
 * loses that field and is dropped when no field remains.
 */
public final class SearchTemplateFieldPruner {
  private static final Set<String> MULTI =
      Set.of("simple_query_string", "multi_match", "query_string");
  private static final Set<String> FIELD =
      Set.of(
          "term",
          "terms",
          "match",
          "match_phrase",
          "match_phrase_prefix",
          "prefix",
          "wildcard",
          "regexp",
          "fuzzy",
          "range",
          "span_term");
  private static final Set<String> FIELD_META = Set.of("boost", "_name");
  private static final List<String> BOOL_CLAUSES = List.of("must", "filter", "should", "must_not");

  private SearchTemplateFieldPruner() {}

  /**
   * Drops clauses that target a removed field. An emptied query matches nothing, the same rule as
   * {@link #pruneRendered}.
   */
  @Nonnull
  public static QueryBuilder prune(@Nonnull QueryBuilder built, @Nonnull Set<String> removed) {
    if (removed.isEmpty()) {
      return built;
    }
    try {
      JsonNode pruned = pruneNode(SearchTemplateMappers.JSON.readTree(built.toString()), removed);
      if (pruned == null || pruned.isNull() || (pruned.isObject() && pruned.isEmpty())) {
        // Same rule as pruneRendered: nothing left to match means no documents, not every document.
        return new MatchNoneQueryBuilder();
      }
      return SearchTemplateJson.parseQuery(SearchTemplateMappers.JSON.writeValueAsString(pruned));
    } catch (Exception e) {
      throw new IllegalStateException("Search template query could not be pruned", e);
    }
  }

  /**
   * Prunes a rendered template body before it is parsed, while an empty {@code fields} list is
   * still visible. Runs even when no field was removed. Returns null when no clause remains, and
   * returns the input unchanged when it is not JSON so the parser reports the error.
   */
  @Nullable
  public static String pruneRendered(@Nullable String json, @Nonnull Set<String> removed) {
    if (json == null || json.isBlank()) {
      return json;
    }
    JsonNode parsed;
    try {
      parsed = SearchTemplateMappers.JSON.readTree(json);
    } catch (Exception e) {
      return json;
    }
    JsonNode pruned = pruneNode(parsed, removed);
    if (pruned == null || pruned.isNull() || (pruned.isObject() && pruned.isEmpty())) {
      return null;
    }
    try {
      return SearchTemplateMappers.JSON.writeValueAsString(pruned);
    } catch (Exception e) {
      throw new IllegalStateException("Search template query could not be pruned", e);
    }
  }

  /**
   * {@link #pruneRendered} for a clauses body, which holds bool lists ({@code must}, {@code
   * filter}, ...) at the top level. Returns null when no list keeps a clause.
   */
  @Nullable
  public static String pruneRenderedClauses(@Nullable String json, @Nonnull Set<String> removed) {
    if (json == null || json.isBlank()) {
      return json;
    }
    JsonNode parsed;
    try {
      parsed = SearchTemplateMappers.JSON.readTree(json);
    } catch (Exception e) {
      return json;
    }
    if (!parsed.isObject()) {
      return json;
    }
    ObjectNode wrapped = SearchTemplateMappers.JSON.createObjectNode();
    wrapped.set("bool", parsed);
    JsonNode pruned = pruneNode(wrapped, removed);
    if (pruned == null || !pruned.has("bool")) {
      return null;
    }
    try {
      return SearchTemplateMappers.JSON.writeValueAsString(pruned.get("bool"));
    } catch (Exception e) {
      throw new IllegalStateException("Search template clauses could not be pruned", e);
    }
  }

  @Nullable
  private static JsonNode pruneNode(@Nullable JsonNode node, @Nonnull Set<String> removed) {
    if (node == null || node.isNull() || !node.isObject()) {
      return node;
    }
    ObjectNode object = (ObjectNode) node;
    String type = singleKey(object);
    if (type == null) {
      return object;
    }
    JsonNode body = object.get(type);
    if (!body.isObject()) {
      return object;
    }
    ObjectNode inner = (ObjectNode) body;
    if (MULTI.contains(type)) {
      return pruneMulti(object, inner, removed) ? object : null;
    }
    if (FIELD.contains(type)) {
      return pruneFieldKeys(inner, removed) ? object : null;
    }
    if ("exists".equals(type)) {
      JsonNode field = inner.get("field");
      if (field != null && field.isTextual() && targets(field.asText(), removed)) {
        return null;
      }
      return object;
    }
    if ("bool".equals(type)) {
      return pruneBool(object, inner, removed);
    }
    if ("dis_max".equals(type)) {
      return pruneQueryList(object, inner, "queries", removed);
    }
    if ("constant_score".equals(type) || "function_score".equals(type) || "boosting".equals(type)) {
      return pruneNestedQueries(object, inner, removed);
    }
    JsonNode nested = pruneNode(inner, removed);
    if (nested == null) {
      return null;
    }
    object.set(type, nested);
    return object;
  }

  private static boolean pruneMulti(
      @Nonnull ObjectNode query, @Nonnull ObjectNode inner, @Nonnull Set<String> removed) {
    JsonNode fields = inner.get("fields");
    boolean keptField = false;
    if (fields != null && fields.isArray()) {
      ArrayNode kept = SearchTemplateMappers.JSON.createArrayNode();
      for (JsonNode field : fields) {
        if (field.isTextual() && !targets(field.asText(), removed)) {
          kept.add(field);
        }
      }
      if (!kept.isEmpty()) {
        inner.set("fields", kept);
        keptField = true;
      } else {
        inner.remove("fields");
      }
    }
    JsonNode defaultField = inner.get("default_field");
    if (defaultField != null && defaultField.isTextual()) {
      if (targets(defaultField.asText(), removed)) {
        inner.remove("default_field");
      } else {
        keptField = true;
      }
    }
    if (fields == null && defaultField == null) {
      return true;
    }
    if (!keptField) {
      query.removeAll();
    }
    return keptField;
  }

  private static boolean pruneFieldKeys(@Nonnull ObjectNode inner, @Nonnull Set<String> removed) {
    List<String> fields = new ArrayList<>();
    Iterator<String> names = inner.fieldNames();
    while (names.hasNext()) {
      String name = names.next();
      if (!FIELD_META.contains(name)) {
        fields.add(name);
      }
    }
    if (fields.isEmpty()) {
      return true;
    }
    boolean kept = false;
    for (String field : fields) {
      if (targets(field, removed)) {
        inner.remove(field);
      } else {
        kept = true;
      }
    }
    return kept;
  }

  @Nullable
  private static JsonNode pruneBool(
      @Nonnull ObjectNode query, @Nonnull ObjectNode inner, @Nonnull Set<String> removed) {
    boolean any = false;
    boolean shouldEmptied = false;
    for (String clause : BOOL_CLAUSES) {
      JsonNode value = inner.get(clause);
      // OpenSearch accepts a bool clause as one object or as a list. An object is one clause.
      ArrayNode list = clauseList(value);
      if (list == null) {
        continue;
      }
      ArrayNode kept = keptQueries(list, removed);
      if (kept.isEmpty()) {
        inner.remove(clause);
        shouldEmptied |= "should".equals(clause) && !list.isEmpty();
      } else {
        inner.set(clause, kept);
        any = true;
      }
    }
    // OpenSearch caps minimum_should_match at the number of should clauses, so a required should
    // that lost every clause would otherwise turn into a filter that matches everything.
    if (shouldEmptied && requiresShould(inner.get("minimum_should_match"))) {
      return null;
    }
    return any ? query : null;
  }

  private static boolean requiresShould(@Nullable JsonNode minimumShouldMatch) {
    if (minimumShouldMatch == null || minimumShouldMatch.isNull()) {
      return false;
    }
    if (minimumShouldMatch.isNumber()) {
      return minimumShouldMatch.asInt() > 0;
    }
    String spec = minimumShouldMatch.asText().trim();
    return !spec.isEmpty() && !"0".equals(spec) && !"0%".equals(spec);
  }

  @Nullable
  private static JsonNode pruneQueryList(
      @Nonnull ObjectNode query,
      @Nonnull ObjectNode inner,
      @Nonnull String key,
      @Nonnull Set<String> removed) {
    JsonNode list = inner.get(key);
    if (list == null || !list.isArray()) {
      return query;
    }
    ArrayNode kept = keptQueries(list, removed);
    if (kept.isEmpty()) {
      return null;
    }
    inner.set(key, kept);
    return query;
  }

  @Nullable
  private static JsonNode pruneNestedQueries(
      @Nonnull ObjectNode query, @Nonnull ObjectNode inner, @Nonnull Set<String> removed) {
    for (String key : List.of("filter", "query", "positive", "negative")) {
      if (!inner.has(key)) {
        continue;
      }
      JsonNode pruned = pruneNode(inner.get(key), removed);
      if (pruned == null) {
        return null;
      }
      inner.set(key, pruned);
    }
    return query;
  }

  /** A bool clause written as one object, or the array it already is. Anything else is ignored. */
  @Nullable
  private static ArrayNode clauseList(@Nullable JsonNode value) {
    if (value == null || value.isNull()) {
      return null;
    }
    if (value.isArray()) {
      return (ArrayNode) value;
    }
    if (value.isObject()) {
      return SearchTemplateMappers.JSON.createArrayNode().add(value);
    }
    return null;
  }

  @Nonnull
  private static ArrayNode keptQueries(@Nonnull JsonNode list, @Nonnull Set<String> removed) {
    ArrayNode kept = SearchTemplateMappers.JSON.createArrayNode();
    for (JsonNode child : list) {
      JsonNode pruned = pruneNode(child, removed);
      if (pruned != null && !pruned.isNull()) {
        kept.add(pruned);
      }
    }
    return kept;
  }

  @Nullable
  private static String singleKey(@Nonnull ObjectNode object) {
    Iterator<String> names = object.fieldNames();
    if (!names.hasNext()) {
      return null;
    }
    String key = names.next();
    return names.hasNext() ? null : key;
  }

  private static boolean targets(@Nonnull String field, @Nonnull Set<String> removed) {
    String bare = field;
    int caret = bare.lastIndexOf('^');
    if (caret >= 0) {
      bare = bare.substring(0, caret);
    }
    // A blank name comes from an unset template variable. OpenSearch would read it as no field
    // or every field, so the clause is dropped.
    if (bare.isBlank()) {
      return true;
    }
    for (String name : removed) {
      if (bare.equals(name) || bare.startsWith(name + ".")) {
        return true;
      }
    }
    return false;
  }
}
