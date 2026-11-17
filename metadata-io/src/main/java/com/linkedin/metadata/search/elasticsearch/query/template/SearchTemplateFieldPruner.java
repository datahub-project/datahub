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
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;

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

  @Nonnull
  public static QueryBuilder prune(@Nonnull QueryBuilder built, @Nonnull Set<String> removed) {
    if (removed.isEmpty()) {
      return built;
    }
    try {
      JsonNode pruned = pruneNode(SearchTemplateMappers.JSON.readTree(built.toString()), removed);
      if (pruned == null || pruned.isNull() || (pruned.isObject() && pruned.isEmpty())) {
        return QueryBuilders.matchAllQuery();
      }
      return SearchTemplateJson.parseQuery(SearchTemplateMappers.JSON.writeValueAsString(pruned));
    } catch (Exception e) {
      throw new IllegalStateException("Search template query could not be pruned", e);
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
    for (String clause : BOOL_CLAUSES) {
      JsonNode list = inner.get(clause);
      if (list == null || !list.isArray()) {
        continue;
      }
      ArrayNode kept = keptQueries(list, removed);
      if (kept.isEmpty()) {
        inner.remove(clause);
      } else {
        inner.set(clause, kept);
        any = true;
      }
    }
    return any ? query : null;
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
    if (caret > 0) {
      bare = bare.substring(0, caret);
    }
    for (String name : removed) {
      if (bare.equals(name) || bare.startsWith(name + ".")) {
        return true;
      }
    }
    return false;
  }
}
