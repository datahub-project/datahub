package com.linkedin.metadata.search.elasticsearch.query.request.v3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.search.elasticsearch.query.template.SearchTemplateMappers;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

/**
 * Fields a rendered query actually reads. Matched fields use these roots and the stored values
 * already fetched for them. A {@code copy_to} sink such as {@code other} has no stored value, so a
 * hit that matches only there returns no matched field.
 */
public final class RenderedQueryFields {
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

  private RenderedQueryFields() {}

  /** Shared-field roots referenced by {@code query}, in encounter order. */
  @Nonnull
  public static List<String> roots(@Nullable QueryBuilder query) {
    if (query == null) {
      return List.of();
    }
    LinkedHashSet<String> roots = new LinkedHashSet<>();
    collectRoots(read(query), roots);
    return List.copyOf(roots);
  }

  private static void collectRoots(@Nullable JsonNode node, @Nonnull Set<String> roots) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.isArray()) {
      node.forEach(child -> collectRoots(child, roots));
      return;
    }
    if (!node.isObject()) {
      return;
    }
    ObjectNode object = (ObjectNode) node;
    String key = singleKey(object);
    if (key != null && MULTI.contains(key) && object.get(key).isObject()) {
      addMultiRoots((ObjectNode) object.get(key), roots);
    } else if (key != null && FIELD.contains(key) && object.get(key).isObject()) {
      addFieldRoots((ObjectNode) object.get(key), roots);
    }
    object.fields().forEachRemaining(entry -> collectRoots(entry.getValue(), roots));
  }

  private static void addMultiRoots(@Nonnull ObjectNode body, @Nonnull Set<String> roots) {
    JsonNode fields = body.get("fields");
    if (fields != null && fields.isArray()) {
      fields.forEach(
          field -> {
            if (field.isTextual()) {
              addRoot(field.asText(), roots);
            }
          });
    }
    JsonNode defaultField = body.get("default_field");
    if (defaultField != null && defaultField.isTextual()) {
      addRoot(defaultField.asText(), roots);
    }
  }

  private static void addFieldRoots(@Nonnull ObjectNode body, @Nonnull Set<String> roots) {
    Iterator<Map.Entry<String, JsonNode>> fields = body.fields();
    while (fields.hasNext()) {
      Map.Entry<String, JsonNode> field = fields.next();
      if (!FIELD_META.contains(field.getKey())) {
        addRoot(field.getKey(), roots);
      }
    }
  }

  private static void addRoot(@Nonnull String field, @Nonnull Set<String> roots) {
    String root = SearchHighlightSelection.indexedRoot(withoutBoosts(field));
    if (root != null) {
      roots.add(root);
    }
  }

  /**
   * A rendered field can carry more than one {@code ^boost}: the template writes one and query
   * serialization appends the clause weight.
   */
  @Nonnull
  private static String withoutBoosts(@Nonnull String field) {
    String stripped = field;
    int boost;
    while ((boost = stripped.lastIndexOf('^')) > 0 && numeric(stripped.substring(boost + 1))) {
      stripped = stripped.substring(0, boost);
    }
    return stripped;
  }

  private static boolean numeric(@Nonnull String value) {
    if (value.isEmpty()) {
      return false;
    }
    for (int i = 0; i < value.length(); i++) {
      char character = value.charAt(i);
      if ((character < '0' || character > '9') && character != '.') {
        return false;
      }
    }
    return true;
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

  @Nonnull
  private static JsonNode read(@Nonnull QueryBuilder query) {
    try {
      return SearchTemplateMappers.JSON.readTree(query.toString());
    } catch (Exception e) {
      throw new IllegalStateException("Rendered query fields could not be read", e);
    }
  }
}
