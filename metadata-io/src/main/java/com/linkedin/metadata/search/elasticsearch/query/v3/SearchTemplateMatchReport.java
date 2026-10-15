package com.linkedin.metadata.search.elasticsearch.query.v3;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.metadata.search.MatchedField;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.opensearch.index.query.QueryBuilder;

/**
 * Reports a structured-property match from a query {@code _name}. The name is {@code exact}, {@code
 * prefix}, {@code contains}, or {@code nameAndValue}, and it is reported only when that name is on
 * exactly one clause and that clause targets exactly one structured-property field.
 */
public final class SearchTemplateMatchReport {
  public static final String EXACT = "exact";
  public static final String PREFIX = "prefix";
  public static final String CONTAINS = "contains";
  public static final String NAME_AND_VALUE = "nameAndValue";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final Set<String> MODES = Set.of(EXACT, PREFIX, CONTAINS, NAME_AND_VALUE);
  private static final Set<String> META =
      Set.of(
          "boost",
          "_name",
          "value",
          "query",
          "case_insensitive",
          "fields",
          "field",
          "operator",
          "analyzer",
          "minimum_should_match",
          "tie_breaker",
          "queries",
          "must",
          "should",
          "filter",
          "must_not",
          "functions",
          "score_mode",
          "boost_mode",
          "slop",
          "fuzziness",
          "prefix_length",
          "max_expansions",
          "rewrite",
          "flags",
          "lenient",
          "zero_terms_query",
          "type",
          "default_operator",
          "default_field",
          "adjust_pure_negative",
          "positive",
          "negative",
          "negative_boost");

  private SearchTemplateMatchReport() {}

  /**
   * Records unique named clauses on this query. A reused name or a multi-property clause is
   * omitted.
   */
  @Nonnull
  public static Map<String, RenderedSearchTemplate.NamedClause> namedClauses(
      @Nonnull QueryBuilder query) {
    Map<String, List<Set<String>>> found = new LinkedHashMap<>();
    try {
      walk(MAPPER.readTree(query.toString()), null, found);
    } catch (Exception e) {
      throw new IllegalStateException("Search template query names could not be read", e);
    }
    Map<String, RenderedSearchTemplate.NamedClause> unique = new LinkedHashMap<>();
    for (Map.Entry<String, List<Set<String>>> entry : found.entrySet()) {
      if (entry.getValue().size() != 1) {
        continue;
      }
      Set<String> fields = entry.getValue().get(0);
      if (fields.size() != 1) {
        continue;
      }
      String path = fields.iterator().next();
      String urn = StructuredPropertyQueryFields.definitionUrn(path).orElse(null);
      if (urn == null) {
        continue;
      }
      String mode = entry.getKey();
      unique.put(
          mode,
          new RenderedSearchTemplate.NamedClause(
              urn, path, PREFIX.equals(mode) || CONTAINS.equals(mode)));
    }
    return Map.copyOf(unique);
  }

  @Nonnull
  public static List<MatchedField> fields(
      @Nonnull Map<String, Set<String>> matched,
      @Nullable Map<String, Object> source,
      @Nonnull String query,
      @Nonnull Map<String, RenderedSearchTemplate.NamedClause> named) {
    Map<String, Set<String>> remaining = new LinkedHashMap<>(matched);
    List<MatchedField> reported = new ArrayList<>();
    Set<String> covered = new LinkedHashSet<>();
    Set<String> consumed = new LinkedHashSet<>();
    for (Map.Entry<String, Set<String>> entry : remaining.entrySet()) {
      RenderedSearchTemplate.NamedClause clause = named.get(entry.getKey());
      if (clause == null) {
        continue;
      }
      for (String value : entry.getValue()) {
        String found = value == null || value.isBlank() ? null : value;
        if (found == null && source != null) {
          found = firstMatch(sourceValue(source, clause.path()), query, clause.analyzed());
        }
        MatchedField field =
            new MatchedField().setName(clause.urn()).setValue(found == null ? "" : found);
        field.setMode(entry.getKey());
        reported.add(field);
      }
      consumed.add(entry.getKey());
      covered.add(clause.path());
    }
    remaining.keySet().removeAll(consumed);
    remaining.keySet().removeIf(name -> MODES.contains(name) || covers(name, covered));
    for (Map.Entry<String, Set<String>> entry : remaining.entrySet()) {
      for (String value : entry.getValue()) {
        reported.add(new MatchedField().setName(entry.getKey()).setValue(value));
      }
    }
    return reported;
  }

  private static void walk(
      @Nullable JsonNode node,
      @Nullable String parentKey,
      @Nonnull Map<String, List<Set<String>>> found) {
    if (node == null || node.isNull()) {
      return;
    }
    if (node.isArray()) {
      node.forEach(child -> walk(child, parentKey, found));
      return;
    }
    if (!node.isObject()) {
      return;
    }
    JsonNode name = node.get("_name");
    if (name != null && name.isTextual() && MODES.contains(name.asText())) {
      Set<String> properties = new LinkedHashSet<>();
      addProperty(parentKey, properties);
      collect(node, properties, true);
      found.computeIfAbsent(name.asText(), key -> new ArrayList<>()).add(properties);
    }
    Iterator<Map.Entry<String, JsonNode>> entries = node.fields();
    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      walk(entry.getValue(), entry.getKey(), found);
    }
  }

  private static void collect(
      @Nonnull JsonNode node, @Nonnull Set<String> properties, boolean root) {
    if (node.isArray()) {
      node.forEach(child -> collect(child, properties, false));
      return;
    }
    if (!node.isObject()) {
      return;
    }
    if (!root) {
      JsonNode name = node.get("_name");
      if (name != null && name.isTextual() && MODES.contains(name.asText())) {
        return;
      }
    }
    JsonNode fields = node.get("fields");
    if (fields != null && fields.isArray()) {
      fields.forEach(field -> addProperty(field.asText(), properties));
    }
    JsonNode field = node.get("field");
    if (field != null && field.isTextual()) {
      addProperty(field.asText(), properties);
    }
    Iterator<Map.Entry<String, JsonNode>> entries = node.fields();
    while (entries.hasNext()) {
      Map.Entry<String, JsonNode> entry = entries.next();
      if (!META.contains(entry.getKey())) {
        addProperty(entry.getKey(), properties);
      }
      collect(entry.getValue(), properties, false);
    }
  }

  private static void addProperty(@Nullable String raw, @Nonnull Set<String> properties) {
    if (raw == null || raw.isBlank()) {
      return;
    }
    String bare = raw;
    int caret = bare.lastIndexOf('^');
    if (caret > 0) {
      bare = bare.substring(0, caret);
    }
    String base = bare;
    if (base.endsWith(".keyword")) {
      base = base.substring(0, base.length() - ".keyword".length());
    } else if (base.endsWith(".text")) {
      base = base.substring(0, base.length() - ".text".length());
    } else if (base.endsWith(".stemmed")) {
      base = base.substring(0, base.length() - ".stemmed".length());
    }
    if (base.startsWith("structuredProperties.")) {
      properties.add(base);
    }
  }

  private static boolean covers(@Nonnull String name, @Nonnull Set<String> paths) {
    for (String path : paths) {
      if (name.equals(path) || name.startsWith(path + ".")) {
        return true;
      }
    }
    return false;
  }

  @Nullable
  private static Object sourceValue(@Nonnull Map<String, Object> source, @Nonnull String field) {
    if (source.containsKey(field)) {
      return source.get(field);
    }
    Object current = source;
    for (String part : field.split("\\.", -1)) {
      if (!(current instanceof Map<?, ?> map) || !map.containsKey(part)) {
        return null;
      }
      current = map.get(part);
    }
    return current;
  }

  @Nullable
  private static String firstMatch(@Nullable Object raw, @Nonnull String query, boolean analyzed) {
    if (raw instanceof Iterable<?> values) {
      for (Object value : values) {
        String found = firstMatch(value, query, analyzed);
        if (found != null) {
          return found;
        }
      }
      return null;
    }
    if (raw instanceof Map<?, ?> map) {
      for (Object value : map.values()) {
        String found = firstMatch(value, query, analyzed);
        if (found != null) {
          return found;
        }
      }
      return null;
    }
    if (raw == null) {
      return null;
    }
    String text = String.valueOf(raw);
    String left = text.trim().toLowerCase(Locale.ROOT);
    String right = query.trim().toLowerCase(Locale.ROOT);
    if (right.isEmpty()) {
      return null;
    }
    return (analyzed ? left.contains(right) : left.equals(right)) ? text : null;
  }
}
