package com.linkedin.metadata.search.elasticsearch.update;

import static com.linkedin.metadata.Constants.INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.metadata.search.elasticsearch.index.entity.v3.MappingConstants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Search-document fields that a timeseries aspect must only move forward. The entity index keeps
 * whichever document was written last, but events for one entity can be written out of order, so
 * these fields are written by a script that compares them with the indexed values instead.
 *
 * <p>The ordered fields are replaced together, and only when the incoming order field is at least
 * the indexed one. Each max field is replaced only when the incoming value is greater.
 */
public final class OrderedSearchFields {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  private static final TypeReference<Map<String, Object>> MAP_TYPE = new TypeReference<>() {};

  static final String SCRIPT =
      "boolean newer = params.orderValue != null"
          + " && (ctx._source[params.orderField] == null"
          + " || ((Number) params.orderValue).longValue()"
          + " >= ((Number) ctx._source[params.orderField]).longValue());"
          + "if (newer) {"
          + "  for (e in params.ordered.entrySet()) { ctx._source[e.getKey()] = e.getValue(); }"
          + "  if (params.aspect != null) {"
          + "    if (ctx._source[params.aspectsField] == null) {"
          + "      ctx._source[params.aspectsField] = [:];"
          + "    }"
          + "    ctx._source[params.aspectsField][params.aspectName] = params.aspect;"
          + "  }"
          + "}"
          + "for (e in params.max.entrySet()) {"
          + "  def current = ctx._source[e.getKey()];"
          + "  if (e.getValue() != null && (current == null"
          + "      || ((Number) e.getValue()).longValue() > ((Number) current).longValue())) {"
          + "    ctx._source[e.getKey()] = e.getValue();"
          + "  }"
          + "}";

  private record Rule(
      @Nonnull String orderField,
      @Nonnull List<String> orderedFields,
      @Nonnull List<String> maxFields) {}

  // A run that finishes after a newer run has started must not replace the newer run's status,
  // but its outcome still counts towards the last successful or failed run.
  private static final Map<String, Rule> RULES =
      Map.of(
          INGESTION_SOURCE_RUN_EVENT_ASPECT_NAME,
          new Rule(
              "lastRunRequestedAtMillis",
              List.of("lastRunRequestedAtMillis", "lastRunStatus"),
              List.of("lastSuccessfulRunRequestedAtMillis", "lastFailedRunRequestedAtMillis")));

  /**
   * A script update that writes an aspect's ordered fields.
   *
   * @param upsert the fields as written to a document that does not exist yet; with nothing to
   *     compare against they apply as they are, even on clients that do not run the script there
   */
  public record Update(
      @Nonnull String script,
      @Nonnull Map<String, Object> params,
      @Nonnull Map<String, Object> upsert) {}

  private OrderedSearchFields() {}

  public static boolean isOrdered(@Nullable String aspectName) {
    return aspectName != null && RULES.containsKey(aspectName);
  }

  /**
   * Removes the aspect's ordered and max fields from {@code document}, along with its {@code
   * _aspects} entry when it has one, and returns the script update that writes them. Empty when the
   * aspect has no ordered fields, leaving the document unchanged.
   */
  @Nonnull
  public static Optional<Update> split(@Nonnull String aspectName, @Nonnull ObjectNode document) {
    Rule rule = RULES.get(aspectName);
    if (rule == null) {
      return Optional.empty();
    }
    Map<String, Object> ordered = new HashMap<>();
    rule.orderedFields().forEach(field -> ordered.put(field, take(document, field)));
    Map<String, Object> max = new HashMap<>();
    rule.maxFields().forEach(field -> max.put(field, take(document, field)));

    Map<String, Object> params = new HashMap<>();
    params.put("orderField", rule.orderField());
    params.put("orderValue", ordered.get(rule.orderField()));
    params.put("ordered", ordered);
    params.put("max", max);
    params.put("aspectsField", MappingConstants.ASPECTS_FIELD_NAME);
    params.put("aspectName", aspectName);
    Map<String, Object> aspect = takeAspect(document, aspectName);
    params.put("aspect", aspect);

    Map<String, Object> upsert = new HashMap<>();
    ordered.forEach((field, value) -> putIfNotNull(upsert, field, value));
    max.forEach((field, value) -> putIfNotNull(upsert, field, value));
    if (aspect != null) {
      upsert.put(MappingConstants.ASPECTS_FIELD_NAME, Map.of(aspectName, aspect));
    }
    return Optional.of(new Update(SCRIPT, params, upsert));
  }

  private static void putIfNotNull(
      @Nonnull Map<String, Object> map, @Nonnull String field, @Nullable Object value) {
    if (value != null) {
      map.put(field, value);
    }
  }

  @Nullable
  private static Object take(@Nonnull ObjectNode document, @Nonnull String field) {
    JsonNode value = document.remove(field);
    return value == null || value.isNull() ? null : OBJECT_MAPPER.convertValue(value, Object.class);
  }

  @Nullable
  private static Map<String, Object> takeAspect(
      @Nonnull ObjectNode document, @Nonnull String aspectName) {
    JsonNode aspects = document.get(MappingConstants.ASPECTS_FIELD_NAME);
    if (!(aspects instanceof ObjectNode aspectsNode)) {
      return null;
    }
    JsonNode aspect = aspectsNode.remove(aspectName);
    if (aspectsNode.isEmpty()) {
      document.remove(MappingConstants.ASPECTS_FIELD_NAME);
    }
    return aspect instanceof ObjectNode ? OBJECT_MAPPER.convertValue(aspect, MAP_TYPE) : null;
  }
}
