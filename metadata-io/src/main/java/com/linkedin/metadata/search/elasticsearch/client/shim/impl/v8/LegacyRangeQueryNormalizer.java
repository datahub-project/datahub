package com.linkedin.metadata.search.elasticsearch.client.shim.impl.v8;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import javax.annotation.Nonnull;

/**
 * Rewrites legacy OpenSearch {@code QueryBuilder#toString()} JSON into a shape the Elasticsearch 8
 * typed client ({@code co.elastic.clients}) will accept. The ES 8 typed query model is generated
 * from the ES API spec and its parser rejects unknown fields with a hard error, whereas
 * OpenSearch's high-level query builders still emit legacy fields. Two cases are handled:
 *
 * <ul>
 *   <li>Legacy {@code range} bounds ({@code from}/{@code to} with {@code include_lower}/{@code
 *       include_upper}) are rewritten to {@code gte}/{@code gt}/{@code lte}/{@code lt}, which ES
 *       8.18+ deprecates in the legacy form.
 *   <li>{@code bool} queries carry {@code adjust_pure_negative}, an internal Lucene default the ES
 *       8 typed {@code BoolQuery} model does not expose. It is stripped; ES 8 applies the same
 *       default ({@code true}) internally, so removal is behavior-preserving.
 * </ul>
 */
public final class LegacyRangeQueryNormalizer {

  private static final String ADJUST_PURE_NEGATIVE = "adjust_pure_negative";

  private LegacyRangeQueryNormalizer() {}

  @Nonnull
  public static String normalize(@Nonnull String queryJson, @Nonnull ObjectMapper objectMapper)
      throws JsonProcessingException {
    JsonNode root = objectMapper.readTree(queryJson);
    normalizeNode(root);
    return objectMapper.writeValueAsString(root);
  }

  private static void normalizeNode(JsonNode node) {
    if (node == null) {
      return;
    }
    if (node.isObject()) {
      ObjectNode objectNode = (ObjectNode) node;
      if (objectNode.has("range") && objectNode.get("range").isObject()) {
        ObjectNode rangeNode = (ObjectNode) objectNode.get("range");
        rangeNode
            .properties()
            .forEach(
                entry -> {
                  if (entry.getValue().isObject()) {
                    normalizeRangeSpec((ObjectNode) entry.getValue());
                  }
                });
      }
      if (objectNode.has("bool") && objectNode.get("bool").isObject()) {
        ((ObjectNode) objectNode.get("bool")).remove(ADJUST_PURE_NEGATIVE);
      }
      objectNode.properties().forEach(entry -> normalizeNode(entry.getValue()));
    } else if (node.isArray()) {
      node.forEach(LegacyRangeQueryNormalizer::normalizeNode);
    }
  }

  private static void normalizeRangeSpec(ObjectNode spec) {
    if (!spec.has("from") && !spec.has("to")) {
      return;
    }

    JsonNode from = spec.get("from");
    JsonNode to = spec.get("to");
    boolean includeLower = !spec.has("include_lower") || spec.get("include_lower").asBoolean(true);
    boolean includeUpper = !spec.has("include_upper") || spec.get("include_upper").asBoolean(true);

    if (from != null && !from.isNull()) {
      spec.set(includeLower ? "gte" : "gt", from);
    }
    if (to != null && !to.isNull()) {
      spec.set(includeUpper ? "lte" : "lt", to);
    }

    spec.remove("from");
    spec.remove("to");
    spec.remove("include_lower");
    spec.remove("include_upper");
  }
}
